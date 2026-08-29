from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import Table, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from biofilter.modules.db.base import Base
from biofilter.modules.db.create_db_mixin import CreateDBMixin
from biofilter.utils.db_loader import bootstrap_models
from biofilter.utils.logger import Logger


# BF4 shorthand URI scheme for "read this parquet bundle directly via DuckDB".
# Format: parquet:///absolute/path/to/bundle/tables
# Internally translated to an in-memory DuckDB engine with one VIEW per
# *.parquet file in the directory (children with `_chr_N` suffix skipped).
PARQUET_URI_SCHEME = "parquet://"

# Bundle manifest filename, looked up next to (or one level above) the
# directory a `parquet://` URI points at.
MANIFEST_FILENAME = "manifest.json"


def _tolerant_json_deserializer(value: Any) -> Any:
    """
    JSON deserializer that tolerates already-decoded values.

    Drivers differ: psycopg2 hands back decoded objects (and SQLAlchemy's
    PostgreSQL dialect disables its deserializer accordingly), while
    duckdb_engine also returns decoded objects but leaves the generic
    dialect's deserializer in place. Passing a dict to json.loads() raises,
    so accept both forms.
    """
    if isinstance(value, (dict, list)):
        return value
    return json.loads(value)


class Database(CreateDBMixin):
    """
    Central DB access layer for Biofilter3R.

    Responsibilities:
    - Normalize & validate DB URI
    - Create SQLAlchemy Engine + Session factory
    - Bootstrap models (declarative + imperative Core tables) into
      Base.metadata
    - Provide a unified Table resolver (Core) via db.table("name")

    Supported URI schemes:
    - `postgresql://...` / `postgresql+psycopg2://...` — production writes
    - `sqlite:///...` — local dev / single-file storage
    - `duckdb:///...` — DuckDB file (advanced)
    - `parquet:///path/to/bundle/tables` — read-only DuckDB over a parquet
      bundle (HPC use case, no DB server required). Each entry in the
      directory becomes a SQL VIEW: a `<table>.parquet` file maps to a
      view over that file, and a `<table>/` directory maps to a view over
      the hive-partitioned dataset beneath it (e.g.
      `variant_masters/chromosome=1/*.parquet`), so the partition key
      comes back as a real column.
    """

    def __init__(self, db_uri: Optional[str] = None, log_level: str = "DEBUG"):
        self.logger = Logger(log_level=log_level)
        self.db_uri: Optional[str] = db_uri

        self.engine: Optional[Engine] = None
        self.SessionLocal = None
        self.connected: bool = False
        # True when the backing *data* is immutable (parquet bundle): the
        # registered VIEWs cannot be written through, so no ETL or import
        # can target this connection.
        #
        # This is advisory, not enforced, and deliberately so — a
        # `parquet://` connection is an in-memory DuckDB, and several
        # reports legitimately CREATE TEMP TABLE against it to stage
        # intermediate results. Blocking all writes would break them.
        self.read_only: bool = False
        # Set when the URI is `parquet://` — path to the tables/ dir.
        self._parquet_dir: Optional[Path] = None

        # Cache of resolved SQLAlchemy Core Table objects
        self._tables: Dict[str, Table] = {}

        if self.db_uri:
            self.connect()

    # -------------------------------------------------------------------------
    # URI / Connection
    # -------------------------------------------------------------------------
    @staticmethod
    def _env_int(name: str, default: int) -> int:
        raw = os.getenv(name)
        if raw is None:
            return int(default)
        try:
            return int(str(raw).strip())
        except Exception:
            return int(default)

    def _engine_kwargs(self, url: URL | str) -> dict:
        """
        Build engine kwargs with safer defaults for long-running jobs.
        """
        parsed = make_url(str(url))
        kwargs: dict = {"future": True}

        if parsed.drivername.startswith("postgresql"):
            connect_args = {
                "connect_timeout": self._env_int("BIOFILTER_DB_CONNECT_TIMEOUT", 10),
                "application_name": os.getenv(
                    "BIOFILTER_DB_APPLICATION_NAME",
                    "biofilter",
                ),
                # libpq TCP keepalive knobs (helps detect dead peers sooner)
                "keepalives": self._env_int("BIOFILTER_DB_KEEPALIVES", 1),
                "keepalives_idle": self._env_int("BIOFILTER_DB_KEEPALIVES_IDLE", 30),
                "keepalives_interval": self._env_int(
                    "BIOFILTER_DB_KEEPALIVES_INTERVAL",
                    10,
                ),
                "keepalives_count": self._env_int("BIOFILTER_DB_KEEPALIVES_COUNT", 5),
            }
            kwargs.update(
                {
                    "pool_pre_ping": True,
                    "pool_recycle": self._env_int("BIOFILTER_DB_POOL_RECYCLE", 1800),
                    "connect_args": connect_args,
                }
            )

        if parsed.drivername.startswith("duckdb"):
            # duckdb_engine already returns JSON columns decoded, but the
            # generic dialect still applies SQLAlchemy's JSON deserializer,
            # so json.loads() gets called on a dict and raises. (The
            # PostgreSQL dialect disables the deserializer for this exact
            # reason; duckdb_engine does not.) Any report selecting a JSON
            # column — e.g. ETLPackage.stats — fails without this.
            kwargs["json_deserializer"] = _tolerant_json_deserializer

            # In-memory DuckDB (used for parquet bundle reads) must share a
            # single connection across sessions, otherwise each new
            # connection gets a fresh DB without the registered VIEWs.
            if parsed.database in (":memory:", "", None):
                kwargs.update({"poolclass": StaticPool})

        return kwargs

    def _normalize_uri(self, uri: str) -> str:
        """
        Translate user-facing URIs into a SQLAlchemy-acceptable form.

        - Bare filesystem path → `sqlite:///<abs path>`
        - `parquet:///path/to/tables` → `duckdb:///:memory:` plus a stored
          path that connect() will use to register parquet VIEWs.
        - Other schemes pass through unchanged.
        """
        # Reset parquet state — successive calls (re-connect) shouldn't
        # carry the previous dir over.
        self._parquet_dir = None

        if uri.startswith(PARQUET_URI_SCHEME):
            raw_path = uri[len(PARQUET_URI_SCHEME):]
            if not raw_path:
                raise ValueError(
                    f"{PARQUET_URI_SCHEME} URI requires a path to the "
                    f"directory containing the bundle parquet files."
                )
            # Strip leading slashes so both parquet://path and
            # parquet:///abs/path work; resolve to absolute.
            parquet_dir = Path(raw_path).expanduser().resolve()
            self._parquet_dir = parquet_dir
            self.read_only = True
            return "duckdb:///:memory:"

        # Mark non-parquet URIs as writable (default).
        self.read_only = False

        if "://" in uri:
            return uri
        return f"sqlite:///{os.path.abspath(uri)}"

    def _bundle_manifest(self) -> Optional[dict]:
        """
        Load the bundle manifest, if one is reachable.

        A `parquet://` URI points at the tables directory, so the manifest
        normally sits one level up. Both locations are accepted.
        """
        if not self._parquet_dir:
            return None

        for candidate in (
            self._parquet_dir / MANIFEST_FILENAME,
            self._parquet_dir.parent / MANIFEST_FILENAME,
        ):
            if candidate.is_file():
                try:
                    return json.loads(candidate.read_text(encoding="utf-8"))
                except Exception:
                    self.logger.log(
                        f"⚠️ Unreadable bundle manifest: {candidate}",
                        "WARNING",
                    )
                    return None
        return None

    def _discover_bundle_sources(self) -> List[tuple]:
        """
        Resolve which relations the bundle exposes, as
        (view_name, duckdb read_parquet glob) pairs.

        Two layouts are supported, and they can coexist:

        - `<table>.parquet`        — a single file, one view over it
        - `<table>/` (a directory) — a partitioned dataset, one view over
          `<table>/**/*.parquet` with hive partitioning enabled, so
          `chromosome=N/` path segments come back as a real column

        Partitioned datasets are what variant tables use: keeping one
        directory per chromosome preserves file-level pruning on
        `WHERE chromosome = N` and lets a single chromosome be rebuilt
        without rewriting the rest.
        """
        assert self._parquet_dir is not None
        base = self._parquet_dir
        sources: Dict[str, str] = {}

        def sql_literal(path: Path) -> str:
            return str(path).replace("'", "''")

        for child in sorted(base.iterdir()):
            if child.is_dir():
                if any(child.rglob("*.parquet")):
                    glob = sql_literal(child / "**" / "*.parquet")
                    sources[child.name] = (
                        f"read_parquet('{glob}', hive_partitioning = true, "
                        f"union_by_name = true)"
                    )
            elif child.suffix == ".parquet":
                sources[child.stem] = (
                    f"read_parquet('{sql_literal(child)}')"
                )

        # Legacy bundles exported both the partitioned parent and its
        # per-chromosome children as sibling files. The parent already
        # carries every row, so the children are duplicates — drop them,
        # but only when the parent they belong to is actually present.
        for name in list(sources):
            parent, sep, _ = name.partition("_chr_")
            if sep and parent in sources:
                del sources[name]

        return sorted(sources.items())

    def _register_parquet_views(self) -> int:
        """
        Register one SQL VIEW per relation exposed by the parquet bundle.

        Returns the number of views registered.
        """
        if not self._parquet_dir or not self.engine:
            return 0

        if not self._parquet_dir.is_dir():
            raise FileNotFoundError(
                f"parquet:// directory not found: {self._parquet_dir}"
            )

        sources = self._discover_bundle_sources()
        if not sources:
            raise FileNotFoundError(
                f"No parquet files or partitioned datasets found in "
                f"{self._parquet_dir}"
            )

        with self.engine.connect() as conn:
            for view_name, reader in sources:
                # View names come from bundle file/directory names, which
                # are table names; quote them so unusual names still work.
                quoted = view_name.replace('"', '""')
                conn.execute(
                    text(
                        f'CREATE OR REPLACE VIEW "{quoted}" AS '
                        f"SELECT * FROM {reader}"
                    )
                )
            conn.commit()

        return len(sources)

    def connect(self, new_uri: Optional[str] = None, check_exists: bool = True) -> None:  # noqa E501
        """
        Connect to database, bootstrap all models for this dialect, and prepare
        a session factory.

        - check_exists=True will attempt a lightweight connectivity check
        before finalizing the connection.
        """
        if new_uri:
            self.db_uri = new_uri

        if not self.db_uri:
            raise ValueError("db_uri must be provided to connect().")

        # Close previous engine (if any)
        if self.engine is not None:
            try:
                self.engine.dispose()
            except Exception:
                pass

        # Reset caches
        self._tables.clear()

        # Normalize uri
        self.db_uri = self._normalize_uri(self.db_uri)

        # Optional connectivity check BEFORE bootstrapping
        if check_exists and not self.exists_db():
            msg = f"❌ Database not found at {self.db_uri}"
            self.logger.log(msg, "ERROR")
            raise ValueError(msg)

        start = time.perf_counter()

        # Create engine
        self.engine = create_engine(self.db_uri, **self._engine_kwargs(self.db_uri))

        # CRITICAL: clear metadata AFTER we know we're switching engines
        # Base.metadata.clear()

        # Re-register everything for this engine/dialect
        bootstrap_models(self.engine)

        # If this is a parquet-bundle backend, register the VIEWs now so
        # subsequent SELECTs against ORM models resolve to read_parquet().
        n_views = 0
        if self._parquet_dir is not None:
            n_views = self._register_parquet_views()

        self.SessionLocal = sessionmaker(
            bind=self.engine,
            future=True,
            expire_on_commit=False,
        )

        elapsed_ms = (time.perf_counter() - start) * 1000

        # Safe URI info logging
        engine_name = host = db_name = "<unknown>"
        try:
            url = make_url(self.db_uri)
            engine_name = url.drivername
            if url.drivername.startswith("sqlite"):
                host = "local file"
                db_name = url.database
            elif self._parquet_dir is not None:
                engine_name = "duckdb+parquet"
                host = "parquet bundle"
                db_name = str(self._parquet_dir)
            else:
                host = url.host or "<unknown>"
                db_name = url.database or "<unknown>"
        except Exception:
            pass

        self.logger.log("🔌 Database connection established", "INFO")
        self.logger.log(f"   • Engine: {engine_name}", "INFO")
        self.logger.log(f"   • Host:   {host}", "INFO")
        self.logger.log(f"   • DB:     {db_name}", "INFO")
        if self._parquet_dir is not None:
            self.logger.log(
                f"   • Views:  {n_views} (read-only)", "INFO"
            )
        self.logger.log(f"   • Time:   {elapsed_ms:.1f} ms", "INFO")
        self.logger.log("════════════════════════════════════", "INFO")

        self.connected = True

    def exists_db(self, new_db=False) -> bool:
        """
        Lightweight check:
        - SQLite: file exists
        - Postgres: SELECT 1 using a temporary engine if needed
        - Parquet bundle (parquet://): directory contains *.parquet files
        """
        if not self.db_uri:
            self.logger.log("Database URI must be set before connecting.", "ERROR")  # noqa E501
            return False

        # Parquet bundle: check the directory directly (the URI we passed
        # to make_url is already the translated duckdb in-memory form, so
        # we use the stashed self._parquet_dir set by _normalize_uri).
        if self._parquet_dir is not None:
            if not self._parquet_dir.is_dir():
                return False
            # Recursive: a bundle may hold only partitioned datasets
            # (`<table>/chromosome=N/*.parquet`) and no top-level files.
            return any(self._parquet_dir.rglob("*.parquet"))

        try:
            url = make_url(self._normalize_uri(self.db_uri))
        except Exception:
            self.logger.log("Invalid database URI.", "ERROR")
            return False

        # SQLite path existence check
        if url.drivername.startswith("sqlite"):
            path = url.database
            return bool(path) and Path(path).exists()

        # DuckDB file existence (parquet:// hits the branch above; this
        # is the explicit `duckdb:///path.duckdb` form).
        if url.drivername.startswith("duckdb"):
            path = url.database
            if path in (":memory:", "", None):
                return True
            return bool(path) and Path(path).exists()

        # PostgreSQL connectivity check
        if url.drivername.startswith("postgresql"):
            temp_engine = None
            try:
                if self.engine is not None:
                    engine = self.engine
                else:
                    temp_engine = create_engine(url, **self._engine_kwargs(url))
                    engine = temp_engine

                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))

                return True
            except Exception as e:
                if not new_db:
                    self.logger.log(f"Could not connect to database: {e}", "ERROR")  # noqa E501
                return False
            finally:
                if temp_engine is not None:
                    temp_engine.dispose()

        self.logger.log("Unsupported database type for exists_db check.", "WARNING")  # noqa E501
        return False

    # -------------------------------------------------------------------------
    # Sessions / Tables
    # -------------------------------------------------------------------------
    def get_session(self):
        if not self.SessionLocal:
            self.logger.log(
                "⚠️ Database not connected. Call connect() first.", "WARNING"
            )
            return None
        return self.SessionLocal()

    def table(self, name: str) -> Table:
        """
        Return a SQLAlchemy Core Table by name, using Base.metadata as the
        source of truth (populated by bootstrap_models).

        Falls back to reflection if the table isn't registered.
        """
        if not self.engine:
            raise RuntimeError("Database not connected. Call connect() first.")

        if name in self._tables:
            return self._tables[name]

        if name in Base.metadata.tables:
            t = Base.metadata.tables[name]
        else:
            # fallback: reflect from DB into Base.metadata
            t = Table(name, Base.metadata, autoload_with=self.engine)

        self._tables[name] = t
        return t
