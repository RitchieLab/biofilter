from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, Optional

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
      bundle (HPC use case, no DB server required). Each .parquet file in
      the directory becomes a SQL VIEW addressable by its stem. Writes are
      blocked.
    """

    def __init__(self, db_uri: Optional[str] = None, log_level: str = "DEBUG"):
        self.logger = Logger(log_level=log_level)
        self.db_uri: Optional[str] = db_uri

        self.engine: Optional[Engine] = None
        self.SessionLocal = None
        self.connected: bool = False
        # Set when the active backend is read-only (parquet bundle).
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

        # In-memory DuckDB (used for parquet bundle reads) must share a
        # single connection across sessions, otherwise each new connection
        # gets a fresh DB without the registered VIEWs.
        if parsed.drivername.startswith("duckdb") and parsed.database in (
            ":memory:",
            "",
            None,
        ):
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

    def _register_parquet_views(self) -> int:
        """
        Register a SQL VIEW for every *.parquet under self._parquet_dir,
        skipping partition children (filenames containing `_chr_`).

        Returns the number of views registered.
        """
        if not self._parquet_dir or not self.engine:
            return 0

        if not self._parquet_dir.is_dir():
            raise FileNotFoundError(
                f"parquet:// directory not found: {self._parquet_dir}"
            )

        parquets = sorted(self._parquet_dir.glob("*.parquet"))
        # Consolidated parents only — children carry duplicate data and
        # the BF4 ORM doesn't declare per-chromosome tables.
        parquets = [p for p in parquets if "_chr_" not in p.stem]

        if not parquets:
            raise FileNotFoundError(
                f"No *.parquet files found in {self._parquet_dir}"
            )

        with self.engine.connect() as conn:
            for p in parquets:
                view_name = p.stem
                path_literal = str(p).replace("'", "''")
                conn.execute(
                    text(
                        f"CREATE OR REPLACE VIEW {view_name} AS "
                        f"SELECT * FROM read_parquet('{path_literal}')"
                    )
                )
            conn.commit()

        return len(parquets)

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
            return any(self._parquet_dir.glob("*.parquet"))

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
