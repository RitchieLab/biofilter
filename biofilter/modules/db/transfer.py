# biofilter/modules/db/transfer.py
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Literal, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import MetaData, Table, inspect, text
from sqlalchemy.dialects.postgresql import JSON, JSONB
from sqlalchemy.engine import Engine

from biofilter.modules.db.database import Database

# =============================================================================
# Types / Manifest
# =============================================================================

ExportFormat = Literal["parquet", "csv"]

# Bundle manifest schema version.
#   1 - name/rows/file per table only
#   2 - adds bytes, sha256, format, provenance and partition handling
MANIFEST_VERSION = 2


@dataclass(frozen=True)
class Manifest:
    """
    Full-clone bundle manifest.

    Notes:
    - This is a logical snapshot: one file per table + manifest.json.
    - Includes PK values to preserve referential integrity on import.
    """

    biofilter_version: str
    schema_version: str
    engine: str
    created_at: str
    tables: list[dict]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# =============================================================================
# Engine / Dialect helpers (single source of truth)
# =============================================================================


def detect_engine_name(engine: Engine) -> str:
    """
    Return normalized dialect name: 'sqlite' or 'postgresql' (or others).
    """
    return (engine.dialect.name or "").lower().strip()


def sqlite_db_path_from_engine(engine: Engine) -> Path:
    """
    Resolve the SQLite database file path from an Engine.

    For sqlite:///:memory: this is not supported for snapshot backup/restore.
    """
    url = engine.url
    if url.database in (None, "", ":memory:"):
        raise ValueError("SQLite in-memory DB cannot be backed up as a file snapshot.")  # noqa E501
    return Path(url.database).expanduser().resolve()


# =============================================================================
# Product A: Physical snapshot backup/restore
# =============================================================================


def _ensure_backup_file_path(output_path: Path, *, suffix: str) -> Path:
    """
    If output_path is a directory, create a timestamped file inside it.
    If it's a file path, ensure parent exists.
    """
    p = Path(output_path).expanduser().resolve()

    if p.exists() and p.is_dir():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return (p / f"biofilter_backup_{ts}{suffix}").resolve()

    # If it ends with "/" but doesn't exist yet, treat as dir.
    if str(output_path).endswith(os.sep):
        p.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return (p / f"biofilter_backup_{ts}{suffix}").resolve()

    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _to_libpq_dsn(engine: Engine) -> str:
    url = engine.url
    drivername = url.drivername
    if drivername.startswith("postgresql+"):
        drivername = "postgresql"
    elif drivername == "postgres":
        drivername = "postgresql"

    url2 = url.set(drivername=drivername)
    return url2.render_as_string(hide_password=False)


def backup_db(
    engine: Engine,
    output_path: str | Path,
    *,
    postgres_pg_dump: str = "pg_dump",
    postgres_format_custom: bool = True,
    sqlite_vacuum_into: bool = True,
) -> Path:
    """
    Physical snapshot backup.

    - SQLite: copies DB file (or uses VACUUM INTO for a consistent
    compact snapshot).
    - Postgres: pg_dump to a single dump file.

    Returns:
        Path to created backup file.
    """
    out = Path(output_path).expanduser().resolve()
    if out.exists() and out.is_dir():
        out.mkdir(parents=True, exist_ok=True)
    else:
        out.parent.mkdir(parents=True, exist_ok=True)

    dialect = detect_engine_name(engine)
    if dialect == "sqlite":
        return backup_sqlite(engine, out, vacuum_into=sqlite_vacuum_into)
    if dialect in ("postgresql", "postgres"):
        return backup_postgres(
            engine, out, pg_dump=postgres_pg_dump, format_custom=postgres_format_custom  # noqa E501
        )

    raise NotImplementedError(f"backup_db not implemented for engine: {dialect}")  # noqa E501


def restore_db(
    engine: Engine,
    input_path: str | Path,
    *,
    postgres_pg_restore: str = "pg_restore",
    postgres_clean: bool = True,
) -> None:
    """
    Physical snapshot restore.

    - SQLite: replaces the DB file contents with the snapshot file.
    - Postgres: pg_restore into an existing target DB (recommended).
    Optionally uses --clean --if-exists.

    Notes:
    - For SQLite, make sure the application is not holding open connections.
    - For Postgres, permissions and database existence are required.
    """
    inp = Path(input_path).expanduser().resolve()
    if not inp.exists():
        raise FileNotFoundError(str(inp))

    dialect = detect_engine_name(engine)
    if dialect == "sqlite":
        restore_sqlite(engine, inp)
        return
    if dialect in ("postgresql", "postgres"):
        restore_postgres(
            engine, inp, pg_restore=postgres_pg_restore, clean=postgres_clean
        )
        return

    raise NotImplementedError(f"restore_db not implemented for engine: {dialect}")  # noqa E501


def backup_sqlite(
    engine: Engine, output_path: Path, *, vacuum_into: bool = True
) -> Path:
    """
    SQLite snapshot backup.

    Preferred: VACUUM INTO (consistent + compact).
    Fallback: file copy.
    """
    src = sqlite_db_path_from_engine(engine)

    # Ensure output is not the same file
    if src == output_path:
        raise ValueError(
            "Output path must be different from the source SQLite DB path."
        )

    if vacuum_into:
        # VACUUM INTO requires SQLite 3.27+ (most modern systems have it).
        # It creates a consistent copy even if there are active connections
        # (but best to avoid).
        with engine.connect() as conn:
            conn.execute(text("VACUUM INTO :out"), {"out": str(output_path)})
        return output_path

    shutil.copy2(src, output_path)
    return output_path


def restore_sqlite(engine: Engine, input_path: Path) -> None:
    """
    SQLite snapshot restore: replace the DB file on disk.

    This overwrites the target DB file path with the snapshot file.
    """
    dst = sqlite_db_path_from_engine(engine)

    # Basic safety check
    if dst == input_path:
        return

    dst.parent.mkdir(parents=True, exist_ok=True)

    # Best-effort: dispose engine to release file handles
    try:
        engine.dispose()
    except Exception:
        pass

    shutil.copy2(input_path, dst)


def backup_postgres(
    engine: Engine,
    output_path: Path,
    *,
    pg_dump: str = "pg_dump",
    format_custom: bool = True,
) -> Path:
    dsn = _to_libpq_dsn(engine)

    # Choose extension + final file path
    suffix = ".dump" if format_custom else ".sql"
    out_file = _ensure_backup_file_path(Path(output_path), suffix=suffix)

    cmd = [pg_dump, dsn]
    if format_custom:
        cmd += ["-Fc", "-f", str(out_file)]
    else:
        cmd += ["-f", str(out_file)]

    _run_subprocess(cmd, "pg_dump failed")
    return out_file


def restore_postgres(
    engine: Engine,
    input_path: Path,
    *,
    pg_restore: str = "pg_restore",
    clean: bool = True,
) -> None:
    """
    Postgres snapshot restore using pg_restore.

    Assumes the target database already exists and user has permission.

    - clean=True uses --clean --if-exists to drop objects before recreating.
    """
    # url = str(engine.url)
    url = _to_libpq_dsn(engine)

    cmd = [pg_restore, "-d", url]
    if clean:
        cmd += ["--clean", "--if-exists"]

    cmd += [str(input_path)]
    _run_subprocess(cmd, "pg_restore failed")


def _run_subprocess(cmd: list[str], err_msg: str) -> None:
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        stdout = (e.stdout or "").strip()
        extra = ""
        if stderr:
            extra += f"\n[stderr]\n{stderr}"
        if stdout:
            extra += f"\n[stdout]\n{stdout}"
        raise RuntimeError(
            f"{err_msg}. Command={cmd}. ExitCode={e.returncode}{extra}"
        ) from e


# =============================================================================
# Product B: Full Clone Bundle export/import (logical snapshot)
# =============================================================================


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> Optional[str]:
    """
    SHA-256 of a file, streamed. Returns None if the file is unreadable.
    """
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    try:
        with path.open("rb") as fh:
            for block in iter(lambda: fh.read(chunk_size), b""):
                digest.update(block)
    except OSError:
        return None
    return digest.hexdigest()


def collect_provenance(engine: Engine) -> list[dict]:
    """
    Summarize which data sources produced the data in this bundle.

    Read from etl_packages so a bundle can answer "which DTP versions is
    this built from?" without the originating database. Best-effort: a
    bundle exported from a DB without the ETL tables still exports fine.
    """
    sql = """
        SELECT s.name  AS source_system,
               d.name  AS data_source,
               p.version_tag,
               p.status,
               MAX(p.created_at) AS created_at,
               COUNT(*)          AS packages
        FROM etl_packages p
        JOIN etl_data_sources d ON d.id = p.data_source_id
        JOIN etl_source_systems s ON s.id = d.source_system_id
        GROUP BY s.name, d.name, p.version_tag, p.status
        ORDER BY s.name, d.name
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(sql)).mappings().all()
    except Exception:
        return []

    return [
        {
            "source_system": r["source_system"],
            "data_source": r["data_source"],
            "version_tag": r["version_tag"],
            "status": r["status"],
            "created_at": (
                r["created_at"].isoformat()
                if hasattr(r["created_at"], "isoformat")
                else r["created_at"]
            ),
            "packages": int(r["packages"]),
        }
        for r in rows
    ]


def partition_child_tables(engine: Engine) -> set[str]:
    """
    Return the names of tables that are partitions of another table.

    Only PostgreSQL has declarative partitioning here (variant_masters,
    variant_molecular_effects and friends are `PARTITION BY LIST
    (chromosome)`); every other dialect returns an empty set.

    This matters for export: `inspect(engine).get_table_names()` lists both
    the partitioned parent and all of its children, while
    `SELECT * FROM <parent>` already returns every child's rows. Exporting
    both writes each row twice.
    """
    if detect_engine_name(engine) not in ("postgresql", "postgres"):
        return set()

    # relkind filter matters: partitioned *indexes* also carry
    # relispartition, and an index name colliding with a table name would
    # silently drop that table from the export.
    sql = """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relispartition
          AND c.relkind IN ('r', 'p', 'f')
          AND n.nspname = current_schema()
    """
    try:
        with engine.connect() as conn:
            return {row[0] for row in conn.execute(text(sql))}
    except Exception:
        # Never let provenance detection break an export.
        return set()


# -----------------------------------------------------------------------------
# Type helpers
# -----------------------------------------------------------------------------


def _is_jsonish_col(coltype) -> bool:
    """
    True for Postgres JSON / JSONB (including reflected/dialect variants).
    """
    if isinstance(coltype, (JSON, JSONB)):
        return True
    return coltype.__class__.__name__.lower() in {"json", "jsonb"}


def _is_datetimeish_col(coltype) -> bool:
    """
    True for SQL timestamp/datetime-like columns.
    """
    return coltype.__class__.__name__.lower() in {"datetime", "timestamp"}


def _is_date_col(coltype) -> bool:
    """
    True for SQL date columns.
    """
    return coltype.__class__.__name__.lower() == "date"


def _is_stringish_col(coltype) -> bool:
    """
    True for SQL string/text-like columns.
    """
    name = coltype.__class__.__name__.lower()
    return name in {"string", "varchar", "text", "char", "unicode", "unicodetext"}


def _coerce_json_cell(v: Any) -> Any:
    """
    Normalize a value for JSON/JSONB binding.
    - dict/list -> keep
    - "null"/"" -> None
    - JSON string -> json.loads
    - other -> keep
    """
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s == "" or s.lower() == "null":
            return None
        # Try to parse JSON strings
        try:
            return json.loads(s)
        except Exception:
            return v
    return v


def _df_nullify_specials(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make DataFrame safe for DB inserts:
    - NaN / NA / NaT -> None
    - Timestamp -> python datetime
    - Timedelta -> python timedelta (or None)
    """
    df = df.copy()

    for col in df.columns:
        s = df[col]

        # Datetime (tz-aware and naive)
        if pd.api.types.is_datetime64_any_dtype(s):
            if pd.api.types.is_datetime64tz_dtype(s):
                s = s.dt.tz_convert("UTC").dt.tz_localize(None)
            df[col] = np.array(s.dt.to_pydatetime(), dtype=object)

        # Timedelta
        elif pd.api.types.is_timedelta64_dtype(s):
            df[col] = s.apply(lambda v: v.to_pytimedelta() if pd.notna(v) else None)  # noqa E501

    # Final pass: convert any remaining NaN/NA/NaT-like to None
    df = df.astype("object").where(pd.notna(df), None)

    return df


def _coerce_df_for_insert(df: pd.DataFrame, table: Table) -> pd.DataFrame:
    """
    Ensure df values match DB types (especially JSON columns).

    Rules:
    - Convert NaN/NaT to None and Timestamp->datetime
    - For JSON/JSONB columns: ensure dict/list stays dict/list; parse JSON
    strings if possible.
    - For non-JSON columns that contain dict/list: serialize to JSON string.
    """
    df = _df_nullify_specials(df)

    col_by_name = {c.name: c for c in table.columns}

    for col in df.columns:
        sa_col = col_by_name.get(col)
        if sa_col is None:
            continue

        # CSV imports can decode empty-string as NaN -> None.
        # For NOT NULL string columns, preserve empty string instead of NULL.
        if not sa_col.nullable and _is_stringish_col(sa_col.type):
            df[col] = df[col].apply(lambda v: "" if v is None else v)

        if _is_date_col(sa_col.type):
            parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
            as_date = parsed.dt.date.astype("object")
            df[col] = as_date.where(parsed.notna(), None)
            continue

        if _is_datetimeish_col(sa_col.type):
            parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
            parsed = parsed.dt.tz_convert("UTC").dt.tz_localize(None)
            as_dt = pd.Series(
                np.array(parsed.dt.to_pydatetime(), dtype=object),
                index=df.index,
            )
            df[col] = as_dt.where(parsed.notna(), None)
            continue

        is_json_col = _is_jsonish_col(sa_col.type)

        if is_json_col:
            # keep dict/list; parse JSON strings; "null" -> None
            df[col] = df[col].map(_coerce_json_cell)
            continue

        # Non-JSON column: if it contains dict/list, serialize them
        if df[col].dtype == "object":
            sample = [v for v in df[col].dropna().head(20).tolist()]
            has_dict_list = any(isinstance(v, (dict, list)) for v in sample)
            if has_dict_list:
                df[col] = df[col].apply(
                    lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v  # noqa E501
                )

    return df


def _df_to_db_records(df: pd.DataFrame, table: Table) -> list[dict]:
    """
    Convert DataFrame into records safe for SQLAlchemy inserts:
    - NaN -> None
    - NaT -> None
    - pandas Timestamp -> python datetime (via object conversion, no
    to_pydatetime)
    - JSON columns: keep dict/list as dict/list (Postgres binds correctly)
    """
    if df.empty:
        return []

    df2 = _coerce_df_for_insert(df, table)

    # Replace NaN with None
    df2 = df2.replace({np.nan: None})

    # Convert datetime-like columns safely (no FutureWarning)
    for col in df2.columns:
        s = df2[col]

        if pd.api.types.is_datetime64_any_dtype(s):
            # Converts Timestamp -> python datetime, NaT -> None
            tmp = s.astype("datetime64[ns]")
            df2[col] = tmp.astype("object").where(tmp.notna(), None)

        elif pd.api.types.is_timedelta64_dtype(s):
            df2[col] = s.astype("object").where(s.notna(), None)

    # Final pass to ensure no NaT survives
    df2 = df2.astype("object").where(pd.notna(df2), None)

    return df2.to_dict(orient="records")


def _insert_df(
    engine: Engine, table: Table, df: pd.DataFrame, chunksize: int = 50_000
) -> None:
    """
    Chunked SQLAlchemy Core insert that handles:
    - NaT/NaN -> NULL
    - Timestamp -> datetime
    - JSON columns (dict/list)
    - dict/list in non-JSON columns -> JSON string
    """
    if df is None or df.empty:
        return

    # BUG: When SQLite get error from data time.
    with engine.begin() as conn:
        n = len(df)
        for i in range(0, n, chunksize):
            chunk_df = df.iloc[i: i + chunksize]
            records = _df_to_db_records(chunk_df, table)
            if records:
                conn.execute(table.insert(), records)


# =============================================================================
# Export / Import full clone
# =============================================================================


def export_full_clone(
    engine: Engine,
    out_dir: str | Path,
    *,
    biofilter_version: str,
    schema_version: str,
    fmt: str = "parquet",
    chunksize: int = 250_000,
    include_tables: Iterable[str] | None = None,
    exclude_tables: Iterable[str] | None = None,
    include_partition_children: bool = False,
    checksums: bool = True,
) -> Path:
    """
    Export a full-clone bundle:
      out_dir/
        manifest.json
        tables/
          <table>.parquet  (or .csv)

    Includes all tables (except alembic_version) and preserves PKs.

    Partition children are excluded by default: their rows are already
    covered by the partitioned parent, so exporting both stores every
    variant row twice. Pass include_partition_children=True to override.

    Set checksums=False to skip per-file SHA-256 (it re-reads every byte
    written, which is significant on large bundles).
    """
    out = Path(out_dir).expanduser().resolve()
    tables_dir = out / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    insp = inspect(engine)
    all_table_names = [t for t in insp.get_table_names() if t != "alembic_version"]

    # Validation stays against everything the DB actually has, so naming a
    # partition child explicitly still works; only the *default* selection
    # drops them.
    available = set(all_table_names)

    partition_children = (
        set() if include_partition_children else partition_child_tables(engine)
    )
    default_table_names = [
        t for t in all_table_names if t not in partition_children
    ]

    selected = set(
        t.strip() for t in (include_tables or []) if isinstance(t, str) and t.strip()
    )
    excluded = set(
        t.strip() for t in (exclude_tables or []) if isinstance(t, str) and t.strip()
    )

    if selected:
        unknown_selected = sorted(selected - available)
        if unknown_selected:
            raise RuntimeError(
                "Requested export table(s) not found in DB: "
                + ", ".join(unknown_selected)
            )
        table_names = sorted(selected)
    else:
        table_names = sorted(default_table_names)

    if excluded:
        unknown_excluded = sorted(excluded - available)
        if unknown_excluded:
            raise RuntimeError(
                "Requested excluded table(s) not found in DB: "
                + ", ".join(unknown_excluded)
            )
        table_names = [t for t in table_names if t not in excluded]

    if not table_names:
        raise RuntimeError("No tables selected for export.")

    rows_meta: list[dict] = []

    with engine.connect() as conn:
        for t in table_names:
            # row count (best-effort)
            try:
                if detect_engine_name(engine) in ("postgresql", "postgres"):
                    cnt = (
                        conn.execute(text(f'SELECT COUNT(*) FROM "{t}"')).scalar() or 0  # noqa E501
                    )
                else:
                    cnt = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar() or 0  # noqa E501
            except Exception:
                cnt = None

            file_name = f"{t}.{fmt}"
            file_path = tables_dir / file_name

            if fmt == "csv":
                _export_table_csv(conn, engine, t, file_path, chunksize=chunksize)  # noqa E501
            else:
                _export_table_parquet(conn, engine, t, file_path, chunksize=chunksize)  # noqa E501

            entry: dict = {
                "name": t,
                "rows": cnt,
                "file": f"tables/{file_name}",
                "bytes": file_path.stat().st_size if file_path.exists() else None,  # noqa E501
            }
            if checksums:
                entry["sha256"] = sha256_file(file_path)
            rows_meta.append(entry)

    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "biofilter_version": biofilter_version,
        "schema_version": schema_version,
        "engine": detect_engine_name(engine),
        "created_at": utc_now_iso(),
        "format": fmt,
        "partition_children_included": bool(include_partition_children),
        "provenance": collect_provenance(engine),
        "tables": rows_meta,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")  # noqa E501
    return out


def verify_bundle(in_dir: str | Path, *, check_hashes: bool = True) -> dict:
    """
    Validate a bundle against its manifest, without a database.

    Checks that every table the manifest declares is present, that file
    sizes match and — unless check_hashes is False — that SHA-256 digests
    still match what was recorded at export time.

    Returns a report dict with an `ok` flag and a list of `problems`.
    Never raises for content problems; only for a missing/unreadable
    manifest, which means "this is not a bundle".
    """
    root = Path(in_dir).expanduser().resolve()
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"No manifest.json found in {root}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    tables = manifest.get("tables") or []
    problems: list[str] = []
    verified = 0

    for entry in tables:
        name = entry.get("name")
        rel = entry.get("file")
        if not rel:
            problems.append(f"{name}: manifest entry has no file path")
            continue

        path = root / rel
        if not path.exists():
            problems.append(f"{name}: missing file {rel}")
            continue

        expected_bytes = entry.get("bytes")
        actual_bytes = path.stat().st_size
        if expected_bytes is not None and actual_bytes != expected_bytes:
            problems.append(
                f"{name}: size mismatch "
                f"(manifest {expected_bytes}, found {actual_bytes})"
            )

        expected_hash = entry.get("sha256")
        if check_hashes and expected_hash:
            actual = sha256_file(path)
            if actual != expected_hash:
                problems.append(f"{name}: sha256 mismatch")
                continue

        verified += 1

    return {
        "ok": not problems,
        "root": str(root),
        "manifest_version": manifest.get("manifest_version", 1),
        "biofilter_version": manifest.get("biofilter_version"),
        "schema_version": manifest.get("schema_version"),
        "created_at": manifest.get("created_at"),
        "tables_declared": len(tables),
        "tables_verified": verified,
        "hashes_checked": bool(check_hashes),
        "problems": problems,
    }


def import_full_clone(
    db: Database,
    in_dir: str,
    fmt: str = "parquet",
    reset_sequences: bool = True,
    chunksize: int = 50_000,
    allow_missing_tables: bool = False,
) -> None:
    """
    Import a full-clone bundle into an existing schema.

    Steps:
    1) Reflect schema and compute dependency order (MetaData.sorted_tables).
    2) Truncate all tables (except alembic_version).
    3) Import parents->children, preserving PKs.
    4) Reset Postgres sequences (recommended).
    """
    engine = db.engine
    if engine is None:
        raise RuntimeError(
            "Database engine is not initialized (db.engine is None). Connect first."  # noqa E501
        )

    # Disable FK enforcement on SQLite for the duration of the import.
    # Without this, DELETE FROM cannot use the truncate optimization
    # (would iterate and FK-validate every row) and INSERT validates each
    # FK on every row. With FK off, both become orders of magnitude
    # faster. The bundle source DB enforced FKs, so the data is already
    # referentially consistent — we just suspend re-checking during the
    # mass load. Per-connection pragma via event listener so every pooled
    # connection inherits the setting.
    if detect_engine_name(engine) == "sqlite":
        from sqlalchemy import event as _sa_event

        @_sa_event.listens_for(engine, "connect")
        def _disable_sqlite_fk(dbapi_conn, _conn_record):  # noqa
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys = OFF")
            cur.close()

        # Apply immediately to any already-open pool connections.
        with engine.begin() as _conn:
            _conn.execute(text("PRAGMA foreign_keys = OFF"))

    base = Path(in_dir).expanduser().resolve()
    manifest_path = base / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(str(manifest_path))

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries: list[dict] = manifest.get("tables", [])
    if not entries:
        raise RuntimeError("manifest.json has no tables.")

    # Reflect full schema for dependency order
    meta = MetaData()
    meta.reflect(bind=engine)

    insert_order = [t for t in meta.sorted_tables if t.name != "alembic_version"]  # noqa E501
    delete_order = list(reversed(insert_order))

    name_to_file = {e["name"]: e["file"] for e in entries}

    bundle_tables = set(name_to_file.keys())
    schema_tables = {t.name for t in insert_order}

    missing_in_bundle = sorted(schema_tables - bundle_tables)
    extra_in_bundle = sorted(bundle_tables - schema_tables)

    if missing_in_bundle and not allow_missing_tables:
        raise RuntimeError(
            "Full clone bundle is missing tables required by current schema: "
            + ", ".join(missing_in_bundle)
            + ".\nThis would break foreign keys. Re-export the bundle from a matching schema."  # noqa E501
        )

    if allow_missing_tables:
        tables_to_import = [t for t in insert_order if t.name in bundle_tables]
        tables_to_truncate = list(reversed(tables_to_import))
    else:
        tables_to_import = insert_order
        tables_to_truncate = delete_order

    if not tables_to_import:
        raise RuntimeError("No common tables between bundle and current schema.")

    # 1) Truncate relevant tables
    with engine.begin() as conn:
        d = detect_engine_name(engine)
        if d in ("postgresql", "postgres"):
            for table in tables_to_truncate:
                conn.execute(
                    text(f'TRUNCATE TABLE "{table.name}" RESTART IDENTITY CASCADE')  # noqa E501
                )
        else:
            for table in tables_to_truncate:
                conn.execute(text(f"DELETE FROM {table.name}"))

    if extra_in_bundle:
        # Extra tables in bundle are ignored (useful across schema versions).
        pass

    # 2) Import in dependency order
    for reflected_table in tables_to_import:
        rel_file = name_to_file.get(reflected_table.name)
        if not rel_file:
            raise RuntimeError(
                f"Bundle manifest missing table entry for: {reflected_table.name}"
            )

        file_path = base / rel_file
        if not file_path.exists():
            raise FileNotFoundError(str(file_path))

        target_table = db.table(reflected_table.name)

        if fmt == "csv":
            for chunk in pd.read_csv(file_path, chunksize=200_000):
                _insert_df(engine, target_table, chunk, chunksize=chunksize)
        else:
            # Stream parquet by row group batches so we never materialize
            # the whole file in memory. Critical for tables like
            # variant_molecular_effects (~1.79B rows would need 50+ GB
            # of pandas memory if loaded as a single DataFrame).
            pq_file = pq.ParquetFile(file_path)
            for batch in pq_file.iter_batches(batch_size=200_000):
                df = batch.to_pandas()
                _insert_df(engine, target_table, df, chunksize=chunksize)

    # 3) Postgres sequences
    if reset_sequences and detect_engine_name(engine) in ("postgresql", "postgres"):  # noqa E501
        reset_postgres_sequences(engine)


# =============================================================================
# Bundle helpers
# =============================================================================


def _select_all_sql(engine: Engine, table_name: str) -> str:
    d = detect_engine_name(engine)
    if d in ("postgresql", "postgres"):
        return f'SELECT * FROM "{table_name}"'
    return f"SELECT * FROM {table_name}"


def _export_table_csv(
    conn, engine: Engine, table_name: str, out_path: Path, *, chunksize: int
) -> None:
    header_written = False
    sql = _select_all_sql(engine, table_name)
    for chunk in pd.read_sql(text(sql), conn, chunksize=chunksize):
        chunk.to_csv(out_path, mode="a", index=False, header=not header_written)  # noqa E501
        header_written = True
    if not header_written:
        # empty table
        pd.DataFrame().to_csv(out_path, index=False)


def _export_table_parquet(
    conn, engine: Engine, table_name: str, out_path: Path, *, chunksize: int
) -> None:
    """
    Export parquet in streaming mode.

    This keeps memory usage stable by writing each chunk directly to the
    destination parquet file (single file output).
    """
    sql = _select_all_sql(engine, table_name)
    writer: pq.ParquetWriter | None = None
    schema: pa.Schema | None = None

    # Force server-side cursor so psycopg2 doesn't buffer the whole table.
    # Without this, pd.read_sql chunksize only slices an already-materialized
    # resultset and large tables (e.g. variant_masters) cause OOM.
    streaming_conn = conn.execution_options(stream_results=True)

    try:
        for chunk in pd.read_sql(text(sql), streaming_conn, chunksize=chunksize):  # noqa E501
            table = pa.Table.from_pandas(chunk, preserve_index=False)

            if writer is None:
                # Promote null-type columns to nullable string. Without this,
                # later chunks with actual string values fail to cast back
                # (pyarrow can't cast string -> null). This commonly affects
                # partitioned parent tables where early chunks come from a
                # partition with all-null columns.
                if any(pa.types.is_null(f.type) for f in table.schema):
                    new_fields = []
                    new_columns = []
                    for i, field in enumerate(table.schema):
                        if pa.types.is_null(field.type):
                            new_fields.append(
                                pa.field(field.name, pa.string(), nullable=True)  # noqa E501
                            )
                            new_columns.append(table.column(i).cast(pa.string()))  # noqa E501
                        else:
                            new_fields.append(field)
                            new_columns.append(table.column(i))
                    table = pa.Table.from_arrays(
                        new_columns, schema=pa.schema(new_fields)
                    )

                schema = table.schema
                writer = pq.ParquetWriter(str(out_path), schema=schema)
            elif table.schema != schema:
                table = table.cast(schema, safe=False)

            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        # Empty table
        pd.DataFrame().to_parquet(out_path, index=False)


def reset_postgres_sequences(engine: Engine) -> None:
    """
    Reset SERIAL/IDENTITY sequences after importing explicit PK values.

    Strategy:
      - For each table with single-column PK
      - Find pg_get_serial_sequence(table, pk_col)
      - setval(seq, max(pk_col), true)

    This prevents future inserts from colliding with existing PK values.
    """
    insp = inspect(engine)
    with engine.begin() as conn:
        for table in insp.get_table_names():
            if table == "alembic_version":
                continue

            pk = insp.get_pk_constraint(table).get("constrained_columns") or []
            if len(pk) != 1:
                continue
            pk_col = pk[0]

            seq = conn.execute(
                text("SELECT pg_get_serial_sequence(:t, :c)"),
                {"t": table, "c": pk_col},
            ).scalar()

            if not seq:
                continue

            conn.execute(
                text(
                    f'SELECT setval(:seq, COALESCE((SELECT MAX("{pk_col}") FROM "{table}"), 1), true)'  # noqa E501
                ),
                {"seq": seq},
            )
