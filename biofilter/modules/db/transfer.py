# biofilter/modules/db/transfer.py
from __future__ import annotations

import json
import os
import shutil
import subprocess
import numpy as np
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Literal, Any

from sqlalchemy import MetaData, Table, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import JSON, JSONB
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.engine.url import make_url

from biofilter.modules.db.database import Database


# =============================================================================
# Types / Manifest
# =============================================================================

ExportFormat = Literal["parquet", "csv"]


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
        raise ValueError("SQLite in-memory DB cannot be backed up as a file snapshot.")
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
    url = engine.url  # URL object (tem password real, se você criou engine com ela)
    drivername = url.drivername
    if drivername.startswith("postgresql+"):
        drivername = "postgresql"
    elif drivername == "postgres":
        drivername = "postgresql"

    url2 = url.set(drivername=drivername)

    # IMPORTANTE: isso precisa ser a senha real; se aqui sair "***", a origem já está mascarada
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

    - SQLite: copies DB file (or uses VACUUM INTO for a consistent compact snapshot).
    - Postgres: pg_dump to a single dump file.

    Returns:
        Path to created backup file.
    """
    out = Path(output_path).expanduser().resolve()
    # out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists() and out.is_dir():
        out.mkdir(parents=True, exist_ok=True)
    else:
        out.parent.mkdir(parents=True, exist_ok=True)

    dialect = detect_engine_name(engine)
    if dialect == "sqlite":
        return backup_sqlite(engine, out, vacuum_into=sqlite_vacuum_into)
    if dialect in ("postgresql", "postgres"):
        return backup_postgres(engine, out, pg_dump=postgres_pg_dump, format_custom=postgres_format_custom)

    raise NotImplementedError(f"backup_db not implemented for engine: {dialect}")


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
        restore_postgres(engine, inp, pg_restore=postgres_pg_restore, clean=postgres_clean)
        return

    raise NotImplementedError(f"restore_db not implemented for engine: {dialect}")


def backup_sqlite(engine: Engine, output_path: Path, *, vacuum_into: bool = True) -> Path:
    """
    SQLite snapshot backup.

    Preferred: VACUUM INTO (consistent + compact).
    Fallback: file copy.
    """
    src = sqlite_db_path_from_engine(engine)

    # Ensure output is not the same file
    if src == output_path:
        raise ValueError("Output path must be different from the source SQLite DB path.")

    if vacuum_into:
        # VACUUM INTO requires SQLite 3.27+ (most modern systems have it).
        # It creates a consistent copy even if there are active connections (but best to avoid).
        with engine.connect() as conn:
            conn.execute(text(f"VACUUM INTO :out"), {"out": str(output_path)})
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
        raise RuntimeError(f"{err_msg}. Command={cmd}. ExitCode={e.returncode}{extra}") from e

# =============================================================================
# Product B: Full Clone Bundle export/import (logical snapshot)
# =============================================================================

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


# def _df_nullify_specials(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Make DataFrame safe for DB inserts:
#     - np.nan -> None
#     - NaT -> None
#     - Timestamp -> python datetime
#     """
#     df = df.copy()

#     # Replace NaN with None (works well before object conversion)
#     df = df.replace({np.nan: None})

#     # Convert datetime columns to python datetime (NaT becomes NaT here, handled later)
#     for col in df.columns:
#         s = df[col]
#         if pd.api.types.is_datetime64_any_dtype(s):
#             # df[col] = s.dt.to_pydatetime()
#             # df[col] = np.array(s.dt.to_pydatetime(), dtype=object)
#             df[col] = np.asarray(s.dt.to_pydatetime(), dtype=object)

#         elif pd.api.types.is_timedelta64_dtype(s):
#             # If any timedeltas exist, keep them as objects or None
#             # df[col] = s.astype("object").where(s.notna(), None)
#             df[col] = s.astype("datetime64[ns]").astype("object").where(s.notna(), None)

#     # Final pass: convert any remaining NaT/NaN-like to None
#     df = df.astype("object").where(pd.notna(df), None)

#     return df
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
            # NOTE: Review this point in future pandas releases
            # df[col] = np.asarray(s.dt.to_pydatetime(), dtype=object)
            # df[col] = s.dt.to_pydatetime().astype("object")
            df[col] = np.array(s.dt.to_pydatetime(), dtype=object)

        # Timedelta
        elif pd.api.types.is_timedelta64_dtype(s):
            df[col] = s.apply(lambda v: v.to_pytimedelta() if pd.notna(v) else None)

    # Final pass: convert any remaining NaN/NA/NaT-like to None
    df = df.astype("object").where(pd.notna(df), None)

    return df


def _coerce_df_for_insert(df: pd.DataFrame, table: Table) -> pd.DataFrame:
    """
    Ensure df values match DB types (especially JSON columns).

    Rules:
    - Convert NaN/NaT to None and Timestamp->datetime
    - For JSON/JSONB columns: ensure dict/list stays dict/list; parse JSON strings if possible.
    - For non-JSON columns that contain dict/list: serialize to JSON string.
    """
    df = _df_nullify_specials(df)

    col_by_name = {c.name: c for c in table.columns}

    for col in df.columns:
        sa_col = col_by_name.get(col)
        if sa_col is None:
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
                    lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v
                )

    return df


# def _df_to_db_records(df: pd.DataFrame, table: Table) -> list[dict]:
#     """
#     Convert a DataFrame into list-of-dicts safe for SQLAlchemy inserts,
#     using table-aware coercion (JSON + null handling).
#     """
#     df2 = _coerce_df_for_insert(df, table)
#     return df2.to_dict(orient="records")
def _df_to_db_records(df: pd.DataFrame, table: Table) -> list[dict]:
    """
    Convert DataFrame into records safe for SQLAlchemy inserts:
    - NaN -> None
    - NaT -> None
    - pandas Timestamp -> python datetime (via object conversion, no to_pydatetime)
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


def _insert_df(engine: Engine, table: Table, df: pd.DataFrame, chunksize: int = 50_000) -> None:
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
            chunk_df = df.iloc[i : i + chunksize]
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
) -> Path:
    """
    Export a full-clone bundle:
      out_dir/
        manifest.json
        tables/
          <table>.parquet  (or .csv)

    Includes all tables (except alembic_version) and preserves PKs.
    """
    out = Path(out_dir).expanduser().resolve()
    tables_dir = out / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    insp = inspect(engine)
    table_names = [t for t in insp.get_table_names() if t != "alembic_version"]

    rows_meta: list[dict] = []

    with engine.connect() as conn:
        for t in sorted(table_names):
            # row count (best-effort)
            try:
                if detect_engine_name(engine) in ("postgresql", "postgres"):
                    cnt = conn.execute(text(f'SELECT COUNT(*) FROM "{t}"')).scalar() or 0
                else:
                    cnt = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar() or 0
            except Exception:
                cnt = None

            file_name = f"{t}.{fmt}"
            file_path = tables_dir / file_name

            if fmt == "csv":
                _export_table_csv(conn, engine, t, file_path, chunksize=chunksize)
            else:
                _export_table_parquet(conn, engine, t, file_path, chunksize=chunksize)

            rows_meta.append({"name": t, "rows": cnt, "file": f"tables/{file_name}"})

    manifest = {
        "biofilter_version": biofilter_version,
        "schema_version": schema_version,
        "engine": detect_engine_name(engine),
        "created_at": utc_now_iso(),
        "tables": rows_meta,
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return out


def import_full_clone(
    db: Database,
    in_dir: str,
    fmt: str = "parquet",
    reset_sequences: bool = True,
    chunksize: int = 50_000,
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
        raise RuntimeError("Database engine is not initialized (db.engine is None). Connect first.")

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

    insert_order = [t for t in meta.sorted_tables if t.name != "alembic_version"]
    delete_order = list(reversed(insert_order))

    # 1) Truncate all tables
    with engine.begin() as conn:
        d = detect_engine_name(engine)
        if d in ("postgresql", "postgres"):
            for table in delete_order:
                conn.execute(text(f'TRUNCATE TABLE "{table.name}" RESTART IDENTITY CASCADE'))
        else:
            for table in delete_order:
                conn.execute(text(f"DELETE FROM {table.name}"))

    # 2) Import in dependency order
    name_to_file = {e["name"]: e["file"] for e in entries}

    bundle_tables = set(name_to_file.keys())
    schema_tables = {t.name for t in insert_order}

    missing_in_bundle = sorted(schema_tables - bundle_tables)
    extra_in_bundle = sorted(bundle_tables - schema_tables)

    if missing_in_bundle:
        raise RuntimeError(
            "Full clone bundle is missing tables required by current schema: "
            + ", ".join(missing_in_bundle)
            + ".\nThis would break foreign keys. Re-export the bundle from a matching schema."
        )

    if extra_in_bundle:
        # não é fatal, mas bom logar
        pass

    for reflected_table in insert_order:
        rel_file = name_to_file.get(reflected_table.name)
        if not rel_file:
            # Strict full clone could raise here, but keeping current behavior.
            # continue
            raise RuntimeError(f"Bundle manifest missing table entry for: {table.name}")

        file_path = base / rel_file
        if not file_path.exists():
            raise FileNotFoundError(str(file_path))

        target_table = db.table(reflected_table.name)

        if fmt == "csv":
            for chunk in pd.read_csv(file_path, chunksize=200_000):
                _insert_df(engine, target_table, chunk, chunksize=chunksize)
        else:
            df = pd.read_parquet(file_path)
            print(f"[IMPORT] {table.name}: {len(df)} rows from {file_path}")
            _insert_df(engine, target_table, df, chunksize=chunksize)

    # 3) Postgres sequences
    if reset_sequences and detect_engine_name(engine) in ("postgresql", "postgres"):
        reset_postgres_sequences(engine)


# =============================================================================
# Bundle helpers
# =============================================================================

def _select_all_sql(engine: Engine, table_name: str) -> str:
    d = detect_engine_name(engine)
    if d in ("postgresql", "postgres"):
        return f'SELECT * FROM "{table_name}"'
    return f"SELECT * FROM {table_name}"


def _export_table_csv(conn, engine: Engine, table_name: str, out_path: Path, *, chunksize: int) -> None:
    header_written = False
    sql = _select_all_sql(engine, table_name)
    for chunk in pd.read_sql(text(sql), conn, chunksize=chunksize):
        chunk.to_csv(out_path, mode="a", index=False, header=not header_written)
        header_written = True
    if not header_written:
        # empty table
        pd.DataFrame().to_csv(out_path, index=False)


def _export_table_parquet(conn, engine: Engine, table_name: str, out_path: Path, *, chunksize: int) -> None:
    """
    Export parquet efficiently with chunking. For simplicity we join chunks at the end.
    If tables get huge, we can switch to dataset-style parquet (directory with row-groups).
    """
    parts_dir = out_path.parent / f".{table_name}_parts"
    if parts_dir.exists():
        shutil.rmtree(parts_dir)
    parts_dir.mkdir(parents=True, exist_ok=True)

    sql = _select_all_sql(engine, table_name)
    part_files: list[Path] = []
    i = 0
    for chunk in pd.read_sql(text(sql), conn, chunksize=chunksize):
        part = parts_dir / f"part_{i}.parquet"
        chunk.to_parquet(part, index=False)
        part_files.append(part)
        i += 1

    if not part_files:
        pd.DataFrame().to_parquet(out_path, index=False)
        shutil.rmtree(parts_dir, ignore_errors=True)
        return

    if len(part_files) == 1:
        shutil.move(str(part_files[0]), str(out_path))
        shutil.rmtree(parts_dir, ignore_errors=True)
        return

    dfs = [pd.read_parquet(p) for p in part_files]
    pd.concat(dfs, ignore_index=True).to_parquet(out_path, index=False)
    shutil.rmtree(parts_dir, ignore_errors=True)


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
                    f'SELECT setval(:seq, COALESCE((SELECT MAX("{pk_col}") FROM "{table}"), 1), true)'
                ),
                {"seq": seq},
            )





# def _is_jsonish_col(coltype) -> bool:
#     # Postgres JSON/JSONB
#     if isinstance(coltype, (JSON, JSONB)):
#         return True
#     # Algumas reflexões retornam variantes/dialects diferentes
#     return coltype.__class__.__name__.lower() in {"json", "jsonb"}


# def _coerce_df_for_insert(df: pd.DataFrame, table: Table) -> pd.DataFrame:
#     """
#     Ensure df values match DB types (especially JSON columns).
#     - For JSON/JSONB columns: ensure python dict/list stays dict/list (OK).
#     - For non-JSON columns that contain dict/list: convert to JSON string.
#     """
#     df = df.copy()

#     col_by_name = {c.name: c for c in table.columns}

#     for col in df.columns:
#         if col not in col_by_name:
#             continue

#         sa_col = col_by_name[col]
#         is_json_col = _is_jsonish_col(sa_col.type)

#         # detect dict/list-like values
#         if df[col].dtype == "object":
#             sample = df[col].dropna().head(20).tolist()
#             has_dict_list = any(isinstance(v, (dict, list)) for v in sample)

#             if has_dict_list and not is_json_col:
#                 # if DB column is TEXT/VARCHAR etc, serialize
#                 df[col] = df[col].apply(lambda v: json.dumps(v) if isinstance(v, (dict, list)) else v)

#             # If it IS JSON/JSONB, keep dict/list as-is (SQLAlchemy binds correctly)
#     return df


# def _insert_df(engine: Engine, table: Table, df: pd.DataFrame, chunksize: int = 50_000) -> None:
#     if df.empty:
#         return

#     df2 = _coerce_df_for_insert(df, table)

#     records = df2.to_dict(orient="records")

#     with engine.begin() as conn:
#         for i in range(0, len(records), chunksize):
#             chunk = records[i : i + chunksize]
#             conn.execute(table.insert(), chunk)


# def _df_to_db_records(df: pd.DataFrame) -> list[dict]:
#     """
#     Convert a DataFrame into a list of records safe for SQLAlchemy inserts:
#     - NaT -> None (NULL)
#     - NaN -> None (NULL)
#     - pandas Timestamp -> python datetime
#     """
#     # Replace NaN with None for object conversion
#     df = df.replace({np.nan: None})

#     # Datetime columns: convert NaT -> None and Timestamp -> datetime
#     for col in df.columns:
#         s = df[col]
#         if pd.api.types.is_datetime64_any_dtype(s):
#             # convert to python datetime / None
#             df[col] = s.dt.to_pydatetime()
#         elif pd.api.types.is_timedelta64_dtype(s):
#             # if you ever have timedeltas, convert similarly
#             df[col] = s.astype("object").where(s.notna(), None)

#     # Replace any remaining NaT (can survive some conversions) with None
#     df = df.astype("object").where(df.notna(), None)

#     return df.to_dict(orient="records")


# def export_full_clone(
#     engine: Engine,
#     out_dir: str | Path,
#     *,
#     biofilter_version: str,
#     schema_version: str,
#     fmt: ExportFormat = "parquet",
#     chunksize: int = 250_000,
# ) -> Path:
#     """
#     Export a full-clone bundle:
#       out_dir/
#         manifest.json
#         tables/
#           <table>.parquet  (or .csv)

#     Includes all tables (except alembic_version) and preserves PKs.

#     Returns:
#         Bundle directory path.
#     """
#     out = Path(out_dir).expanduser().resolve()
#     tables_dir = out / "tables"
#     tables_dir.mkdir(parents=True, exist_ok=True)

#     insp = inspect(engine)
#     table_names = [t for t in insp.get_table_names() if t != "alembic_version"]

#     rows_meta: list[dict] = []

#     with engine.connect() as conn:
#         for t in sorted(table_names):
#             # row count (best-effort)
#             try:
#                 cnt = conn.execute(text(f'SELECT COUNT(*) FROM "{t}"') if detect_engine_name(engine) in ("postgresql", "postgres") else text(f"SELECT COUNT(*) FROM {t}")).scalar() or 0
#             except Exception:
#                 cnt = None

#             file_name = f"{t}.{fmt}"
#             file_path = tables_dir / file_name

#             if fmt == "csv":
#                 _export_table_csv(conn, engine, t, file_path, chunksize=chunksize)
#             else:
#                 _export_table_parquet(conn, engine, t, file_path, chunksize=chunksize)

#             rows_meta.append({"name": t, "rows": cnt, "file": f"tables/{file_name}"})

#     manifest = Manifest(
#         biofilter_version=biofilter_version,
#         schema_version=schema_version,
#         engine=detect_engine_name(engine),
#         created_at=utc_now_iso(),
#         tables=rows_meta,
#     )
#     (out / "manifest.json").write_text(json.dumps(manifest.__dict__, indent=2), encoding="utf-8")
#     return out


# # def import_full_clone(
# #     engine: Engine,
# #     in_dir: str | Path,
# #     *,
# #     fmt: ExportFormat = "parquet",
# #     reset_postgres_sequences: bool = True,
# # ) -> None:
# def import_full_clone(
#     db: Database,
#     in_dir: str,
#     fmt: str = "parquet",
#     reset_sequences: bool = True,
#     chunksize: int = 50_000,
# ):
#     engine = db.engine
#     """
#     Import a full-clone bundle into an existing schema.

#     Steps:
#       1) Reflect schema and compute dependency order (MetaData.sorted_tables).
#       2) Truncate all tables (except alembic_version).
#       3) Import parents->children, preserving PKs.
#       4) Reset Postgres sequences (recommended).
#     """
#     base = Path(in_dir).expanduser().resolve()
#     manifest_path = base / "manifest.json"
#     if not manifest_path.exists():
#         raise FileNotFoundError(str(manifest_path))

#     manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
#     entries: list[dict] = manifest.get("tables", [])
#     if not entries:
#         raise RuntimeError("manifest.json has no tables.")

#     meta = MetaData()
#     meta.reflect(bind=engine)

#     insert_order = [t for t in meta.sorted_tables if t.name != "alembic_version"]
#     delete_order = list(reversed(insert_order))

#     # 1) Truncate-all
#     with engine.begin() as conn:
#         d = detect_engine_name(engine)
#         if d in ("postgresql", "postgres"):
#             for table in delete_order:
#                 conn.execute(text(f'TRUNCATE TABLE "{table.name}" RESTART IDENTITY CASCADE'))
#         else:
#             for table in delete_order:
#                 conn.execute(text(f"DELETE FROM {table.name}"))

#     # 2) Import
#     name_to_file = {e["name"]: e["file"] for e in entries}

#     for table in insert_order:
#         rel_file = name_to_file.get(table.name)
#         if not rel_file:
#             # In a strict full clone, this should not happen.
#             continue

#         file_path = base / rel_file
#         if not file_path.exists():
#             raise FileNotFoundError(str(file_path))

#         if fmt == "csv":
#             for chunk in pd.read_csv(file_path, chunksize=200_000):
#                 chunk.to_sql(table.name, engine, if_exists="append", index=False, method="multi")
#         else:
#             df = pd.read_parquet(file_path)
#             target_table = db.table(table.name)  # <- usa o nome da tabela atual do loop
#             _insert_df(engine, target_table, df, chunksize=chunksize)

#     # 3) Postgres sequences
#     # if reset_postgres_sequences and detect_engine_name(engine) in ("postgresql", "postgres"):
#     #     reset_postgres_sequences(engine)
#     if reset_sequences and detect_engine_name(engine) in ("postgresql", "postgres"):
#         reset_postgres_sequences(engine)   # <- agora é a FUNÇÃO


# # =============================================================================
# # Bundle helpers
# # =============================================================================

# def _select_all_sql(engine: Engine, table_name: str) -> str:
#     d = detect_engine_name(engine)
#     if d in ("postgresql", "postgres"):
#         return f'SELECT * FROM "{table_name}"'
#     return f"SELECT * FROM {table_name}"


# def _export_table_csv(conn, engine: Engine, table_name: str, out_path: Path, *, chunksize: int) -> None:
#     header_written = False
#     sql = _select_all_sql(engine, table_name)
#     for chunk in pd.read_sql(text(sql), conn, chunksize=chunksize):
#         chunk.to_csv(out_path, mode="a", index=False, header=not header_written)
#         header_written = True
#     if not header_written:
#         # empty table
#         pd.DataFrame().to_csv(out_path, index=False)


# def _export_table_parquet(conn, engine: Engine, table_name: str, out_path: Path, *, chunksize: int) -> None:
#     """
#     Export parquet efficiently with chunking. For simplicity we join chunks at the end.
#     If tables get huge, we can switch to dataset-style parquet (directory with row-groups).
#     """
#     parts_dir = out_path.parent / f".{table_name}_parts"
#     if parts_dir.exists():
#         shutil.rmtree(parts_dir)
#     parts_dir.mkdir(parents=True, exist_ok=True)

#     sql = _select_all_sql(engine, table_name)
#     part_files: list[Path] = []
#     i = 0
#     for chunk in pd.read_sql(text(sql), conn, chunksize=chunksize):
#         part = parts_dir / f"part_{i}.parquet"
#         chunk.to_parquet(part, index=False)
#         part_files.append(part)
#         i += 1

#     if not part_files:
#         pd.DataFrame().to_parquet(out_path, index=False)
#         shutil.rmtree(parts_dir, ignore_errors=True)
#         return

#     if len(part_files) == 1:
#         shutil.move(str(part_files[0]), str(out_path))
#         shutil.rmtree(parts_dir, ignore_errors=True)
#         return

#     dfs = [pd.read_parquet(p) for p in part_files]
#     pd.concat(dfs, ignore_index=True).to_parquet(out_path, index=False)
#     shutil.rmtree(parts_dir, ignore_errors=True)


# def reset_postgres_sequences(engine: Engine) -> None:
#     """
#     Reset SERIAL/IDENTITY sequences after importing explicit PK values.

#     Strategy:
#       - For each table with single-column PK
#       - Find pg_get_serial_sequence(table, pk_col)
#       - setval(seq, max(pk_col), true)

#     This prevents future inserts from colliding with existing PK values.
#     """
#     insp = inspect(engine)
#     with engine.begin() as conn:
#         for table in insp.get_table_names():
#             if table == "alembic_version":
#                 continue

#             pk = insp.get_pk_constraint(table).get("constrained_columns") or []
#             if len(pk) != 1:
#                 continue
#             pk_col = pk[0]

#             seq = conn.execute(
#                 text("SELECT pg_get_serial_sequence(:t, :c)"),
#                 {"t": table, "c": pk_col},
#             ).scalar()

#             if not seq:
#                 continue

#             # If table is empty, set to 1
#             conn.execute(
#                 text(
#                     f"SELECT setval(:seq, COALESCE((SELECT MAX({pk_col}) FROM \"{table}\"), 1), true)"
#                 ),
#                 {"seq": seq},
#             )
