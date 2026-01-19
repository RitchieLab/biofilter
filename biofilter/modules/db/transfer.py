# biofilter/modules/db/transfer.py
from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Literal

import pandas as pd
from sqlalchemy import MetaData, inspect, text
from sqlalchemy.engine import Engine


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
    """
    Postgres snapshot backup using pg_dump.

    Uses the engine.url as a connection string. If you need masking or advanced
    options, wrap/override this function.

    output:
      - custom format: .dump (recommended)
      - plain SQL:     .sql
    """
    url = str(engine.url)
    cmd = [pg_dump, url]

    if format_custom:
        cmd += ["-Fc", "-f", str(output_path)]
    else:
        # plain text SQL
        cmd += ["-f", str(output_path)]

    _run_subprocess(cmd, "pg_dump failed")
    return output_path


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
    url = str(engine.url)

    cmd = [pg_restore, "-d", url]
    if clean:
        cmd += ["--clean", "--if-exists"]

    cmd += [str(input_path)]
    _run_subprocess(cmd, "pg_restore failed")


def _run_subprocess(cmd: list[str], err_msg: str) -> None:
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"{err_msg}. Command={cmd}. ExitCode={e.returncode}") from e


# =============================================================================
# Product B: Full Clone Bundle export/import (logical snapshot)
# =============================================================================

def export_full_clone(
    engine: Engine,
    out_dir: str | Path,
    *,
    biofilter_version: str,
    schema_version: str,
    fmt: ExportFormat = "parquet",
    chunksize: int = 250_000,
) -> Path:
    """
    Export a full-clone bundle:
      out_dir/
        manifest.json
        tables/
          <table>.parquet  (or .csv)

    Includes all tables (except alembic_version) and preserves PKs.

    Returns:
        Bundle directory path.
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
                cnt = conn.execute(text(f'SELECT COUNT(*) FROM "{t}"') if detect_engine_name(engine) in ("postgresql", "postgres") else text(f"SELECT COUNT(*) FROM {t}")).scalar() or 0
            except Exception:
                cnt = None

            file_name = f"{t}.{fmt}"
            file_path = tables_dir / file_name

            if fmt == "csv":
                _export_table_csv(conn, engine, t, file_path, chunksize=chunksize)
            else:
                _export_table_parquet(conn, engine, t, file_path, chunksize=chunksize)

            rows_meta.append({"name": t, "rows": cnt, "file": f"tables/{file_name}"})

    manifest = Manifest(
        biofilter_version=biofilter_version,
        schema_version=schema_version,
        engine=detect_engine_name(engine),
        created_at=utc_now_iso(),
        tables=rows_meta,
    )
    (out / "manifest.json").write_text(json.dumps(manifest.__dict__, indent=2), encoding="utf-8")
    return out


def import_full_clone(
    engine: Engine,
    in_dir: str | Path,
    *,
    fmt: ExportFormat = "parquet",
    reset_postgres_sequences: bool = True,
) -> None:
    """
    Import a full-clone bundle into an existing schema.

    Steps:
      1) Reflect schema and compute dependency order (MetaData.sorted_tables).
      2) Truncate all tables (except alembic_version).
      3) Import parents->children, preserving PKs.
      4) Reset Postgres sequences (recommended).
    """
    base = Path(in_dir).expanduser().resolve()
    manifest_path = base / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(str(manifest_path))

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entries: list[dict] = manifest.get("tables", [])
    if not entries:
        raise RuntimeError("manifest.json has no tables.")

    meta = MetaData()
    meta.reflect(bind=engine)

    insert_order = [t for t in meta.sorted_tables if t.name != "alembic_version"]
    delete_order = list(reversed(insert_order))

    # 1) Truncate-all
    with engine.begin() as conn:
        d = detect_engine_name(engine)
        if d in ("postgresql", "postgres"):
            for table in delete_order:
                conn.execute(text(f'TRUNCATE TABLE "{table.name}" RESTART IDENTITY CASCADE'))
        else:
            for table in delete_order:
                conn.execute(text(f"DELETE FROM {table.name}"))

    # 2) Import
    name_to_file = {e["name"]: e["file"] for e in entries}

    for table in insert_order:
        rel_file = name_to_file.get(table.name)
        if not rel_file:
            # In a strict full clone, this should not happen.
            continue

        file_path = base / rel_file
        if not file_path.exists():
            raise FileNotFoundError(str(file_path))

        if fmt == "csv":
            for chunk in pd.read_csv(file_path, chunksize=200_000):
                chunk.to_sql(table.name, engine, if_exists="append", index=False, method="multi")
        else:
            df = pd.read_parquet(file_path)
            df.to_sql(table.name, engine, if_exists="append", index=False, method="multi")

    # 3) Postgres sequences
    if reset_postgres_sequences and detect_engine_name(engine) in ("postgresql", "postgres"):
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

            # If table is empty, set to 1
            conn.execute(
                text(
                    f"SELECT setval(:seq, COALESCE((SELECT MAX({pk_col}) FROM \"{table}\"), 1), true)"
                ),
                {"seq": seq},
            )
