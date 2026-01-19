# biofilter/api/cli/groups/db.py
from __future__ import annotations

from pathlib import Path

import click

from biofilter.biofilter import Biofilter
from biofilter.api.cli.common import require_db_uri, local_db_uri_option


@click.group()
def db():
    """Database transfer utilities (backup/restore/export/import)."""
    pass


# -----------------------------------------------------------------------------
# Physical snapshot: backup / restore
# -----------------------------------------------------------------------------

@db.command("backup")
@local_db_uri_option
@click.option(
    "--out",
    "out_path",
    required=True,
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    help="Output file path for snapshot backup (SQLite file copy / pg_dump).",
)
@click.pass_context
def backup_cmd(ctx, db_uri, out_path: Path):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=False)
    bf.db.connect()

    created = bf.transfer.backup(out_path)
    click.echo(f"✅ Backup created: {created}")


@db.command("restore")
@local_db_uri_option
@click.option(
    "--in",
    "in_path",
    required=True,
    type=click.Path(dir_okay=False, readable=True, path_type=Path),
    help="Input snapshot file path to restore (SQLite file / pg_restore dump).",
)
@click.pass_context
def restore_cmd(ctx, db_uri, in_path: Path):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=False)
    bf.db.connect()

    bf.transfer.restore(in_path)
    click.echo("✅ Restore completed.")


# -----------------------------------------------------------------------------
# Logical full clone bundle: export / import
# -----------------------------------------------------------------------------

@db.command("export")
@local_db_uri_option
@click.option(
    "--out",
    "out_dir",
    required=True,
    type=click.Path(file_okay=False, writable=True, path_type=Path),
    help="Output directory for full-clone bundle (manifest + tables/).",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["parquet", "csv"], case_sensitive=False),
    default="parquet",
    show_default=True,
    help="Export file format for table payloads.",
)
@click.option(
    "--schema-version",
    default="unknown",
    show_default=True,
    help="Schema version tag to write in manifest.json.",
)
@click.option(
    "--chunksize",
    default=250_000,
    show_default=True,
    type=int,
    help="Chunk size for streaming reads during export (CSV + parquet chunking helpers).",
)
@click.pass_context
def export_cmd(ctx, db_uri, out_dir: Path, fmt: str, schema_version: str, chunksize: int):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=False)
    bf.db.connect()

    bundle = bf.transfer.export(
        out_dir=out_dir,
        fmt=fmt.lower(),
        schema_version=schema_version,
        chunksize=chunksize,
    )
    click.echo(f"✅ Bundle exported: {bundle}")


@db.command("import")
@local_db_uri_option
@click.option(
    "--in",
    "in_dir",
    required=True,
    type=click.Path(file_okay=False, readable=True, path_type=Path),
    help="Input directory of full-clone bundle (must contain manifest.json + tables/).",
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["parquet", "csv"], case_sensitive=False),
    default="parquet",
    show_default=True,
    help="Import file format for table payloads.",
)
@click.option(
    "--no-rebuild-indexes",
    is_flag=True,
    help="Do not rebuild indexes after import.",
)
@click.option(
    "--no-reset-sequences",
    is_flag=True,
    help="(Postgres) Do not reset sequences after import.",
)
@click.pass_context
def import_cmd(ctx, db_uri, in_dir: Path, fmt: str, no_rebuild_indexes: bool, no_reset_sequences: bool):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=False)
    bf.db.connect()

    bf.transfer.import_(
        in_dir=in_dir,
        fmt=fmt.lower(),
        rebuild_indexes=not no_rebuild_indexes,
        reset_postgres_sequences=not no_reset_sequences,
    )
    click.echo("✅ Bundle import completed.")
