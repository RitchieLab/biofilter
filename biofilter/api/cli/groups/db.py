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
# Create New DataBase
# -----------------------------------------------------------------------------


# NOTE: Tested
@db.command("create-db")
@click.option("--db-uri", required=True, help="Database URI")
@click.option("--overwrite", is_flag=True, help="Overwrite if exists")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
def create(db_uri: str, overwrite: bool, debug: bool):
    """
    Create a new Biofilter db database.
    """
    bf = Biofilter(debug_mode=debug)

    # In the new architecture, creation is explicit and lives in DBComponent
    bf.db.create_db(db_uri=db_uri, overwrite=overwrite)

    # click.echo(f"🏗️ Biofilter project created at: {db_uri}")


# NOTE: Need fix and apply Alembic
# @db.command("migrate")
# @local_db_uri_option
# @click.option("--debug", is_flag=True, help="Enable debug logging.")
# @click.pass_context
# def migrate(ctx, db_uri, debug: bool):
#     """
#     Run database migrations.
#     """
#     # db_uri = require_db_uri(ctx)
#     db_uri = require_db_uri(ctx, local_db_uri=db_uri)

#     bf = Biofilter(db_uri=db_uri, debug_mode=debug)
#     # bf.db.connect()
#     bf.db.migrate()


#     click.echo("✅ Database migration completed.")
@db.command("migrate")
@local_db_uri_option
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option("--status", is_flag=True, help="Show current DB revision and repo head.")
@click.option(
    "--stamp-head", is_flag=True, help="Stamp DB to Alembic head without running DDL."
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print SQL that would run for upgrade (no execution).",
)
@click.option(
    "--force",
    is_flag=True,
    help="Force dangerous actions (e.g., stamp over existing version).",
)
@click.option(
    "--target",
    default="head",
    show_default=True,
    help="Target revision (default: head).",
)
@click.pass_context
def migrate(
    ctx,
    db_uri,
    debug: bool,
    status: bool,
    stamp_head: bool,
    dry_run: bool,
    force: bool,
    target: str,
):
    """
    Run database migrations.
    """
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)

    # resolve action
    action = "upgrade"
    if status:
        action = "status"
    elif stamp_head:
        action = "stamp-head"
    elif dry_run:
        action = "dry-run"

    bf.db.migrate(action=action, target=target, force=force)

    # Messages
    if action == "status":
        click.echo("✅ Status displayed.")
    elif action == "stamp-head":
        click.echo("✅ Database stamped to head.")
    elif action == "dry-run":
        click.echo("✅ Dry-run completed (SQL printed).")
    else:
        click.echo("✅ Database migration completed.")


# -----------------------------------------------------------------------------
# Upgrade (schema + seeds)
# -----------------------------------------------------------------------------


@db.command("upgrade")
@local_db_uri_option
@click.option(
    "--seed-dir",
    default="seed",
    show_default=True,
    help="Seed directory used to apply master data updates.",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.option(
    "--force", is_flag=True, help="Force dangerous actions (reserved for future use)."
)
@click.pass_context
def upgrade(ctx, db_uri, seed_dir: str, debug: bool, force: bool):
    """
    Upgrade database to latest schema and apply master seeds (idempotent).
    Equivalent to:
      - db migrate --target head
      - apply seed upserts
    """
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)

    # Ensure DB engine/session exist
    bf.db.connect()

    # 1) Schema upgrade
    bf.db.migrate(action="upgrade", target="head", force=force)

    # 2) Master seeds upsert
    bf.db.upgrade(seed_dir=seed_dir)

    click.echo("✅ Database upgraded (schema + seeds).")


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
    # bf.db.connect()

    created = bf.db.backup(out_path)
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
    # bf.db.connect()

    bf.db.restore(in_path)
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
def export_cmd(
    ctx, db_uri, out_dir: Path, fmt: str, schema_version: str, chunksize: int
):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=False)
    # bf.db.connect()

    bundle = bf.db.export(
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
def import_cmd(
    ctx,
    db_uri,
    in_dir: Path,
    fmt: str,
    no_rebuild_indexes: bool,
    no_reset_sequences: bool,
):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=False)
    # bf.db.connect()

    bf.db.import_(
        in_dir=in_dir,
        fmt=fmt.lower(),
        rebuild_indexes=not no_rebuild_indexes,
        reset_postgres_sequences=not no_reset_sequences,
    )
    click.echo("✅ Bundle import completed.")
