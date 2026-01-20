# biofilter/api/cli/groups/project.py
from __future__ import annotations

import click

from biofilter.biofilter import Biofilter
from biofilter.api.cli.common import require_db_uri


@click.group()
def project():
    """Project-level operations (setup, migration, metadata)."""
    pass


@project.command("create")
@click.option("--db-uri", required=True, help="Database URI")
@click.option("--overwrite", is_flag=True, help="Overwrite if exists")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
def create(db_uri: str, overwrite: bool, debug: bool):
    """
    Create a new Biofilter project database.
    """
    bf = Biofilter(debug_mode=debug)

    # In the new architecture, creation is explicit and lives in DBComponent
    bf.db.create(db_uri=db_uri, overwrite=overwrite)

    click.echo(f"🏗️ Biofilter project created at: {db_uri}")


@project.command("migrate")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def migrate(ctx, debug: bool):
    """
    Run database migrations.
    """
    db_uri = require_db_uri(ctx)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()
    bf.db.migrate()

    click.echo("✅ Database migration completed.")
