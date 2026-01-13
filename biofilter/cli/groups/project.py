# biofilter/cli/groups/project.py
from __future__ import annotations

import click
from biofilter.biofilter import Biofilter
from biofilter.cli.common import require_db_uri


@click.group()
def project():
    """Project-level operations (setup, migration, metadata)."""
    pass


@project.command("create")
@click.option("--db-uri", required=True, help="Database URI")
@click.option("--overwrite", is_flag=True, help="Overwrite if exists")
def create(db_uri, overwrite):
    bf = Biofilter(debug_mode=False)
    bf.create_new_project(db_uri=db_uri, overwrite=overwrite)


@project.command("migrate")
@click.pass_context
def migrate(ctx):
    db_uri = require_db_uri(ctx)
    bf = Biofilter(db_uri=db_uri, debug_mode=False)
    bf.migrate()
