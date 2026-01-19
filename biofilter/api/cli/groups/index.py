# biofilter/api/cli/groups/index.py
from __future__ import annotations

import click

from biofilter.biofilter import Biofilter
from biofilter.api.cli.common import require_db_uri, local_db_uri_option


@click.group()
def index():
    """Index management (drop/create/rebuild)."""
    pass


@index.command("rebuild")
@local_db_uri_option
@click.option(
    "--group",
    "groups",
    multiple=True,
    help="Index group (repeatable). If omitted, rebuilds all groups.",
)
@click.option("--drop-only", is_flag=True, help="Only drop indexes, do not create.")
@click.option("--no-drop-first", is_flag=True, help="Do not drop before creating.")
@click.option("--no-write-mode", is_flag=True, help="Disable DB write-mode tuning hooks.")
@click.option("--no-read-mode", is_flag=True, help="Disable DB read-mode tuning hooks.")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def rebuild(ctx, db_uri, groups, drop_only, no_drop_first, no_write_mode, no_read_mode, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    ok, msg = bf.etl.rebuild_indexes(
        groups=list(groups) or None,
        drop_only=drop_only,
        drop_first=not no_drop_first,
        set_write_mode=not no_write_mode,
        set_read_mode=not no_read_mode,
    )

    if not ok:
        raise click.ClickException(msg)

    click.echo(msg)
