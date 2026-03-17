# biofilter/api/cli/groups/etl.py
from __future__ import annotations

import click

from biofilter.api.cli.common import local_db_uri_option, require_db_uri
from biofilter.biofilter import Biofilter


@click.group()
def etl():
    """Run and manage ETL pipelines."""
    pass


def _to_list_or_none(values):
    values = list(values) if values else []
    return values or None


@etl.command("update")
@local_db_uri_option
@click.option(
    "--source-system",
    multiple=True,
    help="Source system name (repeatable). Example: --source-system HGNC",
)
@click.option(
    "--data-source",
    multiple=True,
    help="Data source name (repeatable). Example: --data-source dbsnp_sample",
)
@click.option(
    "--run-step",
    multiple=True,
    type=click.Choice(["extract", "transform", "load"], case_sensitive=False),
    help="ETL step to run (repeatable). Default: all steps.",
)
@click.option(
    "--force-step",
    multiple=True,
    type=click.Choice(["extract", "transform", "load"], case_sensitive=False),
    help="ETL step to force (repeatable). Default: none.",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def update(ctx, db_uri, source_system, data_source, run_step, force_step, debug):  # noqa E501
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    bf.etl.update(
        source_system=_to_list_or_none(source_system),
        data_sources=_to_list_or_none(data_source),
        run_steps=_to_list_or_none(run_step),
        force_steps=_to_list_or_none(force_step),
    )


@etl.command("restart")
@local_db_uri_option
@click.option("--data-source", multiple=True, help="Data source name (repeatable).")  # noqa E501
@click.option("--source-system", multiple=True, help="Source system name (repeatable).")  # noqa E501
@click.option(
    "--delete-files",
    is_flag=True,
    help="Delete downloaded/processed files when restarting.",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def restart(ctx, db_uri, data_source, source_system, delete_files, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    bf.etl.restart(
        data_source=_to_list_or_none(data_source),
        source_system=_to_list_or_none(source_system),
        delete_files=delete_files,
    )


# -----------------------------------------------------------------------------
# Recreate DB Indexs
# -----------------------------------------------------------------------------


@etl.command("index")
@local_db_uri_option
@click.option(
    "--group",
    "groups",
    multiple=True,
    help="Index group (repeatable). If omitted, rebuilds all groups.",
)
@click.option("--drop-only", is_flag=True, help="Only drop indexes, do not create.")  # noqa E501
@click.option("--no-drop-first", is_flag=True, help="Do not drop before creating.")  # noqa E501
@click.option(
    "--no-write-mode", is_flag=True, help="Disable DB write-mode tuning hooks."
)
@click.option("--no-read-mode", is_flag=True, help="Disable DB read-mode tuning hooks.")  # noqa E501
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def index(
    ctx, db_uri, groups, drop_only, no_drop_first, no_write_mode, no_read_mode, debug  # noqa E501
):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    ok, msg = bf.etl.index(
        groups=list(groups) or None,
        drop_only=drop_only,
        drop_first=not no_drop_first,
        set_write_mode=not no_write_mode,
        set_read_mode=not no_read_mode,
    )

    if not ok:
        raise click.ClickException(msg)

    click.echo(msg)
