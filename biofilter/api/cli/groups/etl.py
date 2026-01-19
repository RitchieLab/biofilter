# biofilter/api/cli/groups/etl.py
from __future__ import annotations

import click

from biofilter.biofilter import Biofilter
from biofilter.api.cli.common import require_db_uri, local_db_uri_option


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
def update(ctx, db_uri, source_system, data_source, run_step, force_step, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    bf.etl.update(
        source_system=_to_list_or_none(source_system),
        data_sources=_to_list_or_none(data_source),
        run_steps=_to_list_or_none(run_step),
        force_steps=_to_list_or_none(force_step),
        use_conflict_csv=False,
    )


@etl.command("restart")
@local_db_uri_option
@click.option("--data-source", multiple=True, help="Data source name (repeatable).")
@click.option("--source-system", multiple=True, help="Source system name (repeatable).")
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

    bf.etl.restart_etl(
        data_source=_to_list_or_none(data_source),
        source_system=_to_list_or_none(source_system),
        delete_files=delete_files,
    )


@etl.command("update-conflicts")
@local_db_uri_option
@click.option("--source-system", multiple=True, help="Source system name (repeatable).")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def update_conflicts(ctx, db_uri, source_system, debug):
    """
    Reload step using conflict CSV inputs.

    Note: This keeps compatibility with the older command behavior, but routes
    execution through the new ETL component.
    """
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    bf.etl.update(
        source_system=_to_list_or_none(source_system),
        data_sources=None,
        run_steps=["load"],
        force_steps=["load"],
        use_conflict_csv=True,
    )
