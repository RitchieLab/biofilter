# biofilter/cli/groups/etl.py
from __future__ import annotations

import click
from biofilter.biofilter import Biofilter
from biofilter.cli.common import require_db_uri, local_db_uri_option


@click.group()
def etl():
    """Run and manage ETL pipelines."""
    pass


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
@click.pass_context
def update(ctx, db_uri, source_system, data_source, run_step, force_step):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=False)

    bf.update(
        source_system=list(source_system) or None,
        data_sources=list(data_source) or None,
        run_steps=list(run_step) or None,
        force_steps=list(force_step) or None,
    )


@etl.command("restart")
@local_db_uri_option
@click.option("--data-source", multiple=True, help="Data source name (repeatable).")
@click.option("--source-system", multiple=True, help="Source system name (repeatable).")
@click.option("--delete-files", is_flag=True, help="Delete downloaded/processed files when restarting.")
@click.pass_context
def restart(ctx, db_uri, data_source, source_system, delete_files):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=False)

    bf.restart_etl(
        data_source=list(data_source) or None,
        source_system=list(source_system) or None,
        delete_files=delete_files,
    )


@etl.command("update-conflicts")
@local_db_uri_option
@click.option("--source-system", multiple=True, help="Source system name (repeatable).")
@click.pass_context
def update_conflicts(ctx, db_uri, source_system):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=False)

    bf.update_conflicts(source_system=list(source_system) or None)
