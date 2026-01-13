# biofilter/cli/groups/report.py
from __future__ import annotations

import click
from biofilter.biofilter import Biofilter
from biofilter.cli.common import require_db_uri, local_db_uri_option


@click.group()
def report():
    """Run and manage reports."""
    pass


@report.command("list")
@local_db_uri_option
@click.pass_context
def list_(ctx, db_uri):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=False)

    click.echo("📊 Available Reports:")
    for r in bf.report.list_reports():
        click.echo(f" - {r}")


@report.command("run")
@local_db_uri_option
@click.option("--name", required=True, help="Report name (e.g., qry_etl_status)")
@click.option("--as-csv", is_flag=True, help="Export to CSV")
@click.option("--output", type=click.Path(dir_okay=False), help="Output file path")
@click.pass_context
def run(ctx, db_uri, name, as_csv, output):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=False)

    df = bf.report.run(name=name, as_dataframe=True)

    if as_csv:
        if not output:
            raise click.UsageError("Must provide --output with --as-csv")
        df.to_csv(output, index=False)
        click.echo(f"✅ Report exported to: {output}")
    else:
        click.echo(df.to_string(index=False))
