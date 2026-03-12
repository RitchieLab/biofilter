# biofilter/api/cli/groups/report.py
from __future__ import annotations

import click

from biofilter.biofilter import Biofilter
from biofilter.api.cli.common import require_db_uri, local_db_uri_option


@click.group()
def report():
    """Run and manage reports."""
    pass


# TESTADO
@report.command("list")
@local_db_uri_option
@click.option("--verbose", is_flag=True, help="Show descriptions and module names.")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def list_(ctx, db_uri, verbose, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)

    rows = bf.report.list(verbose=False)  # returns list[dict]
    if not rows:
        click.echo("No reports found.")
        return

    click.echo("📊 Available Reports:\n")
    for i, r in enumerate(rows, start=1):
        name = r.get("name", "")
        desc = r.get("description", "") or ""
        module = r.get("module", "") or ""

        click.echo(f"{i}. {name}")
        if verbose:
            if desc:
                click.echo(f"   {desc}")
            if module:
                click.echo(f"   module: {module}")
        click.echo("")


# TESTADO
@report.command("explain")
@local_db_uri_option
@click.option(
    "--report-name",
    "identifier",
    required=True,
    help="Report identifier (module/friendly/class name).",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def explain(ctx, db_uri, identifier, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)

    text = bf.report.explain(identifier)
    click.echo(text)


# TESTADO
@report.command("example-input")
@local_db_uri_option
@click.option(
    "--report-name",
    "identifier",
    required=True,
    help="Report identifier (module/friendly/class name).",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def example_input(ctx, db_uri, identifier, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)

    text = bf.report.example_input(identifier, print_output=False)
    click.echo(text)


# TESTADO
@report.command("available-columns")
@local_db_uri_option
@click.option(
    "--report-name",
    "identifier",
    required=True,
    help="Report identifier (module/friendly/class name).",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def available_columns(ctx, db_uri, identifier, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)

    text = bf.report.available_columns(identifier, print_output=False)
    click.echo(text)


@report.command("run")
@local_db_uri_option
@click.option(
    "--name",
    "identifier",
    required=True,
    help="Report identifier (module/friendly/class name).",
)
@click.option("--as-csv", is_flag=True, help="Export result to CSV")
@click.option(
    "--output",
    type=click.Path(dir_okay=False),
    help="Output file path (required with --as-csv)",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def run(ctx, db_uri, identifier, as_csv, output, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)

    # NOTE: report-specific parameters can be added later (e.g., --input-json, --param KEY=VALUE, etc.)
    df = bf.report.run(identifier)

    if as_csv:
        if not output:
            raise click.UsageError("Must provide --output with --as-csv")
        df.to_csv(output, index=False)
        click.echo(f"✅ Report exported to: {output}")
    else:
        click.echo(df.to_string(index=False))
