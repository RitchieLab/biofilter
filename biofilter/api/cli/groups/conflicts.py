# biofilter/api/cli/groups/conflicts.py
from __future__ import annotations

import click

from biofilter.biofilter import Biofilter
from biofilter.api.cli.common import require_db_uri, local_db_uri_option


@click.group()
def conflicts():
    """Curation conflicts import/export helpers."""
    pass


@conflicts.command("export-excel")
@local_db_uri_option
@click.option("--output", default="curation_conflicts.xlsx", show_default=True)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def export_excel(ctx, db_uri: str | None, output: str, debug: bool):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    # bf.db.connect()  # explicit in the new architecture

    bf.conflicts.export_to_excel(output_path=output)
    click.echo(f"✅ Exported to: {output}")


@conflicts.command("import-excel")
@local_db_uri_option
@click.option(
    "--input",
    "input_path",
    default="curation_conflicts_template.xlsx",
    show_default=True,
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def import_excel(ctx, db_uri: str | None, input_path: str, debug: bool):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    # bf.db.connect()

    bf.conflicts.import_from_excel(input_path=input_path)
    click.echo(f"✅ Imported from: {input_path}")
