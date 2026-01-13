# biofilter/cli/groups/conflicts.py
from __future__ import annotations

import click
from biofilter.biofilter import Biofilter
from biofilter.cli.common import require_db_uri, local_db_uri_option


@click.group()
def conflicts():
    """Curation conflicts import/export helpers."""
    pass


@conflicts.command("export-excel")
@local_db_uri_option
@click.option("--output", default="curation_conflicts.xlsx", show_default=True)
@click.pass_context
def export_excel(ctx, db_uri, output):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=False)
    bf.export_conflicts_to_excel(output_path=output)
    click.echo(f"✅ Exported to: {output}")


@conflicts.command("import-excel")
@local_db_uri_option
@click.option("--input", "input_path", default="curation_conflicts_template.xlsx", show_default=True)
@click.pass_context
def import_excel(ctx, db_uri, input_path):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=False)
    bf.import_conflicts_from_excel(input_path=input_path)
    click.echo(f"✅ Imported from: {input_path}")
