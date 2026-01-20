# biofilter/api/cli/main.py
from __future__ import annotations

import click

from biofilter.utils.version import __version__ as current_version
from biofilter.api.cli.common import try_resolve_db_uri

# Groups
from biofilter.api.cli.groups.project import project
from biofilter.api.cli.groups.etl import etl
from biofilter.api.cli.groups.index import index
from biofilter.api.cli.groups.conflicts import conflicts
from biofilter.api.cli.groups.report import report
from biofilter.api.cli.groups.config import config
from biofilter.api.cli.groups.db import db


def _version_callback(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

    db_uri = try_resolve_db_uri(ctx.params.get("db_uri"))
    click.echo(f"biofilter {current_version}")
    click.echo(f"DB: {db_uri}" if db_uri else "DB: <not set> (use --db-uri or .biofilter.toml)")
    ctx.exit()


@click.group(
    help="""
Biofilter 4 CLI - Omics Knowledge Platform
""".strip(),
    context_settings=dict(help_option_names=["--help"]),
    invoke_without_command=True,
)
@click.option(
    "--db-uri",
    required=False,
    type=click.STRING,
    help="Database URI (or set in .biofilter.toml).",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Enable debug logging for commands that support it.",
)
@click.option(
    "--version",
    "-V",
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=_version_callback,
    help="Show the version and exit.",
)
@click.pass_context
def main(ctx, db_uri, debug):
    ctx.ensure_object(dict)

    if db_uri:
        ctx.obj["db_uri"] = db_uri
    if debug:
        ctx.obj["debug"] = True

    # If user runs just `biofilter`, show help + resolved DB hint
    if ctx.invoked_subcommand is None:
        resolved = try_resolve_db_uri(db_uri)
        click.echo(ctx.get_help())
        click.echo()
        if resolved:
            click.echo(f"Active DB: {resolved}")
        else:
            click.echo("Active DB: <not set> (use --db-uri or .biofilter.toml)")


# Register groups
main.add_command(project)
main.add_command(etl)
main.add_command(index)
main.add_command(conflicts)
main.add_command(report)
main.add_command(config)
main.add_command(db)


if __name__ == "__main__":
    main()
