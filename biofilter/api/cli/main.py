# biofilter/api/cli/main.py
from __future__ import annotations

import click

from biofilter.api.cli.common import bundle_to_uri, try_resolve_db_uri
from biofilter.utils.bundle_path import BundlePathError, check_bundle_path
from biofilter.api.cli.groups.bundle import bundle
from biofilter.api.cli.groups.config import config
from biofilter.api.cli.groups.db import db

# Groups
from biofilter.api.cli.groups.etl import etl
from biofilter.api.cli.groups.report import report
from biofilter.utils.version import __version__ as current_version


def _version_callback(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

    db_uri = try_resolve_db_uri(
        ctx.params.get("db_uri"), ctx.params.get("bundle")
    )
    click.echo(f"biofilter {current_version}")
    click.echo(
        f"DB: {db_uri}"
        if db_uri
        else "DB: <not set> (use --bundle, BIOFILTER_BUNDLE or .biofilter.toml)"
    )
    ctx.exit()


class BiofilterCLI(click.Group):
    """
    Turns a bad bundle path into a usage error.

    Every command builds a `Biofilter`, which checks the path before the
    engine sees it. Left alone, that surfaces as a traceback ending in
    BundlePathError — accurate, and the wrong shape for a terminal.
    """

    def invoke(self, ctx):
        try:
            return super().invoke(ctx)
        except BundlePathError as exc:
            raise click.UsageError(str(exc)) from exc


@click.group(
    cls=BiofilterCLI,
    help="""
Biofilter 4 CLI - Omics Knowledge Platform
""".strip(),
    context_settings=dict(help_option_names=["--help"]),
    invoke_without_command=True,
)
@click.option(
    "--bundle",
    required=False,
    type=click.Path(exists=True, file_okay=False),
    help=(
        "Path to a bundle folder. The usual way to point Biofilter at "
        "data (or set BIOFILTER_BUNDLE)."
    ),
)
@click.option(
    "--db-uri",
    required=False,
    type=click.STRING,
    help=(
        "Database URI, for a staging SQLite or an existing PostgreSQL. "
        "Prefer --bundle for reading data."
    ),
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
def main(ctx, bundle, db_uri, debug):
    ctx.ensure_object(dict)

    if bundle and db_uri:
        raise click.UsageError(
            "Pass --bundle or --db-uri, not both. --bundle names a bundle "
            "folder; --db-uri names a database."
        )

    resolved_uri = bundle_to_uri(bundle) or db_uri
    if resolved_uri:
        # Check it here, not only where it is opened. Commands that read
        # metadata never open a bundle, so a typo in --bundle would pass
        # silently — and the user did assert a path.
        check_bundle_path(resolved_uri)
        ctx.obj["db_uri"] = resolved_uri
    if debug:
        ctx.obj["debug"] = True

    # If user runs just `biofilter`, show help + resolved DB hint
    if ctx.invoked_subcommand is None:
        resolved = try_resolve_db_uri(db_uri, bundle)
        click.echo(ctx.get_help())
        click.echo()
        if resolved:
            click.echo(f"Active DB: {resolved}")
        else:
            click.echo(
                "Active DB: <not set> (use --db-uri, DATABASE_URL or .biofilter.toml)"  # noqa E501
            )


# Register groups
main.add_command(etl)
main.add_command(report)
main.add_command(config)
main.add_command(db)
main.add_command(bundle)

if __name__ == "__main__":
    main()
