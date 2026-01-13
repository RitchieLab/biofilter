# biofilter/cli/groups/config.py
from __future__ import annotations

import click
from biofilter.utils.config import BiofilterConfig

from biofilter.cli.config_cmds import config_init, config_get, config_set


@click.group()
def config():
    """Configuration inspection and helpers."""
    pass


@config.command("show")
@click.pass_context
def show(ctx):
    """Show resolved Biofilter configuration."""
    click.echo("📄 Biofilter configuration\n")

    # CLI override (global --db-uri)
    cli_db_uri = (ctx.obj or {}).get("db_uri")

    # Load config (if exists)
    try:
        cfg = BiofilterConfig()
        config_path = cfg.path
    except FileNotFoundError:
        cfg = None
        config_path = None

    # ---- Config file info
    click.echo("Config file:")
    if config_path:
        click.echo(f"  {config_path}")
    else:
        click.echo("  <not found>")
    click.echo()

    # ---- Resolved values
    click.echo("Resolved values:")

    def show_value(name, value, source=None):
        if value is None or value == "":
            click.echo(f"  {name}: <not set>")
        else:
            if source:
                click.echo(f"  {name}: {value}   ({source})")
            else:
                click.echo(f"  {name}: {value}")

    # db_uri resolution
    if cli_db_uri:
        show_value("db_uri", cli_db_uri, "from CLI")
    else:
        show_value("db_uri", getattr(cfg, "db_uri", None) if cfg else None)

    show_value("download_path", getattr(cfg, "download_path", None) if cfg else None)
    show_value("processed_path", getattr(cfg, "processed_path", None) if cfg else None)


# attach config subcommands
config.add_command(config_init)
config.add_command(config_get)
config.add_command(config_set)
