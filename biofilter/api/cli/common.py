# biofilter/api/cli/common.py
from __future__ import annotations

import os

import click

from biofilter.utils.config import BiofilterConfig


def _clean_db_uri(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value or None


def try_resolve_db_uri(cli_db_uri: str | None) -> str | None:
    """
    Resolve DB URI with priority:
    1) CLI --db-uri
    2) ENV DATABASE_URL / BIOFILTER_DB_URI
    3) .biofilter.toml (BiofilterConfig)
    """
    cli_db_uri = _clean_db_uri(cli_db_uri)
    if cli_db_uri:
        return cli_db_uri

    env_db_uri = _clean_db_uri(
        os.getenv("DATABASE_URL") or os.getenv("BIOFILTER_DB_URI")
    )
    if env_db_uri:
        return env_db_uri

    try:
        cfg = BiofilterConfig()
        return _clean_db_uri(getattr(cfg, "db_uri", None))
    except FileNotFoundError:
        return None


def resolve_db_uri(cli_db_uri: str | None) -> str:
    db_uri = try_resolve_db_uri(cli_db_uri)
    if db_uri:
        return db_uri
    raise click.UsageError(
        "DB not set. Use --db-uri, DATABASE_URL, or define db_uri in .biofilter.toml."
    )


def get_ctx_db_uri(ctx: click.Context) -> str | None:
    obj = ctx.obj or {}
    return _clean_db_uri(obj.get("db_uri"))


def get_ctx_debug(ctx: click.Context) -> bool:
    return bool((ctx.obj or {}).get("debug"))


def require_db_uri(ctx: click.Context, local_db_uri: str | None = None) -> str:
    """
    Resolve DB URI with priority:
    1) command-local --db-uri
    2) global --db-uri (ctx.obj)
    3) env vars (DATABASE_URL / BIOFILTER_DB_URI)
    4) config file (.biofilter.toml)
    """
    return resolve_db_uri(_clean_db_uri(local_db_uri) or get_ctx_db_uri(ctx))


def local_db_uri_option(fn):
    return click.option(
        "--db-uri",
        required=False,
        type=click.STRING,
        help="Override database URI for this command.",
    )(fn)


def global_db_uri_option(fn):
    """
    Use this on the CLI root group (main) to store db_uri in ctx.obj.
    """
    return click.option(
        "--db-uri",
        required=False,
        type=click.STRING,
        help="Database URI used by all commands (can be overridden per-command).",  # noqa E501
    )(fn)
