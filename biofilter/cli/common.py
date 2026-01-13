# # biofilter/cli/common.py
# import click
# from biofilter.utils.config import BiofilterConfig  # <- existe no seu Biofilter.py


# def resolve_db_uri(db_uri: str | None) -> str:
#     if db_uri:
#         return db_uri

#     try:
#         cfg = BiofilterConfig()
#         if getattr(cfg, "db_uri", None):
#             return cfg.db_uri
#     except FileNotFoundError:
#         pass

#     raise click.UsageError(
#         "Database URI not provided and no .biofilter.toml found.\n"
#         "Use --db-uri or create a .biofilter.toml with a [database] section."
#     )


# def db_uri_option(fn):
#     return click.option(
#         "--db-uri",
#         required=False,
#         type=click.STRING,
#         help="Database URI (or set in .biofilter.toml)",
#     )(fn)


# def try_resolve_db_uri(db_uri: str | None) -> str | None:
#     if db_uri:
#         return db_uri
#     try:
#         cfg = BiofilterConfig()
#         return getattr(cfg, "db_uri", None)
#     except FileNotFoundError:
#         return None

# biofilter/cli/common.py
from __future__ import annotations
import click
from biofilter.utils.config import BiofilterConfig


def try_resolve_db_uri(cli_db_uri: str | None) -> str | None:
    if cli_db_uri:
        return cli_db_uri
    try:
        cfg = BiofilterConfig()
        return getattr(cfg, "db_uri", None)
    except FileNotFoundError:
        return None


def resolve_db_uri(cli_db_uri: str | None) -> str:
    db_uri = try_resolve_db_uri(cli_db_uri)
    if db_uri:
        return db_uri
    raise click.UsageError(
        "DB not set. Use --db-uri or create a .biofilter.toml with [database].db_uri"
    )


def get_ctx_db_uri(ctx: click.Context) -> str | None:
    return (ctx.obj or {}).get("db_uri")


def require_db_uri(ctx: click.Context, local_db_uri: str | None = None) -> str:
    # priority: local option > global option > config
    return resolve_db_uri(local_db_uri or get_ctx_db_uri(ctx))


def local_db_uri_option(fn):
    return click.option(
        "--db-uri",
        required=False,
        type=click.STRING,
        help="Override database URI for this command",
    )(fn)
