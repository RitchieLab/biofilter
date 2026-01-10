# biofilter/cli/common.py
import click
from biofilter.utils.config import BiofilterConfig  # <- existe no seu Biofilter.py


def resolve_db_uri(db_uri: str | None) -> str:
    if db_uri:
        return db_uri

    try:
        cfg = BiofilterConfig()
        if getattr(cfg, "db_uri", None):
            return cfg.db_uri
    except FileNotFoundError:
        pass

    raise click.UsageError(
        "Database URI not provided and no .biofilter.toml found.\n"
        "Use --db-uri or create a .biofilter.toml with a [database] section."
    )


def db_uri_option(fn):
    return click.option(
        "--db-uri",
        required=False,
        type=click.STRING,
        help="Database URI (or set in .biofilter.toml)",
    )(fn)


def try_resolve_db_uri(db_uri: str | None) -> str | None:
    if db_uri:
        return db_uri
    try:
        cfg = BiofilterConfig()
        return getattr(cfg, "db_uri", None)
    except FileNotFoundError:
        return None