"""
biofilter/alembic/env.py

Alembic environment script for Biofilter.

Goals:
- Work both in repo and when installed from wheel/site-packages.
- Ensure Biofilter models + imperative tables are loaded before autogenerate runs.
- Prevent Alembic autogenerate from trying to manage PostgreSQL partitions like:
  variant_snps_chr_1..25 (created by our own DDL).
- Allow overriding DB URI via env var: BIOFILTER_DB_URI
"""

from __future__ import annotations

import os
from logging.config import fileConfig
from typing import Any

from alembic import context
from sqlalchemy import engine_from_config, pool

# ---------------------------------------------------------------------
# Alembic Config
# ---------------------------------------------------------------------

config = context.config

# Override sqlalchemy.url from environment variable if set
db_uri = os.getenv("BIOFILTER_DB_URI")
if db_uri:
    config.set_main_option("sqlalchemy.url", db_uri)

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# ---------------------------------------------------------------------
# Target metadata (Biofilter)
# ---------------------------------------------------------------------
# IMPORTANT:
# - Use the single canonical Base used by the project.
# - Do not import multiple Bases from different modules.
from biofilter.modules.db.base import Base  # noqa: E402
from biofilter.utils.db_loader import bootstrap_models  # noqa: E402

# Imperative table mapping (partitioned parent, etc.)
# from biofilter.modules.db.models.model_variants import map_variant_snp  # noqa E402
from biofilter.modules.db.models.model_variants import (
    map_variant_masters,
    map_variant_molecular_effects,
    map_variant_effect_predictions,
    map_variant_regulatory_elements,
    map_variant_gene_regulatory_evidence,
)  # noqa: E402

target_metadata = Base.metadata


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def _ensure_metadata_loaded(connection) -> None:
    """
    Ensure all declarative models + imperative tables are registered
    into Base.metadata before Alembic compares DB vs metadata.
    """
    engine = connection.engine

    # 1) Load/import all declarative models used by Biofilter
    #    (this must import your new modules/models too)
    bootstrap_models(engine)

    # 2) Register imperative tables into the same metadata
    #    (e.g., variant_snps parent table)
    # map_variant_snp(engine, Base.metadata)
    map_variant_masters(engine, Base.metadata)
    map_variant_molecular_effects(engine, Base.metadata)
    map_variant_effect_predictions(engine, Base.metadata)
    map_variant_regulatory_elements(engine, Base.metadata)
    map_variant_gene_regulatory_evidence(engine, Base.metadata)


def _include_object(
    object_: Any,
    name: str | None,
    type_: str,
    reflected: bool,
    compare_to: Any,
) -> bool:
    """
    Filter objects that must NOT be managed by Alembic autogenerate.

    - PostgreSQL partitions are created via raw DDL in CreateDBMixin,
      so Alembic should ignore them.
    """
    if not name:
        return True

    # Ignore Postgres partition tables (DB-managed)
    if type_ == "table" and name.startswith("variant_snps_chr_"):
        return False

    # Optional: if your parent partitioned table is managed via custom DDL and
    # autogenerate keeps trying to rewrite it, uncomment to ignore it too.
    # if type_ == "table" and name == "variant_snps":
    #     return False

    # Optional: ignore indexes created outside Alembic (example)
    # if type_ == "index" and name == "ix_entity_aliases_alias_norm_trgm":
    #     return False

    return True


def _configure_context_offline(url: str) -> None:
    """
    Configure Alembic context for offline mode (SQL script generation).
    """
    context.configure(
        url=url,
        target_metadata=target_metadata,
        include_object=_include_object,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )


def _configure_context_online(connection) -> None:
    """
    Configure Alembic context for online mode (apply migrations).
    """
    _ensure_metadata_loaded(connection)

    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_object=_include_object,
        compare_type=True,
        compare_server_default=True,
        # render_as_batch=True  # use for SQLite migrations if needed
    )


# ---------------------------------------------------------------------
# Entrypoints
# ---------------------------------------------------------------------
def run_migrations_offline() -> None:
    """
    Run migrations in 'offline' mode.

    Calls to context.execute() emit SQL to the script output.
    """
    url = config.get_main_option("sqlalchemy.url")
    if not url:
        raise RuntimeError(
            "Missing sqlalchemy.url. Provide via alembic.ini or BIOFILTER_DB_URI env var."
        )

    _configure_context_offline(url)

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """
    Run migrations in 'online' mode.

    Creates an Engine and associates a connection with the context.
    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        _configure_context_online(connection)

        with context.begin_transaction():
            context.run_migrations()


# ---------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
