# utils/db_loader.py
from __future__ import annotations
from importlib import import_module
from typing import Callable, Iterable, Tuple

# -------------------------------------------------------------------------
# ORM Models Register
# -------------------------------------------------------------------------


def load_all_models():
    """
    Import all models modules to ensure SQLAlchemy registers all tables.
    """
    import_module("biofilter.modules.db.models.model_config")
    import_module("biofilter.modules.db.models.model_etl")
    import_module("biofilter.modules.db.models.model_entities")
    import_module("biofilter.modules.db.models.model_genes")
    import_module("biofilter.modules.db.models.model_curation")
    import_module("biofilter.modules.db.models.model_variants")
    import_module("biofilter.modules.db.models.model_pathways")
    import_module("biofilter.modules.db.models.model_proteins")
    import_module("biofilter.modules.db.models.model_go")
    import_module("biofilter.modules.db.models.model_diseases")
    import_module("biofilter.modules.db.models.model_chemicals")

    # NOTE: Will be removed in the future
    # import_module("biofilter.modules.db.models.loki_models")


# -------------------------------------------------------------------------
# Core Tables Register
# -------------------------------------------------------------------------
def register_imperative_tables(engine) -> None:
    """
    Register dialect-specific Core tables into Base.metadata.

    Notes:
    - Postgres partitioned parents are created via raw DDL in CreateDBMixin.
    - We still register Table objects here for SQLAlchemy Core usage.
    """
    from biofilter.modules.db.base import Base
    from biofilter.modules.db.models.model_variants import (
        map_variant_masters,
        map_variant_molecular_effects,
        map_variant_effect_predictions,
        map_variant_regulatory_elements,
        map_variant_gene_regulatory_evidence,
    )

    registry: list[Tuple[str, Callable]] = [
        ("variant_masters", map_variant_masters),
        ("variant_molecular_effects", map_variant_molecular_effects),
        ("variant_effect_predictions", map_variant_effect_predictions),
        ("variant_regulatory_elements", map_variant_regulatory_elements),
        ("variant_gene_regulatory_evidence", map_variant_gene_regulatory_evidence),
    ]

    # Remove stale definitions first (important if bootstrap_models is called multiple times)
    for table_name, _ in registry:
        existing = Base.metadata.tables.get(table_name)
        if existing is not None:
            Base.metadata.remove(existing)

    # Register
    for table_name, map_fn in registry:
        tbl = map_fn(engine, Base.metadata)

        # Hard validation: mapper must return the expected table name
        if tbl.name != table_name:
            raise RuntimeError(
                f"Imperative table mapper returned unexpected name: "
                f"expected='{table_name}' got='{tbl.name}' from {map_fn.__name__}"
            )


# -------------------------------------------------------------------------
# Start ORM and Tables im the Metadata
# -------------------------------------------------------------------------
def bootstrap_models(engine):
    """
    One call to prepare all models for the current engine:
    - loads declarative models
    - registers any imperative/dialect-specific tables
    """
    load_all_models()
    register_imperative_tables(engine)
