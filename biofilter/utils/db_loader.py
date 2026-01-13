# utils/db_loader.py

from importlib import import_module


def load_all_models():
    """
    Import all models modules to ensure SQLAlchemy registers all tables.
    """
    import_module("biofilter.db.models.model_config")
    import_module("biofilter.db.models.model_etl")
    import_module("biofilter.db.models.model_entities")
    import_module("biofilter.db.models.model_genes")
    import_module("biofilter.db.models.model_curation")
    import_module("biofilter.db.models.model_variants")
    import_module("biofilter.db.models.model_pathways")
    import_module("biofilter.db.models.model_proteins")
    import_module("biofilter.db.models.model_go")
    import_module("biofilter.db.models.model_diseases")
    import_module("biofilter.db.models.model_chemicals")

    # NOTE: Will be removed in the future
    # import_module("biofilter.db.models.loki_models")


def register_imperative_tables(engine):
    """
    Register dialect-specific tables/mappings into the same metadata
    used by the declarative Base.

    We always register the Table object for 'variant_snps' so SQLAlchemy Core
    inserts/updates can target it in any dialect.

    We only map the ORM class on SQLite (where we want VariantSNP.__table__ and
    ORM usage), because on PostgreSQL the table is created as a partitioned
    parent via raw DDL in CreateDBMixin.
    """
    from biofilter.db.base import Base
    from biofilter.db.models.model_variants import VariantSNP, map_variant_snp

#     if engine.dialect.name != "sqlite":
#         return

    map_variant_snp(engine, Base.metadata, model_cls=VariantSNP)


def bootstrap_models(engine):
    """
    One call to prepare all models for the current engine:
    - loads declarative models
    - registers any imperative/dialect-specific tables
    """
    load_all_models()
    register_imperative_tables(engine)
