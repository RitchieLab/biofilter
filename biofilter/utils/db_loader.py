# utils/db_loader.py

from importlib import import_module


# -------------------------------------------------------------------------
# ORM Models Register
# -------------------------------------------------------------------------

def load_all_models():
    """
    Import all models modules to ensure SQLAlchemy registers all tables.
    """
    import_module("biofilter.modules.db.models.model_config")
    import_module("biofilter.modules.db.models.model_etl")
    import_module("biofilter.modules.db.models.model_kdc")
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

def register_imperative_tables(engine):
    """
    Register dialect-specific tables/mappings into the same metadata
    used by the declarative Base.

    We always register the Table object like 'variant_snps' so SQLAlchemy Core
    inserts/updates can target it in any dialect.

    PostgreSQL the Particional table is created as a partitioned parent via raw
    DDL in CreateDBMixin.
    """
    from biofilter.modules.db.base import Base

    # from biofilter.modules.db.models.model_variants import map_variant_snp
    from biofilter.modules.db.models.model_variants import map_variant_master

    # if "variant_snps" in Base.metadata.tables:
    #     Base.metadata.remove(Base.metadata.tables["variant_snps"])
    # tbl = Base.metadata.tables.get("variant_snps")
    tbl = Base.metadata.tables.get("variant_masters")
    if tbl is not None:
        Base.metadata.remove(tbl)

    # map_variant_snp(engine, Base.metadata)
    map_variant_master(engine, Base.metadata)
    # TODO: Extend to other Model like EntityRelationship

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
