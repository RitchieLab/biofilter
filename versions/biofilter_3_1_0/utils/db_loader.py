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
