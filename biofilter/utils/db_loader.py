# utils/db_loader.py

from importlib import import_module


def load_all_models():
    """
    Import all models modules to ensure SQLAlchemy registers all tables.
    """
    import_module("biofilter.db.models.config_models")
    import_module("biofilter.db.models.etl_models")
    import_module("biofilter.db.models.entity_models")
    import_module("biofilter.db.models.omics_models")
    import_module("biofilter.db.models.curation_models")
    import_module("biofilter.db.models.variants_models")

    # NOTE: Will be removed in the future
    # import_module("biofilter.db.models.loki_models")
