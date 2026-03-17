from __future__ import annotations

import biofilter.modules.db.models  # noqa: F401
from biofilter.modules.db.base import Base
from biofilter.modules.db.models.model_config import SystemConfig
from biofilter.modules.db.models.model_etl import ETLDataSource, ETLPackage


def test_models_register_expected_tables_in_metadata():
    table_names = set(Base.metadata.tables.keys())

    expected = {
        "system_config",
        "biofilter_metadata",
        "etl_source_systems",
        "etl_data_sources",
        "etl_packages",
        "gene_masters",
        "variant_consequences",
    }

    missing = expected - table_names
    assert not missing, f"Missing mapped tables: {sorted(missing)}"


def test_system_config_key_is_unique_and_not_nullable():
    assert SystemConfig.__table__.columns["key"].unique is True
    assert SystemConfig.__table__.columns["key"].nullable is False


def test_etl_models_basic_contracts():
    assert ETLDataSource.__table__.columns["name"].unique is True
    assert ETLDataSource.__table__.columns["source_system_id"].nullable is False
    assert ETLPackage.__table__.columns["data_source_id"].nullable is False
