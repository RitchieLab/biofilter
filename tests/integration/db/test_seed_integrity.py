from __future__ import annotations

import json
from pathlib import Path

import pytest

from biofilter import Biofilter
from biofilter.modules.db.models import (
    BiofilterMetadata,
    ETLDataSource,
    ETLSourceSystem,
    GenomeAssembly,
    OmicStatus,
    SystemConfig,
)


@pytest.mark.integration
def test_seed_minimum_reference_data_exists(sqlite_seeded_db_uri):
    bf = Biofilter(db_uri=sqlite_seeded_db_uri, debug_mode=False)

    with bf.db.get_session() as session:
        assert session.query(SystemConfig).count() >= 1
        assert session.query(BiofilterMetadata).count() >= 1
        assert session.query(ETLSourceSystem).count() >= 1
        assert session.query(ETLDataSource).count() >= 1
        assert session.query(OmicStatus).count() >= 1
        assert session.query(GenomeAssembly).count() >= 25

        assert (
            session.query(SystemConfig)
            .filter(SystemConfig.key == "download_path")
            .one_or_none()
            is not None
        )
        assert (
            session.query(ETLSourceSystem)
            .filter(ETLSourceSystem.name == "NCBI")
            .one_or_none()
            is not None
        )


@pytest.mark.integration
def test_seed_upgrade_is_idempotent(sqlite_seeded_db_uri):
    bf = Biofilter(db_uri=sqlite_seeded_db_uri, debug_mode=False)

    with bf.db.get_session() as session:
        before = {
            "system_config": session.query(SystemConfig).count(),
            "metadata": session.query(BiofilterMetadata).count(),
            "source_systems": session.query(ETLSourceSystem).count(),
            "data_sources": session.query(ETLDataSource).count(),
            "omic_status": session.query(OmicStatus).count(),
            "assemblies": session.query(GenomeAssembly).count(),
        }

    # Re-apply seeds directly (idempotent path)
    bf.core.db.upgrade_db(seed_dir="seed")

    with bf.db.get_session() as session:
        after = {
            "system_config": session.query(SystemConfig).count(),
            "metadata": session.query(BiofilterMetadata).count(),
            "source_systems": session.query(ETLSourceSystem).count(),
            "data_sources": session.query(ETLDataSource).count(),
            "omic_status": session.query(OmicStatus).count(),
            "assemblies": session.query(GenomeAssembly).count(),
        }

    assert after == before


@pytest.mark.integration
def test_seed_upgrade_reconciles_modified_seeded_values(sqlite_seeded_db_uri):
    bf = Biofilter(db_uri=sqlite_seeded_db_uri, debug_mode=False)
    mutated_value = "/tmp/not-from-seed"
    expected_seed_value = "./biofilter_data/raw/"

    with bf.db.get_session() as session:
        row = (
            session.query(SystemConfig)
            .filter(SystemConfig.key == "download_path")
            .one()
        )
        row.value = mutated_value
        session.commit()

    with bf.db.get_session() as session:
        current = (
            session.query(SystemConfig)
            .filter(SystemConfig.key == "download_path")
            .one()
        )
        assert current.value == mutated_value

    bf.core.db.upgrade_db(seed_dir="seed")

    with bf.db.get_session() as session:
        restored = (
            session.query(SystemConfig)
            .filter(SystemConfig.key == "download_path")
            .one()
        )
        assert restored.value == expected_seed_value


@pytest.mark.integration
def test_seed_data_sources_fk_resolution(sqlite_seeded_db_uri):
    seed_file = (
        Path(__file__).resolve().parents[3]
        / "biofilter"
        / "modules"
        / "db"
        / "seed"
        / "initial_data_sources.json"
    )
    payload = json.loads(seed_file.read_text(encoding="utf-8"))
    seeded_rows = payload.get("data_sources", [])
    expected_map = {
        row["name"]: row["source_system"]
        for row in seeded_rows
        if row.get("name") and row.get("source_system")
    }

    bf = Biofilter(db_uri=sqlite_seeded_db_uri, debug_mode=False)
    with bf.db.get_session() as session:
        # no ETLDataSource should remain with unresolved FK
        assert (
            session.query(ETLDataSource)
            .filter(ETLDataSource.source_system_id.is_(None))
            .count()
            == 0
        )

        db_rows = (
            session.query(ETLDataSource.name, ETLSourceSystem.name)
            .join(
                ETLSourceSystem,
                ETLDataSource.source_system_id == ETLSourceSystem.id,
            )
            .all()
        )

    db_map = {data_source_name: source_system_name for data_source_name, source_system_name in db_rows}  # noqa E501

    assert set(expected_map).issubset(set(db_map))
    for data_source_name, expected_source_system in expected_map.items():
        assert db_map[data_source_name] == expected_source_system
