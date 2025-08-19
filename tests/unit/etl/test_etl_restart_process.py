import pytest
from db.models.model_etl import ETLProcess, DataSource, SourceSystem
from biofilter.etl.etl_manager import ETLManager


@pytest.fixture
def setup_sources(db_session):
    source_system = SourceSystem(name="HGNC")
    db_session.add(source_system)
    db_session.flush()

    ds = DataSource(
        name="HGNC",
        active=True,
        dtp_script="hgnc",
        data_type="master",
        format="csv",
        source_system_id=source_system.id,
    )
    db_session.add(ds)
    db_session.commit()
    return ds


def test_restart_etl_creates_process(db_session, setup_sources):
    manager = ETLManager(db_session)
    result = manager.restart_etl_process("HGNC")

    assert result is True

    process = (
        db_session.query(ETLProcess)
        .filter_by(data_source_id=setup_sources.id)
        .first()  # noqa: E501
    )

    assert process is not None
    assert process.global_status == "pending"
    assert process.extract_status == "pending"
    assert process.transform_status == "pending"
    assert process.load_status == "pending"


def test_restart_etl_updates_existing_process(db_session, setup_sources):
    process = ETLProcess(
        data_source_id=setup_sources.id,
        global_status="completed",
        extract_status="completed",
        transform_status="completed",
        load_status="completed",
        dtp_script="hgnc",
    )
    db_session.add(process)
    db_session.commit()

    manager = ETLManager(db_session)
    result = manager.restart_etl_process("HGNC")

    assert result is True

    updated = (
        db_session.query(ETLProcess)
        .filter_by(data_source_id=setup_sources.id)
        .first()  # noqa: E501
    )

    assert updated.global_status == "pending"
    assert updated.extract_status == "pending"
    assert updated.transform_status == "pending"
    assert updated.load_status == "pending"


def test_restart_etl_datasource_not_found(db_session):
    manager = ETLManager(db_session)
    result = manager.restart_etl_process("NON_EXISTENT")
    assert result is False
