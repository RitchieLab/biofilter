from biofilter.db.models.etl_models import (
    DataSource,
    SourceSystem,
    ETLProcess,
    ETLLog,
)  # noqa: E501
from biofilter.etl.etl_manager import ETLManager


def test_restart_etl_process_creates_new_process(db_session, tmp_path):
    source = SourceSystem(name="HGNC")
    db_session.add(source)
    db_session.commit()

    ds = DataSource(
        name="HGNC",
        dtp_script="hgnc",
        source_system_id=source.id,
        data_type="omics",
        format="json",
    )  # noqa: E501
    db_session.add(ds)
    db_session.commit()

    manager = ETLManager(db_session)
    result = manager.restart_etl_process(
        data_source="HGNC", download_path=str(tmp_path), processed_path=str(tmp_path)
    )  # noqa E501

    process = (
        db_session.query(ETLProcess).filter_by(data_source_id=ds.id).first()
    )  # noqa: E501
    log = db_session.query(ETLLog).filter_by(etl_process_id=process.id).first()

    assert result is True
    assert process is not None
    assert log is not None
    assert process.extract_status == "pending"
    assert log.action == "restart"


def test_restart_etl_process_handles_no_datasource(db_session):
    manager = ETLManager(db_session)
    result = manager.restart_etl_process(data_source="INVALID")
    assert result is False


def test_restart_etl_process_resets_existing_process(db_session, tmp_path):
    source = SourceSystem(name="HGNC")
    db_session.add(source)
    db_session.commit()

    ds = DataSource(
        name="HGNC",
        dtp_script="hgnc",
        source_system_id=source.id,
        data_type="omics",
        format="json",
    )  # noqa: E501
    db_session.add(ds)
    db_session.commit()

    process = ETLProcess(
        data_source_id=ds.id,
        extract_status="completed",
        transform_status="completed",
        load_status="completed",
        dtp_script="dtp_test",
    )  # noqa: E501
    db_session.add(process)
    db_session.commit()

    manager = ETLManager(db_session)
    result = manager.restart_etl_process(
        data_source="HGNC", download_path=str(tmp_path), processed_path=str(tmp_path)
    )  # noqa E501

    updated_process = (
        db_session.query(ETLProcess).filter_by(data_source_id=ds.id).first()
    )  # noqa: E501
    assert result is True
    assert updated_process.extract_status == "pending"
    assert updated_process.transform_status == "pending"
    assert updated_process.load_status == "pending"
