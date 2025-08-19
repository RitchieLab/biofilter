from db.models.model_etl import DataSource, SourceSystem, ETLProcess
from biofilter.etl.etl_manager import ETLManager


def test_get_etl_process_creates_new_process(db_session):
    source = SourceSystem(name="HGNC")
    db_session.add(source)
    db_session.commit()

    ds = DataSource(
        name="HGNC",
        dtp_script="hgnc",
        source_system_id=source.id,
        data_type="omics",
        format="json",
    )
    db_session.add(ds)
    db_session.commit()

    manager = ETLManager(db_session)
    process = manager.get_etl_process(data_source=ds)

    assert process is not None
    assert isinstance(process, ETLProcess)
    assert process.data_source_id == ds.id
    assert process.extract_status == "pending"
    assert process.global_status == "running"


def test_get_etl_process_returns_existing_process(db_session):
    source = SourceSystem(name="HGNC")
    db_session.add(source)
    db_session.commit()

    ds = DataSource(
        name="HGNC",
        dtp_script="hgnc",
        source_system_id=source.id,
        data_type="omics",
        format="json",
    )
    db_session.add(ds)
    db_session.commit()

    existing_process = ETLProcess(
        data_source_id=ds.id,
        extract_status="completed",
        transform_status="completed",
        load_status="completed",
        global_status="completed",
        dtp_script="hgnc",
    )
    db_session.add(existing_process)
    db_session.commit()

    manager = ETLManager(db_session)
    returned_process = manager.get_etl_process(data_source=ds)

    assert returned_process.id == existing_process.id
    assert returned_process.global_status == "completed"
