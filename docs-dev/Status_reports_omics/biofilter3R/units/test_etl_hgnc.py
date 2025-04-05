# import os
from biofilter.biofilter import Biofilter
from biofilter.biofilter.db.database import Database
from biofilter.biofilter.db.models.etl_models import ETLProcess, DataSource
from biofilter.etl.etl_manager import ETLManager
from sqlalchemy.orm import Session

TEST_DB_URI = "sqlite:///tests/biofilter3R/data/new_biofilter.sqlite"

# def remove_test_db():
#     path = TEST_DB_URI.replace("sqlite:///", "")
#     if os.path.exists(path):
#         os.remove(path)


def test_etl_hgnc_process():
    # remove_test_db()

    db = Database(TEST_DB_URI)
    # biofilter.db.create_db(overwrite=True)

    session: Session = biofilter.db.get_session()

    hgnc_ds = session.query(DataSource).filter_by(name="HGNC").first()
    assert hgnc_ds is not None
    assert hgnc_ds.active is True

    manager = ETLManager(session)
    source_list = ["HGNC"]
    source_list = "HGNC"
    manager.start_process(source_system=source_list)

    etl_process = session.query(ETLProcess).filter_by(data_source_id=hgnc_ds.id).first()
    assert etl_process is not None
    assert etl_process.status == "completed"
    assert etl_process.records_processed > 0

    print(f"✅ ETL process completed with {etl_process.records_processed} records")


def test_etl_hgnc_process_bio_class():
    bf = Biofilter()
    bf.connect_db(TEST_DB_URI)

    source_list = ["HGNC"]

    try:
        result = bf.update(source_system=source_list)
        assert result is True or result is None  # Pode não retornar nada
    except Exception as e:
        assert False, f"ETL process failed: {e}"
