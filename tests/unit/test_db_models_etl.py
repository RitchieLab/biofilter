import datetime

# import pytest
from biofilter.db.models import SourceSystem, DataSource, ETLProcess, ETLLog


def test_create_source_system(db_session):
    source = SourceSystem(
        name="NCBI",
        description="National Center for Biotechnology Information",
        homepage="https://www.ncbi.nlm.nih.gov/",
        active=True,
    )
    db_session.add(source)
    db_session.commit()

    result = db_session.query(SourceSystem).filter_by(name="NCBI").first()
    assert result is not None
    assert result.name == "NCBI"
    assert result.active is True
    assert isinstance(result.created_at, datetime.datetime)


def test_create_data_source(db_session):
    source = SourceSystem(name="Ensembl", active=True)
    db_session.add(source)
    db_session.commit()

    ds = DataSource(
        name="Ensembl Genes",
        source_system_id=source.id,
        data_type="Gene",
        source_url="http://ensembl.org",
        format="GTF",
        grch_version="GRCh38",
        ucschg_version="hg38",
        dtp_version="v102",
        last_status="pending",
        active=True,
    )
    db_session.add(ds)
    db_session.commit()

    result = (
        db_session.query(DataSource).filter_by(name="Ensembl Genes").first()
    )  # noqa: E501
    assert result is not None
    assert result.data_type == "Gene"
    assert result.last_status == "pending"


def test_create_etl_process(db_session):
    source = SourceSystem(name="UCSC", active=True)
    db_session.add(source)
    db_session.commit()

    ds = DataSource(
        name="UCSC Genome Browser",
        source_system_id=source.id,
        data_type="SNP",
        source_url="http://genome.ucsc.edu",
        format="BED",
        grch_version="GRCh37",
        ucschg_version="hg19",
        dtp_version="2025.04",
        last_status="pending",
        active=True,
    )
    db_session.add(ds)
    db_session.commit()

    etl = ETLProcess(
        data_source_id=ds.id,
        global_status="running",
        extract_status="pending",
        transform_status="pending",
        load_status="pending",
        dtp_script="load_snp.py",
    )
    db_session.add(etl)
    db_session.commit()

    result = (
        db_session.query(ETLProcess).filter_by(data_source_id=ds.id).first()
    )  # noqa: E501
    assert result is not None
    assert result.global_status == "running"
    assert result.dtp_script == "load_snp.py"


def test_create_etl_log(db_session):
    etl = ETLProcess(
        data_source_id=1,
        global_status="running",
        dtp_script="dummy.py",
    )
    db_session.add(etl)
    db_session.commit()

    log = ETLLog(
        etl_process_id=etl.id,
        phase="extract",
        action="insert",
        message="Extracted 500 records successfully",
    )
    db_session.add(log)
    db_session.commit()

    result = db_session.query(ETLLog).filter_by(etl_process_id=etl.id).first()
    assert result is not None
    assert result.phase == "extract"
    assert result.action == "insert"
    assert isinstance(result.timestamp, datetime.datetime)
