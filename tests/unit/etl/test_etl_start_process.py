import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from biofilter.etl.etl_manager import ETLManager
from db.models.model_etl import SourceSystem, DataSource, ETLProcess

"""
🧪 Extensões possíveis:
Criar testes específicos para falha em cada fase (extract, transform, load) e
    garantir que os status sejam marcados como "failed" e que load não é
    executado se transform falhar.

Verificar se ETLLog é criado.

Simular extract com mesmo hash e ver se transform/load são pulados.

"""


@pytest.fixture
def sample_data_source(db_session):
    system = SourceSystem(name="HGNC")
    db_session.add(system)
    db_session.commit()

    ds = DataSource(
        name="HGNC",
        active=True,
        dtp_script="hgnc",
        source_system_id=system.id,
        data_type="omics",
        format="json",
    )
    db_session.add(ds)
    db_session.commit()
    return ds


@patch("biofilter.etl.etl_manager.importlib.import_module")
def test_start_process_flow(
    mock_import_module, db_session, sample_data_source
):  # noqa: E501
    # Mock the module and its DTP class methods
    mock_dtp_class = MagicMock()
    mock_dtp_instance = mock_dtp_class.return_value

    # Retornos válidos para todas as etapas
    mock_dtp_instance.extract.return_value = (True, "Extract OK", "hash123")
    df_mock = pd.DataFrame({"gene": [1]})  # 👈 aqui
    mock_dtp_instance.transform.return_value = (df_mock, True, "Transform OK")
    mock_dtp_instance.load.return_value = (100, True, "Load OK")

    mock_import_module.return_value.DTP = mock_dtp_class

    manager = ETLManager(db_session)
    manager.start_process(
        source_system=["HGNC"],
        download_path="/tmp/raw",
        processed_path="/tmp/processed",
    )  # noqa: E501

    process = (
        db_session.query(ETLProcess)
        .filter_by(data_source_id=sample_data_source.id)
        .first()
    )  # noqa: E501

    assert process is not None
    assert process.extract_status == "completed"
    assert process.transform_status == "completed"
    assert process.load_status == "completed"
    assert process.global_status == "completed"

    mock_dtp_instance.extract.assert_called_once()
    mock_dtp_instance.transform.assert_called_once()
    mock_dtp_instance.load.assert_called_once()


@pytest.mark.parametrize("stage_to_fail", ["extract", "transform", "load"])
@patch("biofilter.etl.etl_manager.importlib.import_module")
def test_start_process_failure_paths(
    mock_import_module, db_session, sample_data_source, stage_to_fail
):  # noqa: E501
    """
    Test controlled failure in each ETL stage: extract, transform, load.
    """

    # Setup mock DTP
    mock_dtp_class = MagicMock()
    mock_dtp_instance = mock_dtp_class.return_value

    # Configure stage results
    if stage_to_fail == "extract":
        mock_dtp_instance.extract.return_value = (
            False,
            "Extract failed",
            None,
        )  # noqa: E501
    else:
        mock_dtp_instance.extract.return_value = (
            True,
            "Extract OK",
            "hash123",
        )  # noqa: E501

    if stage_to_fail == "transform":
        mock_dtp_instance.transform.return_value = (
            pd.DataFrame(),
            False,
            "Transform failed",
        )
    else:
        mock_dtp_instance.transform.return_value = (
            pd.DataFrame({"gene": [1]}),
            True,
            "Transform OK",
        )

    if stage_to_fail == "load":
        mock_dtp_instance.load.return_value = (0, False, "Load failed")
    else:
        mock_dtp_instance.load.return_value = (100, True, "Load OK")

    mock_import_module.return_value.DTP = mock_dtp_class

    # Run ETL
    manager = ETLManager(db_session)
    manager.start_process(
        source_system=["HGNC"],
        download_path="/tmp/raw",
        processed_path="/tmp/processed",
    )

    # Assertions
    process: ETLProcess = (
        db_session.query(ETLProcess)
        .filter_by(data_source_id=sample_data_source.id)
        .first()
    )

    assert process is not None
    if stage_to_fail == "extract":
        assert process.extract_status == "failed"
        assert process.global_status == "failed"
    elif stage_to_fail == "transform":
        assert process.extract_status == "completed"
        assert process.transform_status == "failed"
        assert process.global_status == "failed"
    elif stage_to_fail == "load":
        assert process.extract_status == "completed"
        assert process.transform_status == "completed"
        assert process.load_status == "failed"
        assert process.global_status == "failed"


@patch("biofilter.etl.etl_manager.importlib.import_module")
def test_etl_skips_transform_load_on_same_hash(
    mock_import_module, db_session, sample_data_source
):  # noqa: E501
    """
    Check if transform() and load() are skipped when the hash of the extracted
    file does not change.
    """

    # Create process with known hash
    process = ETLProcess(
        data_source_id=sample_data_source.id,
        global_status="completed",
        extract_status="completed",
        transform_status="completed",
        load_status="completed",
        raw_data_hash="hash123",  # já conhecido
        dtp_script=sample_data_source.dtp_script,
    )
    db_session.add(process)
    db_session.commit()

    # Mocks
    mock_dtp_class = MagicMock()
    mock_dtp_instance = mock_dtp_class.return_value

    # Simulate extract() returning the same hash
    mock_dtp_instance.extract.return_value = (True, "No changes", "hash123")
    # DO NOT CALL transform() and load()
    mock_dtp_instance.transform.return_value = (
        pd.DataFrame(),
        True,
        "Transform OK",
    )  # noqa: E501
    mock_dtp_instance.load.return_value = (100, True, "Load OK")

    mock_import_module.return_value.DTP = mock_dtp_class

    # Run ETL process
    manager = ETLManager(db_session)
    manager.start_process(
        source_system=["HGNC"],
        download_path="/tmp/raw",
        processed_path="/tmp/processed",
    )

    # Process reloaded to get updated status
    updated_process: ETLProcess = (
        db_session.query(ETLProcess)
        .filter_by(data_source_id=sample_data_source.id)
        .first()
    )

    # Check if transform and load were marked as 'completed' without running
    assert updated_process.extract_status == "completed"
    assert updated_process.transform_status == "completed"
    assert updated_process.load_status == "completed"

    # Check if transform() and load() were not called
    mock_dtp_instance.transform.assert_not_called()
    mock_dtp_instance.load.assert_not_called()
