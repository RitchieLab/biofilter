import pytest
from unittest.mock import MagicMock
from biofilter.biofilter import Biofilter

# from biofilter.etl.etl_manager import ETLManager


@pytest.fixture
def mock_biofilter(monkeypatch):
    bf = Biofilter()
    bf.db = MagicMock()
    bf.db.get_session.return_value = MagicMock()

    bf._settings = {"download_path": "/tmp/raw", "processed_path": "/tmp/processed"}

    mock_manager = MagicMock()
    monkeypatch.setattr(
        "biofilter.biofilter.ETLManager", lambda session: mock_manager
    )  # noqa: E501
    return bf, mock_manager


def test_restart_etl_by_data_source(mock_biofilter):
    bf, mock_manager = mock_biofilter

    bf.restart_etl(data_source=["HGNC"], delete_files=True)

    mock_manager.restart_etl_process.assert_called_once_with(
        data_source=["HGNC"],
        source_system=None,
        download_path="/tmp/raw",
        processed_path="/tmp/processed",
        delete_files=True,
    )


def test_restart_etl_by_source_system(mock_biofilter):
    bf, mock_manager = mock_biofilter

    bf.restart_etl(source_system=["ENSEMBL"], delete_files=False)

    mock_manager.restart_etl_process.assert_called_once_with(
        data_source=None,
        source_system=["ENSEMBL"],
        download_path="/tmp/raw",
        processed_path="/tmp/processed",
        delete_files=False,
    )
