import pytest


def test_update_runs_successfully(biofilter_instance, mock_etl_manager):
    bf = biofilter_instance

    # Call Update Method
    result = bf.update(source_system=["HGNC"])

    # Chack if the update method returns True
    assert result is True
    mock_etl_manager.start_process.assert_called_once_with(
        source_system=["HGNC"],
        download_path=bf.settings.get("download_path"),
        processed_path=bf.settings.get("processed_path"),
    )


def test_update_without_db_raises_error():
    from biofilter.biofilter import Biofilter

    bf = Biofilter()
    bf.db = None  # Force DB connect

    with pytest.raises(RuntimeError) as exc:
        bf.update(source_system=["HGNC"])

    assert "Database not connected" in str(exc.value)
