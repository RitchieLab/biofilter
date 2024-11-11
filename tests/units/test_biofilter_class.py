import os
import pytest
from unittest.mock import MagicMock, patch
from biofilter_modules.biofilter_class import Biofilter


@pytest.fixture
def mock_options():
    class MockOptions:
        quiet = "no"
        verbose = "yes"
        stdout = "no"
        prefix = "test_prefix"
        overwrite = "yes"

    return MockOptions()


def test_get_version_string():
    # Testa o método getVersionString da classe Biofilter
    expected_version = "2.4.3 (2023-09-20)"  # ajuste conforme necessário
    version_string = Biofilter.getVersionString()
    assert (
        version_string == expected_version
    ), f"Expected version string '{expected_version}' but got '{version_string}'"  # noqa E501


@patch("biofilter_modules.biofilter_class.loki_db")
def test_init_no_options(mock_loki_db, tmp_path):

    # Set up the expected return value for the `getVersionTuple` method
    mock_loki_db.Database.getVersionTuple.return_value = (2, 2, 1, "a", 2)
    mock_loki_db.Database.getVersionString.return_value = "2.2.1a2"

    # Test the initialization without passing `options`
    biofilter = Biofilter()
    # Check if the default options were set
    assert biofilter._options is not None
    assert hasattr(biofilter._options, "quiet")


@patch("biofilter_modules.biofilter_class.loki_db")
def test_init_with_options(mock_loki_db, mock_options, tmp_path):

    # Set up the expected return value for the `getVersionTuple` method
    mock_loki_db.Database.getVersionTuple.return_value = (2, 2, 1, "a", 2)
    mock_loki_db.Database.getVersionString.return_value = "2.2.1a2"

    # Test the initialization with mocked options
    log_path = tmp_path / "test_prefix"
    mock_options.prefix = str(log_path)

    # Create Biofilter instance with test options
    biofilter = Biofilter(options=mock_options)
    assert biofilter._quiet is False
    assert biofilter._verbose is True
    assert biofilter._logFile is not None

    # Check if the log file was created
    assert os.path.exists(str(log_path) + ".log")


@patch("biofilter_modules.biofilter_class.loki_db")
def test_init_loki_version_check(mock_loki_db):

    # Set up the expected return value for the `getVersionTuple` method
    mock_loki_db.Database.getVersionTuple.return_value = (2, 0, 1, "a", 2)
    mock_loki_db.Database.getVersionString.return_value = "2.0.1a2"

    with pytest.raises(SystemExit) as excinfo:
        Biofilter()
    assert "ERROR: LOKI version" in str(excinfo.value)


@patch("biofilter_modules.biofilter_class.loki_db")
def test_database_initialization(mock_loki_db, mock_options, tmp_path):

    # Set up the expected return value for the `getVersionTuple` method
    mock_loki_db.Database = MagicMock()
    mock_loki_db.Database.getVersionTuple.return_value = (2, 2, 1, "a", 2)
    mock_loki_db.Database.getVersionString.return_value = "2.2.1a2"

    # Test the initialization with mocked options
    log_path = tmp_path / "test_prefix"
    mock_options.prefix = str(log_path)

    # Create Biofilter instance with test options
    biofilter = Biofilter(options=mock_options)

    # Check if the temp database was attached
    mock_loki_db.Database().attachTempDatabase.assert_called()
    # Check if the database tables were created
    mock_loki_db.Database().createDatabaseTables.assert_called()

    # Assert that _loki is an instance of the mocked Database
    assert isinstance(
        biofilter._loki, MagicMock
    ), "Expected _loki to be a MagicMock instance."

    # Verify that _loki was set as an instance of `loki_db.Database`
    # mock_loki_db.Database.assert_called_once()  # DB was instantiated once
    # assert (
    #     biofilter._loki is mock_loki_db.Database()
    # ), "Expected _loki to reference the mocked Database instance."

    # Chack if the `Database` class was instantiated exactly 3 times
    assert mock_loki_db.Database.call_count == 3

    # Additional assertions to verify _loki was set up correctly
    # Check if _loki logger was set to the biofilter instance
    biofilter._loki.setLogger.assert_called_once_with(biofilter)

    # Chack if the `attachTempDatabase` and `createDatabaseTables`
    # methods were called
    mock_loki_db.Database().attachTempDatabase.assert_called()
    mock_loki_db.Database().createDatabaseTables.assert_called()
