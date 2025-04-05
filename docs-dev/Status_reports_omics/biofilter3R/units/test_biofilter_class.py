import pytest
from biofilter.biofilter import Biofilter


@pytest.fixture
# def tmp_sqlite_uri(tmp_path_factory):
# data_dir = tmp_path_factory.mktemp("data", numbered=False)
# db_file = data_dir / "test_biofilter.sqlite"
# return f"sqlite:///{db_file}"
def tmp_sqlite_uri():
    return "sqlite:///tests/biofilter3R/data/test_biofilter.sqlite"


def test_biofilter_init_without_db():
    bf = Biofilter()
    assert bf.db is None
    assert bf.db_uri is None


def test_connect_to_invalid_database_raises_error():
    invalid_path = "tests/nonexistent/fake_database.sqlite"
    with pytest.raises(ValueError) as exc_info:
        bf = Biofilter(invalid_path)  # noqa F841
    assert "Database not found" in str(exc_info.value)


def test_biofilter_connect_db(tmp_sqlite_uri):
    bf = Biofilter()
    bf.connect_db(tmp_sqlite_uri)
    assert bf.db is not None
    assert bf.db_uri == tmp_sqlite_uri  # NOTE get error in future


# def test_biofilter_settings_access(tmp_sqlite_uri):
#     bf = Biofilter(tmp_sqlite_uri)
#     settings = bf.settings
#     assert settings is not None
#     assert hasattr(settings, "get")


# def test_biofilter_settings_without_db():
#     bf = Biofilter()
#     with pytest.raises(RuntimeError, match="You must connect to a database first."):
#         _ = bf.settings


# def test_logger_message_when_accessing_settings(tmp_sqlite_uri, caplog):
#     bf = Biofilter(tmp_sqlite_uri)
#     with caplog.at_level("INFO"):
#         _ = bf.settings
#     assert "⚙️ Initializing settings..." in caplog.text
