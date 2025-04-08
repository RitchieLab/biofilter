import json
import tempfile
from unittest.mock import MagicMock, patch
from biofilter.db.create_db_mixin import CreateDBMixin


def test_create_db_calls_internal_methods():
    mixin = CreateDBMixin()
    mixin.exists_db = MagicMock(return_value=False)
    mixin.connect = MagicMock()
    mixin._create_tables = MagicMock()
    mixin._seed_all = MagicMock()
    mixin.logger = MagicMock()
    mixin.db_uri = "sqlite:///:memory:"

    mixin.create_db(overwrite=True)

    mixin.connect.assert_called_once_with(check_exists=False)
    mixin._create_tables.assert_called_once()
    mixin._seed_all.assert_called_once()


def test_seed_from_json_loads_data_and_adds_to_session():
    # Cria um JSON temporário
    data = {"test_data": [{"id": 1, "name": "Test"}]}
    with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
        json.dump(data, tmp)
        tmp_path = tmp.name

    # Configura mock do model
    class DummyModel:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    dummy_module = MagicMock()
    dummy_module.Dummy = DummyModel

    mixin = CreateDBMixin()
    mixin.logger = MagicMock()
    mixin.get_session = MagicMock()
    mock_session = MagicMock()
    mixin.get_session.return_value.__enter__.return_value = mock_session

    with patch(
        "biofilter.db.create_db_mixin.import_module", return_value=dummy_module
    ):  # noqa: E501
        mixin._seed_from_json(tmp_path, "dummy", "Dummy", key="test_data")

    assert mock_session.add.call_count == 1
    mock_session.commit.assert_called_once()
