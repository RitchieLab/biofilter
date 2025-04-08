from biofilter.db.database import Database
from unittest.mock import MagicMock


def test_connect_creates_engine_and_session(tmp_path):
    db_path = tmp_path / "test.sqlite"
    db_uri = f"sqlite:///{db_path}"

    db = Database()
    db.connect(new_uri=db_uri, check_exists=False)

    assert db.engine is not None
    assert db.session is not None
    assert db.connected is True


def test_exists_db_returns_true_for_existing_sqlite(tmp_path):
    db_file = tmp_path / "exists.sqlite"
    db_file.write_text("")  # cria o arquivo vazio

    db = Database(str(db_file))
    assert db.exists_db() is True


def test_get_session_returns_session(tmp_path):
    db_path = tmp_path / "test.sqlite"
    db_uri = f"sqlite:///{db_path}"

    db = Database()
    db.connect(new_uri=db_uri, check_exists=False)
    session = db.get_session()

    assert session is not None


def test_get_session_returns_none_if_not_connected():
    db = Database()
    db.logger = MagicMock()  # evitar print
    assert db.get_session() is None
