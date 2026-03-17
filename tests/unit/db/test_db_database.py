from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import Column, Integer, Table

import biofilter.modules.db.database as dbmod
from biofilter.modules.db.base import Base


class DummyLogger:
    def __init__(self, *args, **kwargs):
        self.messages = []

    def log(self, message, level="INFO"):
        self.messages.append((level, message))


def test_normalize_uri_converts_filesystem_path_to_sqlite(monkeypatch, tmp_path):
    monkeypatch.setattr(dbmod, "Logger", DummyLogger)
    db = dbmod.Database(db_uri=None)

    relative_path = str(tmp_path / "my_db.sqlite")
    normalized = db._normalize_uri(relative_path)

    assert normalized.startswith("sqlite:///")
    assert normalized.endswith("my_db.sqlite")


def test_normalize_uri_keeps_uri_with_scheme(monkeypatch):
    monkeypatch.setattr(dbmod, "Logger", DummyLogger)
    db = dbmod.Database(db_uri=None)

    uri = "postgresql+psycopg2://user:pass@localhost/biofilter_dev"
    assert db._normalize_uri(uri) == uri


def test_exists_db_sqlite_true_and_false(monkeypatch, tmp_path):
    monkeypatch.setattr(dbmod, "Logger", DummyLogger)

    existing = tmp_path / "existing.sqlite"
    existing.write_text("", encoding="utf-8")

    db = dbmod.Database(db_uri=None)
    db.db_uri = f"sqlite:///{existing}"
    assert db.exists_db() is True

    missing = tmp_path / "missing.sqlite"
    db.db_uri = f"sqlite:///{missing}"
    assert db.exists_db() is False


def test_get_session_returns_none_when_not_connected(monkeypatch):
    monkeypatch.setattr(dbmod, "Logger", DummyLogger)
    db = dbmod.Database(db_uri=None)

    assert db.get_session() is None


def test_connect_initializes_engine_session_and_state(monkeypatch, tmp_path):
    monkeypatch.setattr(dbmod, "Logger", DummyLogger)

    created = {}

    class FakeEngine:
        def __init__(self, uri):
            self.uri = uri
            self.disposed = False

        def dispose(self):
            self.disposed = True

    def fake_create_engine(uri, future=True):
        created["uri"] = uri
        created["future"] = future
        return FakeEngine(uri)

    def fake_bootstrap_models(engine):
        created["bootstrapped"] = engine

    def fake_sessionmaker(**kwargs):
        created["sessionmaker_kwargs"] = kwargs
        return "SESSION_FACTORY"

    monkeypatch.setattr(dbmod, "create_engine", fake_create_engine)
    monkeypatch.setattr(dbmod, "bootstrap_models", fake_bootstrap_models)
    monkeypatch.setattr(dbmod, "sessionmaker", fake_sessionmaker)

    db_file = tmp_path / "db_for_connect.sqlite"
    db = dbmod.Database(db_uri=None)
    db.connect(new_uri=str(db_file), check_exists=False)

    assert db.connected is True
    assert db.engine is not None
    assert db.SessionLocal == "SESSION_FACTORY"
    assert created["uri"].startswith("sqlite:///")
    assert created["future"] is True
    assert created["bootstrapped"] is db.engine
    assert created["sessionmaker_kwargs"]["bind"] is db.engine


def test_table_raises_when_not_connected(monkeypatch):
    monkeypatch.setattr(dbmod, "Logger", DummyLogger)
    db = dbmod.Database(db_uri=None)
    db.engine = None

    with pytest.raises(RuntimeError, match="Database not connected"):
        db.table("system_config")


def test_table_uses_metadata_and_cache(monkeypatch):
    monkeypatch.setattr(dbmod, "Logger", DummyLogger)
    db = dbmod.Database(db_uri=None)
    db.engine = object()

    table_name = f"unit_table_{uuid4().hex}"
    table = Table(table_name, Base.metadata, Column("id", Integer, primary_key=True))

    try:
        first = db.table(table_name)
        second = db.table(table_name)
        assert first is table
        assert second is first
    finally:
        Base.metadata.remove(table)
