from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import biofilter.core.components.db_component as dbcomp_mod


class DummyLogger:
    def __init__(self):
        self.messages = []

    def log(self, message, level="INFO"):
        self.messages.append((level, message))


class DummyCore:
    def __init__(self):
        self.db_uri = "sqlite:///initial.db"
        self.db = None
        self.version = "4.1.0-test"
        self.logger = DummyLogger()
        self.etl = SimpleNamespace(rebuild_indexes=lambda **kwargs: (True, "ok"))

    def require_db(self):
        if self.db is None:
            raise RuntimeError("Database not connected.")
        return self.db


def test_connect_updates_uri_and_instantiates_database(monkeypatch):
    core = DummyCore()
    component = dbcomp_mod.DBComponent(core)

    class FakeDatabase:
        def __init__(self, db_uri=None):
            self.db_uri = db_uri

    monkeypatch.setattr(dbcomp_mod, "Database", FakeDatabase)

    db = component.connect(new_uri="sqlite:///new.db")

    assert core.db_uri == "sqlite:///new.db"
    assert isinstance(db, FakeDatabase)
    assert core.db is db


def test_create_db_calls_database_create_db(monkeypatch):
    core = DummyCore()
    component = dbcomp_mod.DBComponent(core)

    calls = {}

    class FakeDatabase:
        def __init__(self):
            self.db_uri = None

        def create_db(self, overwrite=False):
            calls["overwrite"] = overwrite

    monkeypatch.setattr(dbcomp_mod, "Database", FakeDatabase)

    ok = component.create_db(db_uri="sqlite:///created.db", overwrite=True)

    assert ok is True
    assert core.db_uri == "sqlite:///created.db"
    assert isinstance(core.db, FakeDatabase)
    assert calls["overwrite"] is True


def test_upgrade_runs_migration_and_seed_update(monkeypatch):
    core = DummyCore()
    component = dbcomp_mod.DBComponent(core)

    calls = {}

    class FakeDB:
        engine = object()

        def upgrade_db(self, seed_dir):
            calls["seed_dir"] = seed_dir

    core.db = FakeDB()

    def fake_migrate(**kwargs):
        calls["migrate"] = kwargs
        return True

    monkeypatch.setattr(component, "migrate", fake_migrate)

    ok = component.upgrade(seed_dir="custom_seed")

    assert ok is True
    assert calls["migrate"] == {"action": "upgrade", "target": "head", "force": False}
    assert calls["seed_dir"] == "custom_seed"


def test_migrate_raises_if_engine_not_initialized():
    core = DummyCore()
    core.db = SimpleNamespace(engine=None, SessionLocal="S", db_uri="sqlite:///x.db")
    component = dbcomp_mod.DBComponent(core)

    with pytest.raises(RuntimeError, match="engine not initialized"):
        component.migrate()


def test_migrate_calls_run_migration(monkeypatch):
    core = DummyCore()
    core.db = SimpleNamespace(engine=object(), SessionLocal="S", db_uri="sqlite:///x.db")
    component = dbcomp_mod.DBComponent(core)

    captured = {}

    def fake_run_migration(**kwargs):
        captured.update(kwargs)
        return True

    monkeypatch.setattr(dbcomp_mod, "run_migration", fake_run_migration)

    ok = component.migrate(action="status", target="head", force=True)

    assert ok is True
    assert captured["session_factory"] == "S"
    assert captured["engine"] is core.db.engine
    assert captured["db_uri"] == "sqlite:///x.db"
    assert captured["action"] == "status"
    assert captured["target"] == "head"
    assert captured["force"] is True


def test_get_session_passthrough():
    core = DummyCore()
    core.db = SimpleNamespace(get_session=lambda: "SESSION")
    component = dbcomp_mod.DBComponent(core)

    assert component.get_session() == "SESSION"


def test_backup_calls_backup_db(monkeypatch, tmp_path):
    core = DummyCore()
    core.db = SimpleNamespace(engine=object())
    component = dbcomp_mod.DBComponent(core)

    backup_path = tmp_path / "backup.sqlite"
    expected = backup_path.resolve()

    captured = {}

    def fake_backup_db(engine, out_path):
        captured["engine"] = engine
        captured["out_path"] = out_path
        return out_path

    monkeypatch.setattr(dbcomp_mod, "backup_db", fake_backup_db)

    out = component.backup(backup_path)

    assert out == expected
    assert captured["engine"] is core.db.engine
    assert captured["out_path"] == expected


def test_restore_calls_restore_db_and_reconnect(monkeypatch, tmp_path):
    db = SimpleNamespace(engine=object(), connect=lambda check_exists: None)
    core = DummyCore()
    core.db = db
    component = dbcomp_mod.DBComponent(core)

    restored = {}

    def fake_restore_db(engine, inp):
        restored["engine"] = engine
        restored["inp"] = inp

    connect_calls = {}

    def fake_connect(check_exists=True):
        connect_calls["check_exists"] = check_exists

    db.connect = fake_connect
    monkeypatch.setattr(dbcomp_mod, "restore_db", fake_restore_db)

    in_path = tmp_path / "snapshot.dump"
    in_path.write_text("x", encoding="utf-8")

    component.restore(in_path)

    assert restored["engine"] is db.engine
    assert restored["inp"] == in_path.resolve()
    assert connect_calls["check_exists"] is True


def test_export_calls_export_full_clone(monkeypatch, tmp_path):
    core = DummyCore()
    core.db = SimpleNamespace(engine=object())
    component = dbcomp_mod.DBComponent(core)

    out_dir = tmp_path / "bundle"
    expected_bundle = out_dir.resolve()

    captured = {}

    def fake_export_full_clone(engine, out, **kwargs):
        captured["engine"] = engine
        captured["out"] = out
        captured["kwargs"] = kwargs
        return out

    monkeypatch.setattr(dbcomp_mod, "export_full_clone", fake_export_full_clone)

    out = component.export(out_dir=out_dir, fmt="csv", schema_version="4.1.0")

    assert out == expected_bundle
    assert captured["engine"] is core.db.engine
    assert captured["out"] == expected_bundle
    assert captured["kwargs"]["biofilter_version"] == core.version
    assert captured["kwargs"]["schema_version"] == "4.1.0"
    assert captured["kwargs"]["fmt"] == "csv"
    assert captured["kwargs"]["include_tables"] is None
    assert captured["kwargs"]["exclude_tables"] is None


def test_export_passes_table_filters(monkeypatch, tmp_path):
    core = DummyCore()
    core.db = SimpleNamespace(engine=object())
    component = dbcomp_mod.DBComponent(core)

    captured = {}

    def fake_export_full_clone(engine, out, **kwargs):
        captured["engine"] = engine
        captured["out"] = out
        captured["kwargs"] = kwargs
        return out

    monkeypatch.setattr(dbcomp_mod, "export_full_clone", fake_export_full_clone)

    component.export(
        out_dir=tmp_path / "bundle",
        fmt="parquet",
        tables=["variants", "variant_consequences"],
        exclude_tables=["etl_status"],
    )

    assert captured["kwargs"]["include_tables"] == [
        "variants",
        "variant_consequences",
    ]
    assert captured["kwargs"]["exclude_tables"] == ["etl_status"]


def test_import_calls_import_full_clone_and_rebuild_indexes(monkeypatch, tmp_path):
    connect_calls = {}

    def fake_connect(check_exists=True):
        connect_calls["check_exists"] = check_exists

    db = SimpleNamespace(engine=object(), connect=fake_connect)
    core = DummyCore()
    rebuild_calls = {}

    def fake_rebuild_indexes(**kwargs):
        rebuild_calls.update(kwargs)
        return True, "ok"

    core.etl = SimpleNamespace(rebuild_indexes=fake_rebuild_indexes)
    core.db = db
    component = dbcomp_mod.DBComponent(core)

    imported = {}

    def fake_import_full_clone(**kwargs):
        imported.update(kwargs)

    monkeypatch.setattr(dbcomp_mod, "import_full_clone", fake_import_full_clone)

    in_dir = tmp_path / "bundle"
    in_dir.mkdir(parents=True, exist_ok=True)

    component.import_(
        in_dir=in_dir,
        fmt="csv",
        rebuild_indexes=True,
        reset_postgres_sequences=False,
    )

    assert imported["db"] is db
    assert imported["in_dir"] == in_dir.resolve()
    assert imported["fmt"] == "csv"
    assert imported["reset_sequences"] is False
    assert imported["allow_missing_tables"] is False
    assert connect_calls["check_exists"] is True
    assert rebuild_calls == {"groups": None, "drop_first": True}


def test_import_passes_allow_missing_tables(monkeypatch, tmp_path):
    connect_calls = {}

    def fake_connect(check_exists=True):
        connect_calls["check_exists"] = check_exists

    db = SimpleNamespace(engine=object(), connect=fake_connect)
    core = DummyCore()
    core.etl = SimpleNamespace(rebuild_indexes=lambda **kwargs: (True, "ok"))
    core.db = db
    component = dbcomp_mod.DBComponent(core)

    imported = {}

    def fake_import_full_clone(**kwargs):
        imported.update(kwargs)

    monkeypatch.setattr(dbcomp_mod, "import_full_clone", fake_import_full_clone)

    in_dir = tmp_path / "bundle"
    in_dir.mkdir(parents=True, exist_ok=True)

    component.import_(
        in_dir=in_dir,
        fmt="parquet",
        rebuild_indexes=False,
        allow_missing_tables=True,
    )

    assert imported["allow_missing_tables"] is True
    assert connect_calls["check_exists"] is True
