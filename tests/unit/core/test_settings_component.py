from __future__ import annotations

from contextlib import contextmanager

import biofilter.core.components.settings_component as scmod


class DummyLogger:
    def __init__(self):
        self.messages = []

    def log(self, message, level="INFO"):
        self.messages.append((level, message))


class FakeDB:
    def __init__(self, session_obj):
        self._session_obj = session_obj
        self.get_session_calls = 0

    @contextmanager
    def get_session(self):
        self.get_session_calls += 1
        yield self._session_obj


class DummyCore:
    def __init__(self, db):
        self._db = db
        self.logger = DummyLogger()
        self._settings_manager = None

    def require_db(self):
        return self._db


def test_get_manager_initializes_once_and_caches(monkeypatch):
    created = {"count": 0, "session": None}

    class FakeSettingsManager:
        def __init__(self, session):
            created["count"] += 1
            created["session"] = session

    monkeypatch.setattr(scmod, "SettingsManager", FakeSettingsManager)

    session_obj = object()
    db = FakeDB(session_obj)
    core = DummyCore(db)
    comp = scmod.SettingsComponent(core)

    m1 = comp._get_manager()
    m2 = comp._get_manager()

    assert m1 is m2
    assert created["count"] == 1
    assert created["session"] is session_obj
    assert db.get_session_calls == 1
    assert any("Initializing settings manager" in msg for _, msg in core.logger.messages)


def test_get_and_set_delegate_to_manager(monkeypatch):
    class FakeSettingsManager:
        def __init__(self):
            self.calls = []

        def get(self, key, default=None):
            self.calls.append(("get", key, default))
            return "value-from-manager"

        def set(self, key, value):
            self.calls.append(("set", key, value))
            return "set-ok"

    mgr = FakeSettingsManager()

    session_obj = object()
    db = FakeDB(session_obj)
    core = DummyCore(db)
    comp = scmod.SettingsComponent(core)

    monkeypatch.setattr(comp, "_get_manager", lambda: mgr)

    out_get = comp.get("download_path", default="fallback")
    out_set = comp.set("download_path", "/tmp/raw")

    assert out_get == "value-from-manager"
    assert out_set == "set-ok"
    assert mgr.calls == [
        ("get", "download_path", "fallback"),
        ("set", "download_path", "/tmp/raw"),
    ]
