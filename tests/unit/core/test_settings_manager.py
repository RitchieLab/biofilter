from __future__ import annotations

from pathlib import Path

import pytest

import biofilter.core.settings_manager as smod


class _KeyField:
    def __eq__(self, other):
        return ("eq", "key", other)


class DummySystemConfig:
    key = _KeyField()

    def __init__(self, key: str, value: str, type: str = "str"):
        self.key = key
        self.value = value
        self.type = type


class FakeQuery:
    def __init__(self, session):
        self.session = session
        self._key = None

    def filter(self, expr):
        if isinstance(expr, tuple) and expr[:2] == ("eq", "key"):
            self._key = expr[2]
        return self

    def one_or_none(self):
        return self.session.rows.get(self._key)


class FakeSession:
    def __init__(self, rows=None):
        self.rows = dict(rows or {})
        self.added = []
        self.query_calls = 0
        self.commit_calls = 0

    def query(self, model):
        self.query_calls += 1
        return FakeQuery(self)

    def add(self, obj):
        self.added.append(obj)
        self.rows[obj.key] = obj

    def commit(self):
        self.commit_calls += 1


def _mk_mgr(monkeypatch, rows=None):
    monkeypatch.setattr(smod, "SystemConfig", DummySystemConfig)
    session = FakeSession(rows=rows)
    return smod.SettingsManager(session), session


def test_get_raw_uses_cache(monkeypatch):
    manager, session = _mk_mgr(
        monkeypatch,
        rows={"download_path": DummySystemConfig("download_path", "./raw", "path")},
    )

    first = manager._get_raw("download_path")
    second = manager._get_raw("download_path")

    assert first is second
    assert session.query_calls == 1


def test_refresh_all_and_single_key(monkeypatch):
    manager, _ = _mk_mgr(monkeypatch)
    manager._cache = {"a": object(), "b": object()}

    manager.refresh("a")
    assert "a" not in manager._cache
    assert "b" in manager._cache

    manager.refresh()
    assert manager._cache == {}


def test_get_returns_default_when_missing(monkeypatch):
    manager, _ = _mk_mgr(monkeypatch, rows={})
    assert manager.get("missing", default="X") == "X"


@pytest.mark.parametrize(
    "raw, expected",
    [("true", True), ("1", True), ("yes", True), ("on", True)],
)
def test_get_bool_truthy(monkeypatch, raw, expected):
    manager, _ = _mk_mgr(
        monkeypatch,
        rows={"k": DummySystemConfig("k", raw, "bool")},
    )
    assert manager.get("k", default=None) is expected


@pytest.mark.parametrize(
    "raw, expected",
    [("false", False), ("0", False), ("no", False), ("off", False)],
)
def test_get_bool_falsey(monkeypatch, raw, expected):
    manager, _ = _mk_mgr(
        monkeypatch,
        rows={"k": DummySystemConfig("k", raw, "bool")},
    )
    assert manager.get("k", default=None) is expected


def test_get_bool_invalid_returns_default(monkeypatch):
    manager, _ = _mk_mgr(
        monkeypatch,
        rows={"k": DummySystemConfig("k", "not-a-bool", "bool")},
    )
    assert manager.get("k", default="fallback") == "fallback"


def test_get_int_float_and_default_on_parse_error(monkeypatch):
    manager, _ = _mk_mgr(
        monkeypatch,
        rows={
            "i": DummySystemConfig("i", "42", "int"),
            "f": DummySystemConfig("f", "3.5", "float"),
            "bad_i": DummySystemConfig("bad_i", "oops", "int"),
        },
    )

    assert manager.get("i") == 42
    assert manager.get("f") == 3.5
    assert manager.get("bad_i", default=-1) == -1


def test_get_path_returns_string_or_path(monkeypatch):
    manager, _ = _mk_mgr(
        monkeypatch,
        rows={"p": DummySystemConfig("p", "./biofilter_data/raw", "path")},
    )

    assert manager.get("p") == "./biofilter_data/raw"
    as_path = manager.get("p", as_path=True)
    assert isinstance(as_path, Path)
    assert str(as_path).endswith("biofilter_data/raw")


def test_get_default_string_type(monkeypatch):
    manager, _ = _mk_mgr(
        monkeypatch,
        rows={"s": DummySystemConfig("s", "value", "str")},
    )
    assert manager.get("s") == "value"


def test_require_returns_value_or_raises(monkeypatch):
    manager, _ = _mk_mgr(
        monkeypatch,
        rows={"ok": DummySystemConfig("ok", "true", "bool")},
    )
    assert manager.require("ok") is True

    with pytest.raises(KeyError, match="Missing or invalid config key"):
        manager.require("missing")


def test_set_creates_new_and_commits(monkeypatch):
    manager, session = _mk_mgr(monkeypatch, rows={})

    manager.set("new_key", 123, commit=True)

    assert "new_key" in session.rows
    assert session.rows["new_key"].value == "123"
    assert session.rows["new_key"].type == "str"
    assert session.commit_calls == 1
    assert session.added


def test_set_updates_existing_and_no_commit_when_disabled(monkeypatch):
    existing = DummySystemConfig("k", "old", "str")
    manager, session = _mk_mgr(monkeypatch, rows={"k": existing})

    manager.set("k", "new", commit=False)

    assert session.rows["k"].value == "new"
    assert session.commit_calls == 0


def test_set_typed_creates_and_updates(monkeypatch):
    manager, session = _mk_mgr(monkeypatch, rows={})

    manager.set_typed("x", 1, "int", commit=True)
    assert session.rows["x"].value == "1"
    assert session.rows["x"].type == "int"
    assert session.commit_calls == 1

    manager.set_typed("x", 2, "float", commit=False)
    assert session.rows["x"].value == "2"
    assert session.rows["x"].type == "float"
    assert session.commit_calls == 1
