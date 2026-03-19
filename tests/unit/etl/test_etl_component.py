from __future__ import annotations

from types import SimpleNamespace

import biofilter.core.components.etl_component as etl_comp_mod


class DummyLogger:
    def __init__(self):
        self.messages = []

    def log(self, message, level="INFO"):
        self.messages.append((level, message))


class DummySettings:
    def __init__(self, values=None):
        self.values = values or {}
        self.calls = []

    def get(self, key, default=None):
        self.calls.append((key, default))
        return self.values.get(key, default)


class DummyCore:
    def __init__(self):
        self.debug_mode = True
        self.logger = DummyLogger()
        self.settings = DummySettings(
            {"download_path": "/tmp/raw", "processed_path": "/tmp/processed"}
        )
        self._db = object()

    def require_db(self):
        return self._db


def test_manager_uses_core_debug_db_and_logger(monkeypatch):
    captured = {}

    class FakeETLManager:
        def __init__(self, debug_mode, db, logger):
            captured["debug_mode"] = debug_mode
            captured["db"] = db
            captured["logger"] = logger

    monkeypatch.setattr(etl_comp_mod, "ETLManager", FakeETLManager)

    core = DummyCore()
    component = etl_comp_mod.ETLComponent(core)
    manager = component._manager()

    assert isinstance(manager, FakeETLManager)
    assert captured["debug_mode"] is True
    assert captured["db"] is core.require_db()
    assert captured["logger"] is core.logger


def test_update_passes_paths_and_steps_to_manager(monkeypatch):
    called = {}

    class FakeManager:
        def start_process(self, **kwargs):
            called.update(kwargs)

    core = DummyCore()
    component = etl_comp_mod.ETLComponent(core)
    monkeypatch.setattr(component, "_manager", lambda: FakeManager())

    ok = component.update(
        source_system=["NCBI"],
        data_sources=["dbsnp_chr1"],
        run_steps=["extract", "transform"],
        force_steps=["load"],
    )

    assert ok is True
    assert called["source_system"] == ["NCBI"]
    assert called["data_sources"] == ["dbsnp_chr1"]
    assert called["download_path"] == "/tmp/raw"
    assert called["processed_path"] == "/tmp/processed"
    assert called["run_steps"] == ["extract", "transform"]
    assert called["force_steps"] == ["load"]


def test_restart_passes_paths_and_returns_manager_value(monkeypatch):
    called = {}

    class FakeManager:
        def restart_etl_process(self, **kwargs):
            called.update(kwargs)
            return {"ok": True}

    core = DummyCore()
    component = etl_comp_mod.ETLComponent(core)
    monkeypatch.setattr(component, "_manager", lambda: FakeManager())

    out = component.restart(
        data_source=["dbsnp_chr22"],
        source_system=["NCBI"],
        delete_files=True,
    )

    assert out == {"ok": True}
    assert called["data_source"] == ["dbsnp_chr22"]
    assert called["source_system"] == ["NCBI"]
    assert called["download_path"] == "/tmp/raw"
    assert called["processed_path"] == "/tmp/processed"
    assert called["delete_files"] is True


def test_update_all_passes_paths_and_returns_summary(monkeypatch):
    called = {}

    class FakeManager:
        def start_process_all(self, **kwargs):
            called.update(kwargs)
            return {"selected": 2, "processed": 1}

    core = DummyCore()
    component = etl_comp_mod.ETLComponent(core)
    monkeypatch.setattr(component, "_manager", lambda: FakeManager())

    out = component.update_all(
        source_system=["NCBI"],
        data_sources=["hgnc"],
        drop_files_on_success=True,
        only_active=False,
        stop_on_error=True,
    )

    assert out == {"selected": 2, "processed": 1}
    assert called["source_system"] == ["NCBI"]
    assert called["data_sources"] == ["hgnc"]
    assert called["download_path"] == "/tmp/raw"
    assert called["processed_path"] == "/tmp/processed"
    assert called["drop_files_on_success"] is True
    assert called["only_active"] is False
    assert called["stop_on_error"] is True


def test_rollback_passes_paths_and_returns_manager_value(monkeypatch):
    called = {}

    class FakeManager:
        def rollback_etl_process(self, **kwargs):
            called.update(kwargs)
            return {"ok": True}

    core = DummyCore()
    component = etl_comp_mod.ETLComponent(core)
    monkeypatch.setattr(component, "_manager", lambda: FakeManager())

    out = component.rollback(
        package_ids=[10, 11],
        data_source=["dbsnp_chr22"],
        source_system=["NCBI"],
        delete_files=True,
    )

    assert out == {"ok": True}
    assert called["package_ids"] == [10, 11]
    assert called["data_source"] == ["dbsnp_chr22"]
    assert called["source_system"] == ["NCBI"]
    assert called["download_path"] == "/tmp/raw"
    assert called["processed_path"] == "/tmp/processed"
    assert called["delete_files"] is True


def test_index_with_none_groups_calls_manager_with_none(monkeypatch):
    called = {}

    class FakeManager:
        def rebuild_indexes(self, **kwargs):
            called.update(kwargs)
            return True, "done"

    core = DummyCore()
    component = etl_comp_mod.ETLComponent(core)
    monkeypatch.setattr(component, "_manager", lambda: FakeManager())

    ok, msg = component.index(groups=None)

    assert ok is True
    assert msg == "done"
    assert called["index_group"] is None
    assert called["drop_only"] is False
    assert called["drop_first"] is True
    assert called["set_write_mode"] is True
    assert called["set_read_mode"] is True


def test_index_normalizes_string_group_to_list(monkeypatch):
    called = {}

    class FakeManager:
        def rebuild_indexes(self, **kwargs):
            called.update(kwargs)
            return True, "ok"

    core = DummyCore()
    component = etl_comp_mod.ETLComponent(core)
    monkeypatch.setattr(component, "_manager", lambda: FakeManager())

    ok, _ = component.index(groups="genes")

    assert ok is True
    assert called["index_group"] == ["genes"]


def test_index_normalizes_iterable_groups_to_list(monkeypatch):
    called = {}

    class FakeManager:
        def rebuild_indexes(self, **kwargs):
            called.update(kwargs)
            return False, "failed"

    core = DummyCore()
    component = etl_comp_mod.ETLComponent(core)
    monkeypatch.setattr(component, "_manager", lambda: FakeManager())

    ok, msg = component.index(groups=("genes", "variant"), drop_only=True)

    assert ok is False
    assert msg == "failed"
    assert called["index_group"] == ["genes", "variant"]
    assert called["drop_only"] is True

    # last log should be warning when operation failed
    assert core.logger.messages[-1] == ("WARNING", "failed")
