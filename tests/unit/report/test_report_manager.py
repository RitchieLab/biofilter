from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import biofilter.modules.report.report_manager as rmod
from biofilter.modules.report.reports.base_report import ReportBase


class DummyLogger:
    def __init__(self):
        self.messages = []

    def log(self, message, level="INFO"):
        self.messages.append((level, message))


class DummySession:
    def __init__(self):
        self.rollback_calls = 0

    def rollback(self):
        self.rollback_calls += 1


@contextmanager
def _session_ctx(session):
    yield session


def _manager_with(session: DummySession):
    return rmod.ReportManager(
        session_factory=lambda: _session_ctx(session),
        db=object(),
        logger=DummyLogger(),
    )


def test_index_builds_sorted_cache_and_list_reports(monkeypatch):
    session = DummySession()
    manager = _manager_with(session)

    class BReport(ReportBase):
        name = "B Report"
        description = "bbb"

    class AReport(ReportBase):
        name = "A Report"
        description = "aaa"

    monkeypatch.setattr(manager, "iter_modules", lambda: iter(["report_b", "report_a"]))

    calls = {"n": 0}

    def fake_load_class(module_name):
        calls["n"] += 1
        return {"report_a": AReport, "report_b": BReport}[module_name]

    monkeypatch.setattr(manager, "_load_class", fake_load_class)

    first = manager.index()
    second = manager.index()

    assert [x.module for x in first] == ["report_a", "report_b"]
    assert [x.name for x in first] == ["A Report", "B Report"]
    assert first == second
    assert calls["n"] == 2  # cached in second call

    listed = manager.list_reports()
    assert listed[0]["name"] == "A Report"
    assert listed[1]["module"] == "report_b"


def test_refresh_clears_caches():
    session = DummySession()
    manager = _manager_with(session)
    manager._class_cache = {"x": object()}
    manager._index_cache = [rmod.ReportInfo(module="m", name="n", description="d")]

    manager.refresh()

    assert manager._class_cache == {}
    assert manager._index_cache is None


def test_resolve_supports_module_friendly_and_class(monkeypatch):
    session = DummySession()
    manager = _manager_with(session)

    idx = [
        rmod.ReportInfo(module="report_alpha", name="Alpha", description="A"),
        rmod.ReportInfo(module="report_beta", name="Beta", description="B"),
    ]
    monkeypatch.setattr(manager, "index", lambda: idx)

    class BetaClass(ReportBase):
        name = "Beta"

    monkeypatch.setattr(
        manager,
        "_load_class",
        lambda module_name: BetaClass if module_name == "report_beta" else ReportBase,
    )

    assert manager.resolve("report_alpha") == "report_alpha"
    assert manager.resolve("alpha") == "report_alpha"
    assert manager.resolve("BetaClass") == "report_beta"


def test_resolve_raises_for_empty_and_unknown(monkeypatch):
    session = DummySession()
    manager = _manager_with(session)
    monkeypatch.setattr(
        manager,
        "index",
        lambda: [rmod.ReportInfo(module="report_x", name="X", description="")],
    )
    monkeypatch.setattr(manager, "_load_class", lambda module_name: ReportBase)

    with pytest.raises(ValueError, match="cannot be empty"):
        manager.resolve(" ")

    with pytest.raises(ValueError, match="Report not found"):
        manager.resolve("missing")


def test_get_falls_back_when_report_does_not_accept_db_kw(monkeypatch):
    session = DummySession()
    manager = _manager_with(session)

    class LegacyReport(ReportBase):
        def __init__(self, session=None, logger=None, x=None):
            self.session = session
            self.logger = logger
            self.kwargs = {"x": x}

        def run(self):
            return "legacy-ok"

    monkeypatch.setattr(manager, "get_class", lambda identifier: LegacyReport)

    report = manager.get("legacy", session=session, x=1)

    assert isinstance(report, LegacyReport)
    assert report.session is session
    assert report.kwargs["x"] == 1
    assert any(
        level == "WARNING" and "Falling back to session-only" in msg
        for level, msg in manager.logger.messages
    )


def test_run_returns_result_and_rolls_back_once(monkeypatch):
    session = DummySession()
    manager = _manager_with(session)

    report_obj = SimpleNamespace(run=lambda: "ok")
    monkeypatch.setattr(manager, "get", lambda *a, **k: report_obj)

    out = manager.run("alpha")

    assert out == "ok"
    assert session.rollback_calls == 1  # finalizer


def test_run_logs_and_reraises_and_rolls_back_twice(monkeypatch):
    session = DummySession()
    manager = _manager_with(session)

    class FailingReport:
        def run(self):
            raise RuntimeError("boom")

    monkeypatch.setattr(manager, "get", lambda *a, **k: FailingReport())

    with pytest.raises(RuntimeError, match="boom"):
        manager.run("alpha")

    # once in except + once in finally
    assert session.rollback_calls == 2
    assert any(
        level == "ERROR" and "failed: boom" in msg
        for level, msg in manager.logger.messages
    )


def test_run_example_uses_default_input_data_and_preserves_explicit(monkeypatch):
    session = DummySession()
    manager = _manager_with(session)

    class ExampleReport(ReportBase):
        @classmethod
        def example_input(cls):
            return {"seed": "value"}

    monkeypatch.setattr(manager, "get_class", lambda identifier: ExampleReport)

    calls = []

    def fake_run(identifier, **kwargs):
        calls.append((identifier, kwargs))
        return "ok"

    monkeypatch.setattr(manager, "run", fake_run)

    manager.run_example("alpha")
    manager.run_example("alpha", input_data={"custom": 1})

    assert calls[0] == ("alpha", {"input_data": {"seed": "value"}})
    assert calls[1] == ("alpha", {"input_data": {"custom": 1}})
