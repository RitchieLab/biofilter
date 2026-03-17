from __future__ import annotations

import biofilter.core.components.report_component as rcmod


class DummyLogger:
    def __init__(self):
        self.messages = []

    def log(self, message, level="INFO"):
        self.messages.append((level, message))


class DummyCore:
    def __init__(self):
        self.logger = DummyLogger()
        self._db = object()

    def require_db(self):
        return self._db


def test_get_manager_is_cached(monkeypatch):
    created = {"count": 0}

    class FakeManager:
        def __init__(self, session_factory, db, logger):
            created["count"] += 1
            self.session_factory = session_factory
            self.db = db
            self.logger = logger

    monkeypatch.setattr(rcmod, "ReportManager", FakeManager)

    core = DummyCore()
    core._db = type("DB", (), {"get_session": lambda self: "SESSION"})()

    comp = rcmod.ReportComponent(core)
    m1 = comp._get_manager()
    m2 = comp._get_manager()

    assert created["count"] == 1
    assert m1 is m2


def test_component_methods_delegate_to_manager(monkeypatch):
    class FakeManager:
        def explain(self, identifier):
            return f"explain:{identifier}"

        def list_reports(self):
            return [{"name": "x"}]

        def refresh(self):
            return None

        def example_input(self, identifier):
            return {"id": identifier}

        def available_columns(self, identifier):
            return ["a", identifier]

        def run(self, identifier, **kwargs):
            return ("run", identifier, kwargs)

        def run_example(self, identifier, **kwargs):
            return ("run_example", identifier, kwargs)

        def get_class(self, identifier):
            return f"class:{identifier}"

    core = DummyCore()
    comp = rcmod.ReportComponent(core)
    monkeypatch.setattr(comp, "_get_manager", lambda: FakeManager())

    assert comp.explain("etl_status") == "explain:etl_status"
    assert comp.list() == [{"name": "x"}]
    assert comp.example_input("etl_status") == {"id": "etl_status"}
    assert comp.available_columns("etl_status") == ["a", "etl_status"]
    assert comp.run("etl_status", p=1) == ("run", "etl_status", {"p": 1})
    assert comp.run_example("etl_status", p=2) == (
        "run_example",
        "etl_status",
        {"p": 2},
    )
    assert comp.get_report_class("etl_status") == "class:etl_status"
