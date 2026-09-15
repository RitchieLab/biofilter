"""
ReportComponent delegation.

`test_routing.py` covers the routing decision against a real bundle.
This covers the mechanical part: every public method reaches the manager
that owns the report, and each manager is built once. A method added to
the facade and not wired through fails here.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import biofilter.core.components.report_component as rcmod


class DummyLogger:
    def __init__(self):
        self.messages = []

    def log(self, message, level="INFO"):
        self.messages.append((level, message))


class DummyCore:
    def __init__(self, db_uri=None):
        self.logger = DummyLogger()
        self.db_uri = db_uri
        self.db = None
        self._db = type("DB", (), {"get_session": lambda self: "SESSION"})()

    def require_db(self):
        return self._db


class FakeManager:
    """Answers for whichever report names it was told it owns."""

    def __init__(self, owns=(), tag="fake"):
        self.owns = set(owns)
        self.tag = tag
        self.bundle = None

    def resolve(self, identifier):
        if identifier in self.owns:
            return f"report_{identifier}"
        raise ValueError(f"Report not found: '{identifier}'")

    def list_reports(self):
        return [{"name": n, "module": f"report_{n}"} for n in sorted(self.owns)]

    def explain(self, identifier):
        return f"{self.tag}:explain:{identifier}"

    def example_input(self, identifier):
        return {"tag": self.tag, "id": identifier}

    def available_columns(self, identifier):
        return [self.tag, identifier]

    def get_class(self, identifier):
        return f"{self.tag}:class:{identifier}"

    def run(self, identifier, **kwargs):
        return (self.tag, "run", identifier, kwargs)

    def run_example(self, identifier, **kwargs):
        return (self.tag, "run_example", identifier, kwargs)

    def refresh(self):
        return None


@pytest.fixture
def component(monkeypatch):
    native = FakeManager(owns={"template"}, tag="native")
    legacy = FakeManager(owns={"etl_status"}, tag="legacy")

    comp = rcmod.ReportComponent(DummyCore())
    monkeypatch.setattr(comp, "_native_manager", lambda: native)
    monkeypatch.setattr(comp, "_legacy_manager", lambda: legacy)
    return comp


class TestDelegation:
    @pytest.mark.parametrize(
        "identifier,tag", [("template", "native"), ("etl_status", "legacy")]
    )
    def test_every_public_method_reaches_the_owning_manager(
        self, component, identifier, tag
    ):
        assert component.explain(identifier) == f"{tag}:explain:{identifier}"
        assert component.example_input(identifier) == {"tag": tag, "id": identifier}
        assert component.available_columns(identifier) == [tag, identifier]
        assert component.get_report_class(identifier) == f"{tag}:class:{identifier}"
        assert component.run(identifier, p=1) == (tag, "run", identifier, {"p": 1})
        assert component.run_example(identifier, p=2) == (
            tag,
            "run_example",
            identifier,
            {"p": 2},
        )

    def test_list_merges_both_and_labels_the_engine(self, component):
        rows = component.list()
        assert [(r["name"], r["engine"]) for r in rows] == [
            ("etl_status", "legacy"),
            ("template", "native"),
        ]

    def test_a_name_in_both_modules_is_served_by_the_native_one(self, monkeypatch):
        """
        The collision that happens mid-migration: a report is rewritten
        natively while the legacy one is still present. The new one wins,
        and the name appears once.
        """
        native = FakeManager(owns={"etl_status"}, tag="native")
        legacy = FakeManager(owns={"etl_status"}, tag="legacy")
        comp = rcmod.ReportComponent(DummyCore())
        monkeypatch.setattr(comp, "_native_manager", lambda: native)
        monkeypatch.setattr(comp, "_legacy_manager", lambda: legacy)

        assert comp.engine_for("etl_status") == rcmod.NATIVE
        assert comp.run("etl_status") == ("native", "run", "etl_status", {})
        assert [r["name"] for r in comp.list()] == ["etl_status"]


class TestManagerLifecycle:
    def test_each_manager_is_built_once(self, monkeypatch):
        built = {"native": 0, "legacy": 0}

        class CountingNative(FakeManager):
            def __init__(self, logger=None):
                super().__init__(owns={"template"}, tag="native")
                built["native"] += 1

        class CountingLegacy(FakeManager):
            def __init__(self, session_factory, db, logger):
                super().__init__(owns={"etl_status"}, tag="legacy")
                built["legacy"] += 1

        monkeypatch.setattr(rcmod, "NativeManager", CountingNative)
        monkeypatch.setattr(rcmod, "LegacyManager", CountingLegacy)

        comp = rcmod.ReportComponent(DummyCore())
        for _ in range(3):
            comp.explain("template")
            comp.explain("etl_status")

        assert built == {"native": 1, "legacy": 1}
