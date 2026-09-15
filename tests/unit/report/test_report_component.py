"""
ReportComponent: the facade over the report module.

Reports read a bundle and nothing else. What the component still has to
get right is when a bundle is needed (running) and when it is not
(listing, explaining), plus reporting honestly how much of the old layer
is still waiting to be rewritten.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import biofilter.core.components.report_component as rcmod
from biofilter.modules.report.result import ReportResult


def _core(db_uri=None):
    return SimpleNamespace(
        logger=SimpleNamespace(log=lambda *a, **k: None),
        db_uri=db_uri,
        db=None,
    )


@pytest.fixture
def component(fixture_bundle):
    comp = rcmod.ReportComponent(_core(f"parquet://{fixture_bundle}"))
    yield comp
    comp.close()


@pytest.fixture
def bundleless():
    comp = rcmod.ReportComponent(_core())
    yield comp
    comp.close()


class TestWithoutABundle:
    def test_listing_needs_no_bundle(self, bundleless):
        """
        Which reports exist is a question about the installed package.
        Answering it should not require opening 21 GB of parquet, or any
        database — the old component built its manager through
        `require_db()` and failed wherever none was reachable.
        """
        names = [r["name"] for r in bundleless.list()]
        assert "annotate_gene" in names

    def test_explain_needs_no_bundle(self, bundleless):
        assert "gene" in bundleless.explain("annotate_gene").lower()

    def test_available_columns_needs_no_bundle(self, bundleless):
        assert "entity_id" in bundleless.available_columns("annotate_gene")

    def test_running_says_how_to_supply_one(self, bundleless):
        with pytest.raises(ValueError, match="--bundle"):
            bundleless.run("annotate_gene", input_data=["TP53"])


class TestWithABundle:
    def test_the_bundle_comes_from_the_parquet_uri(self, component, fixture_bundle):
        assert component._bundle_root() == fixture_bundle

    def test_running_returns_a_result_with_provenance(self, component):
        result = component.run("annotate_gene", input_data=["TP53", "NOPE"])

        assert isinstance(result, ReportResult)
        assert result.num_rows == 2
        assert result.provenance["bundle_id"] == "fixturebundle0001"

    def test_the_manager_is_built_once(self, component, monkeypatch):
        built = {"count": 0}
        real = rcmod.ReportManager

        class Counting(real):
            def __init__(self, *args, **kwargs):
                built["count"] += 1
                super().__init__(*args, **kwargs)

        monkeypatch.setattr(rcmod, "ReportManager", Counting)
        component._manager = None
        for _ in range(3):
            component.list()

        assert built["count"] == 1

    def test_an_unknown_report_lists_what_exists(self, component):
        with pytest.raises(ValueError, match="Report not found"):
            component.run("no_such_report")


class TestPendingMigration:
    def test_counts_reports_awaiting_rewrite_without_importing_them(self):
        """
        Several of them no longer import — they select columns the
        bundles stopped carrying — so the count comes from the directory
        listing. It is the migration's progress bar, and it reaches zero
        when the directory empties.
        """
        pending = rcmod.ReportComponent.pending_migration()

        # Deliberately not naming one: every name here is a moving
        # target, and a test that has to be edited each time a report is
        # migrated tests the migration schedule rather than the code.
        assert all(isinstance(name, str) and name for name in pending)
        assert not any(name.startswith("report_") for name in pending)
        assert pending == sorted(pending)

    def test_a_migrated_report_is_no_longer_pending(self):
        assert "annotate_gene" not in (
            rcmod.ReportComponent.pending_migration()
        )

    def test_the_scaffold_is_not_counted(self):
        assert "template" not in rcmod.ReportComponent.pending_migration()

    def test_an_empty_directory_means_nothing_pending(self, monkeypatch, tmp_path):
        monkeypatch.setattr(rcmod, "_PENDING_DIR", tmp_path / "gone")
        assert rcmod.ReportComponent.pending_migration() == []
