"""ReportComponent: which module serves which report, during the migration."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from biofilter.core.components.report_component import LEGACY, NATIVE, ReportComponent
from biofilter.modules.report.result import ReportResult


class _NoDatabase(SimpleNamespace):
    """
    A session pointed at a bundle and nothing else.

    `require_db` raising is the realistic case for the native module:
    there is no relational database, which is the whole point of the
    migration.
    """

    def require_db(self):
        raise RuntimeError("No database is connected.")


@pytest.fixture
def component(fixture_bundle):
    core = _NoDatabase(
        logger=SimpleNamespace(log=lambda *a, **k: None),
        db_uri=f"parquet://{fixture_bundle}",
        db=None,
    )
    comp = ReportComponent(core=core)
    yield comp
    comp.close()


class TestRouting:
    def test_native_report_routes_native(self, component):
        assert component.engine_for("template") == NATIVE

    def test_unknown_name_falls_through_to_legacy(self, component):
        """
        Anything the native module does not have is the legacy module's,
        and the error the user sees comes from there.
        """
        assert component.engine_for("snp_snp_model") == LEGACY

    def test_listing_survives_having_no_database(self, component):
        """
        `report list` is a question about the installed package. A
        session with no relational database still answers it — for the
        native half, which is all that exists there.
        """
        names = [r["name"] for r in component.list()]
        assert names == sorted(names)
        assert "template" in names
        assert "annotation_master_gene" in names
        assert {r["engine"] for r in component.list()} == {NATIVE}

    def test_explain_routes_to_the_owning_module(self, component):
        assert "example report" in component.explain("template").lower()


class TestExecution:
    def test_native_run_returns_a_result_with_provenance(self, component):
        result = component.run("template", input_data=["TP53", "NOPE"])

        assert isinstance(result, ReportResult)
        assert result.num_rows == 2
        assert result.provenance["bundle_id"] == "fixturebundle0001"

    def test_bundle_comes_from_the_parquet_uri(self, component, fixture_bundle):
        assert component._bundle_root() == fixture_bundle

    def test_running_a_legacy_report_without_a_database_says_so(self, component):
        with pytest.raises(RuntimeError, match="No database"):
            component.run("snp_snp_model", input_data=["rs1"])
