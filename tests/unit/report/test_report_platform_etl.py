"""
platform_etl_status and platform_etl_packages: what the build did.

Most of what is tested here is `pipeline_state`, because the word "ok"
is what the rewrite changed. The relational version returned False for
51 of a real bundle's 68 sources and not one of them was broken.
"""

from __future__ import annotations

import pytest

from biofilter.modules.report import Bundle, ReportManager


@pytest.fixture
def run(fixture_bundle):
    with Bundle.open(fixture_bundle) as bundle:
        manager = ReportManager(bundle=bundle)

        def _run(report, **params):
            result = manager.run(report, **params)
            return {r["data_source"]: r for r in result.table.to_pylist()}, result

        yield _run


class TestPipelineState:
    REPORT = "platform_etl_status"

    def test_a_complete_core_pipeline_is_ok(self, run):
        rows, _ = run(self.REPORT)
        row = rows["hgnc"]

        assert row["pipeline_state"] == "ok"
        assert row["pipeline_ok"] is True
        assert row["transform_aligned"] is True
        assert row["load_aligned"] is True

    def test_a_variant_source_needs_no_load(self, run):
        """
        The finding that drove the rewrite. The variant branch writes
        parquet straight from transform, so there is no load stage to
        miss — and the relational version called that a failed pipeline,
        for 51 of 68 sources.
        """
        rows, _ = run(self.REPORT)
        row = rows["gnomad_test"]

        assert row["branch"] == "variant"
        assert row["load_package_id"] is None
        assert row["pipeline_state"] == "ok"
        assert row["pipeline_ok"] is True

    def test_a_core_source_without_a_load_is_incomplete(self, run):
        """The same missing stage, and here it does mean something."""
        rows, _ = run(self.REPORT)
        row = rows["reactome"]

        assert row["branch"] == "core"
        assert row["pipeline_state"] == "incomplete"
        assert row["pipeline_ok"] is False

    def test_stages_without_hashes_are_unverifiable_not_broken(self, run):
        """
        Some DTPs read database state rather than a file and produce no
        hash, so alignment cannot be shown either way. Null, not false —
        false reads as "this is wrong" rather than "this is unproven".
        """
        rows, _ = run(self.REPORT)
        row = rows["uniprot"]

        assert row["transform_aligned"] is None
        assert row["pipeline_state"] == "unverifiable"
        assert row["pipeline_ok"] is True

    def test_mismatched_hashes_are_misaligned(self, run):
        rows, _ = run(self.REPORT)
        row = rows["gene_ontology"]

        assert row["transform_aligned"] is False
        assert row["pipeline_state"] == "misaligned"
        assert row["pipeline_ok"] is False

    def test_a_source_that_never_ran_says_so(self, run):
        rows, _ = run(self.REPORT)
        row = rows["mondo"]

        assert row["pipeline_state"] == "never_run"
        assert row["pipeline_ok"] is False
        assert row["extract_package_id"] is None


class TestFailureHistory:
    REPORT = "platform_etl_status"

    def test_a_past_failure_is_reported_even_when_the_source_is_ok(self, run):
        """
        "It succeeded, but not on the first try" is worth knowing, and it
        is not the same as a broken pipeline.
        """
        rows, _ = run(self.REPORT)
        row = rows["clingen"]

        assert row["latest_error"] is not None
        assert "failed" in row["latest_error"]

    def test_the_message_survives_a_null_note(self, run):
        """
        Every failed package in the real bundle has a null note, so
        reading `note` straight through reported nothing at all.
        """
        rows, _ = run(self.REPORT)
        assert "package" in rows["clingen"]["latest_error"]

    def test_a_clean_source_has_no_error(self, run):
        rows, _ = run(self.REPORT)
        assert rows["hgnc"]["latest_error"] is None


class TestFilters:
    REPORT = "platform_etl_status"

    def test_by_source_system(self, run):
        rows, _ = run(self.REPORT, source_system="HGNC")
        assert set(rows) == {"hgnc"}

    def test_by_data_source(self, run):
        rows, _ = run(self.REPORT, data_sources=["hgnc", "mondo"])
        assert set(rows) == {"hgnc", "mondo"}

    def test_every_source_is_listed_by_default(self, run):
        rows, _ = run(self.REPORT)
        assert len(rows) == 7


class TestPackages:
    REPORT = "platform_etl_packages"

    def test_one_row_per_package(self, run):
        _, result = run(self.REPORT)
        assert result.num_rows == 14

    def test_a_package_carries_its_stage_and_source(self, run):
        _, result = run(self.REPORT, data_sources=["hgnc"])
        rows = result.table.to_pylist()

        assert [r["operation_type"] for r in rows] == ["extract", "transform", "load"]
        assert {r["source_system"] for r in rows} == {"HGNC"}

    def test_the_hash_is_carried_from_stage_to_stage(self, run):
        """
        What "aligned" means in the status report: the extract's digest
        reappears as the transform's, and then as the load's.
        """
        _, result = run(self.REPORT, data_sources=["hgnc"])
        rows = result.table.to_pylist()

        assert rows[0]["extract_hash"] == "aaa"
        assert rows[1]["transform_hash"] == "aaa"
        assert rows[2]["load_hash"] == "aaa"

    def test_filtering_by_operation_type(self, run):
        _, result = run(self.REPORT, operation_type="load")
        assert {r["operation_type"] for r in result.table.to_pylist()} == {"load"}

    def test_failed_packages_are_not_hidden(self, run):
        _, result = run(self.REPORT, data_sources=["clingen"])
        statuses = [r["status"] for r in result.table.to_pylist()]

        assert "failed" in statuses


class TestContract:
    @pytest.mark.parametrize(
        "report", ["platform_etl_status", "platform_etl_packages"]
    )
    def test_columns_match_what_is_declared(self, run, report):
        _, result = run(report)
        declared = ReportManager().get_class(report).COLUMNS

        assert result.columns == list(declared)

    @pytest.mark.parametrize(
        "report", ["platform_etl_status", "platform_etl_packages"]
    )
    def test_provenance_names_the_bundle(self, run, report):
        _, result = run(report)
        assert result.provenance["bundle_id"] == "fixturebundle0001"

    @pytest.mark.parametrize(
        "report", ["platform_etl_status", "platform_etl_packages"]
    )
    def test_they_need_no_input(self, run, report):
        """A platform report describes the bundle; there is nothing to ask about."""
        _, result = run(report)
        assert result.num_rows > 0
