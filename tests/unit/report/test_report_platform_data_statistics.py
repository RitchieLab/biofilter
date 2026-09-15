"""
platform_data_statistics: what is in this bundle, how much, and how big.

A long-format report — one row per measurement — so the tests mostly ask
whether each section measured the right thing, and whether the cheap
sections are actually cheap.
"""

from __future__ import annotations

import pytest

from biofilter.modules.report import Bundle, ReportManager


@pytest.fixture
def run(fixture_bundle):
    with Bundle.open(fixture_bundle) as bundle:
        manager = ReportManager(bundle=bundle)

        def _run(**params):
            result = manager.run("platform_data_statistics", **params)
            return result.table.to_pylist(), result

        yield _run


def _by_metric(rows, section, metric=None):
    return [
        r
        for r in rows
        if r["section"] == section and (metric is None or r["metric"] == metric)
    ]


class TestBundleSection:
    def test_identity_comes_from_the_manifest(self, run):
        rows, _ = run(sections=["bundle"])
        values = {r["metric"]: r for r in rows}

        assert values["bundle_id"]["value_text"] == "fixturebundle0001"
        assert values["biofilter_version"]["value_text"] == "4.3.0"

    def test_totals_are_counted(self, run):
        rows, _ = run(sections=["bundle"])
        values = {r["metric"]: r["value_number"] for r in rows}

        assert values["tables"] > 0
        assert values["files"] >= values["tables"]  # partitioned tables span several
        assert values["rows"] > 0
        assert values["bytes"] > 0

    def test_empty_tables_are_counted_as_their_own_measure(self, run):
        """
        A declared table with no rows is a source that was planned and
        did not land. Worth a number of its own rather than being buried
        in the per-table list.
        """
        rows, _ = run(sections=["bundle"])
        values = {r["metric"]: r for r in rows}

        assert "tables_without_rows" in values
        assert values["tables_without_rows"]["value_number"] is not None


class TestStorageSection:
    def test_one_row_per_logical_table(self, run):
        rows, _ = run(sections=["storage"])
        names = [r["dimension_1"] for r in rows]

        assert "entity_relationships" in names
        assert "variant_masters" in names
        assert len(names) == len(set(names)), "a table should be measured once"

    def test_a_partitioned_table_is_summed_across_its_files(self, run):
        rows, _ = run(sections=["storage"])
        variants = next(r for r in rows if r["dimension_1"] == "variant_masters")

        assert variants["dimension_2"] == "variant"
        assert "2 file(s)" in variants["note"]
        assert variants["value_number"] == 5  # 4 on chr17, 1 on chr22

    def test_size_is_reported_in_bytes_and_readably(self, run):
        rows, _ = run(sections=["storage"])
        row = next(r for r in rows if r["dimension_1"] == "entity_relationships")

        assert row["value_text"].endswith("MB")
        assert "bytes" in row["note"]


class TestDataSections:
    def test_entities_are_counted_by_group(self, run):
        rows, _ = run(sections=["entities"])
        counts = {r["dimension_1"]: r["value_number"] for r in rows}

        assert counts["Genes"] == 4
        assert counts["Diseases"] == 2

    def test_variants_are_counted_per_chromosome(self, run):
        """
        Grouped from the data, not parsed out of a filename — the
        manifest counts rows per file, and a file happening to be a
        chromosome is a convention, not a guarantee.
        """
        rows, _ = run(sections=["variants"])
        by_chrom = {
            r["dimension_2"]: r["value_number"]
            for r in rows
            if r["dimension_1"] == "variant_masters"
        }

        assert by_chrom == {"17": 4, "22": 1}

    def test_relationships_are_counted_by_group_pair(self, run):
        rows = _by_metric(run(sections=["relationships"])[0],
                          "relationships", "relationships_by_group_pair")
        pairs = {(r["dimension_1"], r["dimension_2"]): r["value_number"] for r in rows}

        assert pairs[("Genes", "Proteins")] == 1
        assert pairs[("Diseases", "Genes")] == 2

    def test_relationships_are_also_counted_by_type(self, run):
        rows = _by_metric(run(sections=["relationships"])[0],
                          "relationships", "relationships_by_type")
        by_type = {r["dimension_1"]: r["value_number"] for r in rows}

        assert by_type == {"interacts_with": 4, "in_pathway": 2}

    def test_every_source_is_listed_even_one_that_never_ran(self, run):
        rows, _ = run(sections=["sources"])
        sources = {r["dimension_1"]: r for r in rows}

        assert "hgnc" in sources
        assert "mondo" in sources  # no packages at all
        assert sources["mondo"]["value_text"] is None


class TestSectionSelection:
    def test_all_sections_by_default(self, run):
        rows, _ = run()
        assert {r["section"] for r in rows} == {
            "bundle", "storage", "entities", "variants", "relationships", "sources"
        }

    def test_a_subset_can_be_asked_for(self, run):
        rows, _ = run(sections=["bundle", "storage"])
        assert {r["section"] for r in rows} == {"bundle", "storage"}

    def test_an_unknown_section_is_rejected(self, run):
        with pytest.raises(ValueError, match="Unknown section"):
            run(sections=["bundle", "wishful_thinking"])

    def test_the_manifest_sections_read_no_data(self, run, fixture_bundle):
        """
        `bundle` and `storage` answer from manifest.json alone. On a real
        21 GB bundle that is the difference between instant and a scan;
        here it is asserted structurally, by deleting the parquet first.
        """
        import shutil

        with Bundle.open(fixture_bundle) as bundle:
            manager = ReportManager(bundle=bundle)
            shutil.rmtree(fixture_bundle / "tables" / "variant_masters")

            result = manager.run("platform_data_statistics", sections=["bundle", "storage"])

        assert result.num_rows > 0
        names = {r["dimension_1"] for r in result.table.to_pylist()}
        assert "variant_masters" in names


class TestContract:
    def test_columns_match_what_is_declared(self, run):
        _, result = run()
        from biofilter.modules.report.reports.report_platform_data_statistics import (
            PlatformDataStatisticsReport,
        )

        assert result.columns == list(PlatformDataStatisticsReport.COLUMNS)

    def test_provenance_names_the_bundle(self, run):
        _, result = run()
        assert result.provenance["bundle_id"] == "fixturebundle0001"

    def test_it_needs_no_input(self, run):
        _, result = run()
        assert result.num_rows > 0
