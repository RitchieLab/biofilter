"""annotation_master_gene, against the fixture bundle."""

from __future__ import annotations

import pytest

from biofilter.modules.report import Bundle, ReportManager


@pytest.fixture
def run(fixture_bundle):
    with Bundle.open(fixture_bundle) as bundle:
        manager = ReportManager(bundle=bundle)

        def _run(**params):
            result = manager.run("annotation_master_gene", **params)
            return {row["input_value"]: row for row in result.table.to_pylist()}, result

        yield _run


class TestResolution:
    def test_resolves_symbol_and_reports_canonical_ids(self, run):
        rows, _ = run(input_data=["TP53"])
        row = rows["TP53"]

        assert row["entity_id"] == 1
        assert row["gene_symbol"] == "TP53"
        assert row["hgnc_id"] == "HGNC:11998"
        assert row["ensembl_id"] == "ENSG00000141510"
        assert row["entrez_id"] == "7157"
        assert row["status"] == "ok"
        assert row["note"] is None

    def test_matching_is_case_insensitive(self, run):
        rows, _ = run(input_data=["tp53", "Tp53"])
        assert {r["entity_id"] for r in rows.values()} == {1}

    def test_a_synonym_resolves_to_the_same_gene(self, run):
        rows, _ = run(input_data=["p53"])
        assert rows["p53"]["entity_id"] == 1
        assert rows["p53"]["gene_symbol"] == "TP53"
        assert rows["p53"]["input_matched_alias"] == "p53"

    def test_unresolved_input_is_kept_not_dropped(self, run):
        """
        A report that silently drops what it could not resolve tells the
        user nothing about the difference between "absent from the
        bundle" and "never asked for".
        """
        rows, _ = run(input_data=["TP53", "NOT_A_GENE"])
        assert set(rows) == {"TP53", "NOT_A_GENE"}

        missing = rows["NOT_A_GENE"]
        assert missing["entity_id"] is None
        assert missing["status"] == "not_found"
        assert "not resolved" in missing["note"]

    def test_emit_not_found_rows_false_drops_them(self, run):
        rows, _ = run(input_data=["TP53", "NOT_A_GENE"], emit_not_found_rows=False)
        assert set(rows) == {"TP53"}

    def test_all_returns_every_gene_entity(self, run):
        rows, _ = run(input_data="__ALL__")
        assert {r["gene_symbol"] for r in rows.values()} == {
            "TP53",
            "BRCA1",
            "NOLOC1",
        }


class TestAnnotation:
    def test_location_and_metadata(self, run):
        rows, _ = run(input_data=["BRCA1"])
        row = rows["BRCA1"]

        assert (row["build"], row["chromosome"]) == (38, 17)
        assert (row["start_position"], row["end_position"]) == (5000, 5400)
        assert row["gene_locus_group"] == "protein-coding gene"
        assert row["gene_locus_type"] == "gene with protein product"
        assert row["omic_status"] == "active"
        assert row["gene_groups"] == ["BRCA1 A complex"]

    def test_a_gene_without_a_build38_location_is_partial(self, run):
        rows, _ = run(input_data=["NOLOC1"])
        row = rows["NOLOC1"]

        assert row["entity_id"] == 3
        assert row["status"] == "partial"
        assert row["chromosome"] is None
        assert "No build 38 location" in row["note"]

    def test_other_aliases_exclude_the_canonical_ids(self, run):
        rows, _ = run(input_data=["TP53"])
        assert rows["TP53"]["other_aliases"] == ["p53"]

    def test_relationships_are_counted_in_both_directions(self, run):
        """
        TP53 is entity_1 of two relationships and entity_2 of a third.
        All three count, grouped by what is on the other side.
        """
        rows, _ = run(input_data=["TP53"])
        row = rows["TP53"]

        assert row["total_entity_relationships"] == 3
        assert row["entity_relationships_by_group"] == [
            {"group_name": "Pathways", "count": 2},
            {"group_name": "Proteins", "count": 1},
        ]

    def test_a_gene_with_no_relationships_reports_zero(self, run):
        rows, _ = run(input_data=["BRCA1"])
        assert rows["BRCA1"]["total_entity_relationships"] == 0
        assert rows["BRCA1"]["entity_relationships_by_group"] == []

    def test_include_relationships_false_zeroes_the_section(self, run):
        rows, _ = run(input_data=["TP53"], include_relationships=False)
        assert rows["TP53"]["total_entity_relationships"] == 0
        assert rows["TP53"]["entity_relationships_by_group"] == []


class TestVariantCount:
    def test_counts_variants_inside_the_gene_range(self, run):
        """Three of chr17's four variants fall in TP53's 100-400."""
        rows, _ = run(input_data=["TP53"])
        assert rows["TP53"]["variant_count_in_gene_range"] == 3

    def test_a_range_with_no_variants_is_zero_not_null(self, run):
        rows, _ = run(input_data=["BRCA1"])
        assert rows["BRCA1"]["variant_count_in_gene_range"] == 0

    def test_without_a_location_the_count_is_null_not_zero(self, run):
        """
        No range means the question has no answer, which is not the same
        as the answer being none.
        """
        rows, _ = run(input_data=["NOLOC1"])
        assert rows["NOLOC1"]["variant_count_in_gene_range"] is None

    def test_include_variant_summary_false_nulls_the_column(self, run):
        """
        Off means not counted, which is null — not zero. Zero would
        claim the range was searched and held nothing.
        """
        rows, _ = run(input_data=["TP53"], include_variant_summary=False)
        assert rows["TP53"]["variant_count_in_gene_range"] is None

    def test_the_count_reads_the_partitions_not_the_shadowing_parent(self, run):
        """
        The fixture's undeclared `variant_masters.parquet` is empty and
        carries `position_start`. If the report were reading it, this
        count would be zero and the query would not even bind.
        """
        rows, _ = run(input_data=["TP53"])
        assert rows["TP53"]["variant_count_in_gene_range"] == 3


class TestContract:
    def test_columns_match_what_is_declared(self, run):
        _, result = run(input_data=["TP53"])
        from biofilter.modules.report.reports.report_annotation_master_gene import (
            AnnotationMasterGeneReport,
        )

        assert result.columns == list(AnnotationMasterGeneReport.COLUMNS)

    def test_provenance_names_the_bundle(self, run):
        _, result = run(input_data=["TP53"])
        assert result.provenance["bundle_id"] == "fixturebundle0001"

    def test_csv_export_renders_the_list_columns(self, run, tmp_path):
        _, result = run(input_data=["TP53"])
        written = result.write(tmp_path / "genes.csv")
        text = written[0].read_text()

        assert '"p53"' in text  # other_aliases, as JSON
        assert "Pathways" in text  # relationships, as JSON
