"""
expand_variant_regulatory: which genes a variant regulates, and where.

The report exists because VEP and eQTL disagree: the gene a variant sits
in is usually not the gene it regulates. The fixture is built so that is
true — a variant inside TP53's range regulating BRCA1.
"""

from __future__ import annotations

import pytest

from biofilter.modules.report import Bundle, ReportManager


@pytest.fixture
def run(fixture_bundle):
    with Bundle.open(fixture_bundle) as bundle:
        manager = ReportManager(bundle=bundle)

        def _run(**params):
            result = manager.run("expand_variant_regulatory", **params)
            return result.table.to_pylist(), result

        yield _run


class TestTheQuestionItAnswers:
    def test_the_gene_containing_the_variant_is_not_the_one_it_regulates(self, run):
        """
        The whole reason this report is separate from `annotate_variant`.
        On chr22 of the real bundle these differ for 88.5% of pairs.
        """
        rows, _ = run(input_data=["17:150"])
        named = [r for r in rows if r["regulated_gene_symbol"]]

        assert named
        assert {r["position_gene_symbol"] for r in named} == {"TP53"}
        assert {r["regulated_gene_symbol"] for r in named} == {"BRCA1"}

    def test_one_row_per_variant_tissue_and_regulated_gene(self, run):
        rows, _ = run(input_data=["17:150"])
        assert len(rows) == 3

        keys = {(r["bio_context"], r["regulated_gene_id"]) for r in rows}
        assert len(keys) == 3

    def test_effect_size_and_significance_come_through(self, run):
        rows, _ = run(input_data=["17:150"], tissues=["Brain_Cortex"], qtl_type="eQTL")
        row = next(r for r in rows if r["regulated_gene_symbol"] == "BRCA1")

        assert row["beta"] == pytest.approx(1.2)
        assert row["se"] == pytest.approx(0.1)
        assert row["p_value"] == pytest.approx(1e-20)
        assert row["n"] == 200
        assert row["effect_allele"] == "G"


class TestInputModes:
    def test_a_position(self, run):
        rows, _ = run(input_data=["17:150"])
        assert {r["input_kind"] for r in rows} == {"chr_pos"}

    def test_a_full_variant_id(self, run):
        rows, _ = run(input_data=["17:150:A:G"])
        assert {r["input_kind"] for r in rows} == {"chr_pos_allele"}

    def test_an_rsid(self, run):
        rows, _ = run(input_data=["rs101"])
        assert {r["input_kind"] for r in rows} == {"rsid"}
        assert all(r["status"] == "ok" for r in rows)

    def test_a_gene_symbol_expands_to_variants_in_its_range(self, run):
        """
        Gene mode: the variants inside TP53's locus, and what they
        regulate. Anything that is not an rsID or a position is read as a
        gene name, which is what lets the three share one list.
        """
        rows, _ = run(input_data=["TP53"])

        assert {r["input_kind"] for r in rows} == {"gene"}
        assert {r["position_gene_symbol"] for r in rows} == {"TP53"}
        assert {r["regulated_gene_symbol"] for r in rows if r["regulated_gene_symbol"]} == {"BRCA1"}

    def test_the_three_modes_mix_in_one_call(self, run):
        rows, _ = run(input_data=["TP53", "rs101", "17:200"])
        assert {r["input_kind"] for r in rows} == {"gene", "rsid", "chr_pos"}


class TestFilters:
    def test_by_tissue(self, run):
        rows, _ = run(input_data=["17:150"], tissues=["Liver"])
        assert {r["bio_context"] for r in rows} == {"Liver"}

    def test_tissue_names_are_read_from_the_data_not_hardcoded(self, run):
        """
        13 of GTEx's 50 tissues are loaded today, all brain. Which ones a
        bundle carries is a build flag, so a non-brain tissue has to work
        exactly as well.
        """
        rows, _ = run(input_data=["17:150"], tissues=["liver"])  # case-insensitive
        assert rows
        assert {r["bio_context"] for r in rows} == {"Liver"}

    def test_by_qtl_type(self, run):
        rows, _ = run(input_data=["17:200"], qtl_type="sQTL")
        assert {r["qtl_type"] for r in rows} == {"sQTL"}

    def test_by_significance(self, run):
        strict, _ = run(input_data=["17:150"], p_value_max=1e-10)
        loose, _ = run(input_data=["17:150"], p_value_max=1e-2)

        assert len(strict) < len(loose)
        assert all(r["p_value"] <= 1e-10 for r in strict)

    def test_flanking_widens_the_gene_range(self, run):
        """
        17:200 is inside TP53 (100-400); 17:150 too. Flanking matters for
        variants just outside, and the parameter has to reach the join.
        """
        rows, _ = run(input_data=["TP53"], flanking_bp=0)
        widened, _ = run(input_data=["TP53"], flanking_bp=10_000)

        assert len(widened) >= len(rows)


class TestWhatCameBackEmpty:
    def test_a_variant_with_no_evidence_is_kept(self, run):
        rows, _ = run(input_data=["17:300"])

        assert [r["status"] for r in rows] == ["not_found"]
        assert "No regulatory evidence for this variant" in rows[0]["note"]

    def test_a_gene_with_no_evidence_says_so_differently(self, run):
        rows, _ = run(input_data=["NOLOC1"])
        assert "variants in this gene" in rows[0]["note"]

    def test_emit_not_found_rows_false_drops_them(self, run):
        rows, _ = run(input_data=["17:150", "17:300"], emit_not_found_rows=False)
        assert {r["status"] for r in rows} == {"ok"}


class TestUnresolvedTargets:
    def test_a_regulated_gene_the_bundle_does_not_know_keeps_its_id(self, run):
        """
        17% of GTEx's targets on chr22 have no BF4 entity — lncRNAs and
        pseudogenes without HGNC symbols. The evidence is still real, so
        the row stays with `regulated_gene_id` and a null symbol.
        """
        rows, _ = run(input_data=["17:150"])
        unnamed = [r for r in rows if r["regulated_gene_symbol"] is None]

        assert len(unnamed) == 1
        assert unnamed[0]["regulated_gene_id"] == "ENSG09999999999"
        assert unnamed[0]["p_value"] is not None


class TestContract:
    def test_columns_match_what_is_declared(self, run):
        _, result = run(input_data=["17:150"])
        from biofilter.modules.report.reports.report_expand_variant_regulatory import (
            ExpandVariantRegulatoryReport,
        )

        assert result.columns == list(ExpandVariantRegulatoryReport.COLUMNS)

    def test_coverage_records_the_chromosomes(self, run):
        _, result = run(input_data=["17:150"])
        assert result.provenance["coverage"]["chromosomes"] == [17, 22]

    def test_provenance_names_the_bundle(self, run):
        _, result = run(input_data=["17:150"])
        assert result.provenance["bundle_id"] == "fixturebundle0001"
