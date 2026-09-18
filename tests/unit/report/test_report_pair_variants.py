"""
pair_variants: candidate pairs whose genes share biology.

Runs against `pairing_bundle` — the shared fixture plus a connector that
reaches two different genes, since without one there is no pair to make.

The shape to keep in mind: PATHWAY reaches TP53 and BRCA1 (two genes).
DISEASE reaches TP53, BRCA1 and DGENE (three). TP53 holds variants at
17:150, 200 and 300; BRCA1 holds 17:5100 and 17:5200.
"""

from __future__ import annotations

import pytest

from biofilter.modules.report import Bundle, ReportManager


@pytest.fixture
def run(pairing_bundle):
    with Bundle.open(pairing_bundle) as bundle:
        manager = ReportManager(bundle=bundle)

        def _run(**params):
            params.setdefault("group_types", ["Pathways"])
            result = manager.run("pair_variants", **params)
            return result.table.to_pylist(), result

        yield _run


def _pairs(rows):
    return {
        tuple(sorted((r["variant_1_key"], r["variant_2_key"]))) for r in rows
    }


class TestTheThreeStages:
    def test_a_gene_input_reaches_the_variants_of_the_gene_it_pairs_with(self, run):
        rows, _ = run(input_data=["TP53"], membership="either")

        assert rows
        assert {r["gene_1_symbol"] for r in rows} == {"TP53"}
        assert {r["gene_2_symbol"] for r in rows} == {"BRCA1"}

    def test_both_genes_named_pairs_their_variants_across(self, run):
        """3 variants in TP53 x 2 in BRCA1 = 6 pairs, one gene pair."""
        rows, _ = run(input_data=["TP53", "BRCA1"])

        assert len(rows) == 6
        assert _pairs(rows) == {
            (a, b)
            for a in ("17:150:A:G", "17:200:A:G", "17:300:A:G")
            for b in ("17:5100:A:G", "17:5200:A:G")
        }

    def test_a_variant_input_is_placed_on_its_gene(self, run):
        """17:150 sits in TP53, so it pairs with BRCA1's variants."""
        rows, _ = run(input_data=["17:150"], membership="either")

        assert {r["variant_1_key"] for r in rows} == {"17:150:A:G"}
        assert {r["gene_1_symbol"] for r in rows} == {"TP53"}

    def test_an_input_that_resolves_to_nothing_makes_no_pair(self, run):
        rows, _ = run(input_data=["NOT_A_GENE"], membership="either")

        assert rows == []

    def test_an_empty_input_is_refused(self, run):
        with pytest.raises(ValueError, match="at least one gene or variant"):
            run(input_data=[])


class TestNamingAGeneAndNamingAVariantAreDifferentRequests:
    """
    `from_input` cannot mean the same thing for both. Naming a gene asks
    for its variants; naming a variant asks for that variant, not for
    every other one in the gene that happens to contain it.
    """

    def test_naming_a_gene_makes_all_its_variants_count_as_input(self, run):
        rows, _ = run(input_data=["TP53", "BRCA1"])

        assert all(r["variant_1_from_input"] for r in rows)
        assert all(r["variant_2_from_input"] for r in rows)

    def test_naming_one_variant_does_not_enrol_its_neighbours(self, run):
        """
        17:150 and 17:300 are both in TP53. Asking for the first must not
        silently pair the second.
        """
        rows, _ = run(input_data=["17:150", "17:5100"])

        assert _pairs(rows) == {("17:150:A:G", "17:5100:A:G")}

    def test_the_named_variant_carries_the_text_that_named_it(self, run):
        rows, _ = run(input_data=["17:150", "17:5100"])

        assert rows[0]["input_1"] == "17:150"
        assert rows[0]["input_2"] == "17:5100"


class TestMembership:
    def test_both_keeps_only_pairs_the_input_covers_on_each_side(self, run):
        rows, _ = run(input_data=["TP53"], membership="both")

        # BRCA1 was never asked for, so its variants are not input.
        assert rows == []

    def test_either_admits_a_partner_the_input_never_named(self, run):
        rows, _ = run(input_data=["TP53"], membership="either")

        assert rows
        assert all(r["variant_1_from_input"] for r in rows)
        assert not any(r["variant_2_from_input"] for r in rows)

    def test_the_mode_is_a_column_and_a_provenance_entry(self, run):
        rows, result = run(input_data=["TP53"], membership="either")

        assert {r["membership"] for r in rows} == {"either"}
        assert result.provenance["pairing"]["membership"] == "either"

    def test_an_unknown_mode_is_refused_by_name(self, run):
        with pytest.raises(ValueError, match="membership must be one of"):
            run(input_data=["TP53"], membership="any")


class TestGroupSize:
    """
    The parameter that decides both the size and the meaning of the
    answer. A pathway naming 2,615 genes links its members by saying
    almost nothing about them.
    """

    def test_a_group_reaching_too_many_genes_is_excluded(self, run):
        """DISEASE reaches three genes; PATHWAY reaches two."""
        rows, _ = run(
            input_data=["TP53", "BRCA1"],
            group_types=["Pathways", "Diseases"],
            max_group_size=2,
        )

        assert {t for r in rows for t in r["group_support_types"]} == {"Pathways"}

    def test_raising_it_lets_the_larger_group_back_in(self, run):
        rows, _ = run(
            input_data=["TP53", "BRCA1"],
            group_types=["Pathways", "Diseases"],
            max_group_size=10,
        )

        assert {t for r in rows for t in r["group_support_types"]} == {
            "Pathways",
            "Diseases",
        }

    def test_zero_means_no_limit_rather_than_the_default(self, run):
        _, result = run(
            input_data=["TP53", "BRCA1"],
            group_types=["Diseases"],
            max_group_size=0,
        )

        assert result.provenance["group_filter"]["max_group_size"] is None
        assert result.provenance["group_filter"]["groups_excluded_by_size"] == 0

    def test_an_empty_result_says_the_filter_caused_it(self, run):
        """
        Otherwise an empty answer reads as "these genes share no
        biology", when the truth is "the only thing linking them is a
        group you excluded".
        """
        rows, result = run(
            input_data=["TP53", "BRCA1"],
            group_types=["Diseases"],
            max_group_size=2,
        )
        excluded = result.provenance["group_filter"]

        assert rows == []
        assert excluded["groups_excluded_by_size"] >= 1
        assert excluded["smallest_excluded"] == 3
        assert "raising max_group_size" in excluded["means"]

    def test_a_negative_size_is_refused(self, run):
        with pytest.raises(ValueError, match="max_group_size must be 0 or positive"):
            run(input_data=["TP53"], max_group_size=-1)


class TestSupport:
    def test_the_count_is_the_number_of_distinct_groups(self, run):
        rows, _ = run(
            input_data=["TP53", "BRCA1"],
            group_types=["Pathways", "Diseases"],
            max_group_size=10,
        )

        assert {r["group_support_count"] for r in rows} == {2}

    def test_min_group_support_filters_on_it(self, run):
        rows, _ = run(
            input_data=["TP53", "BRCA1"],
            group_types=["Pathways"],
            min_group_support=2,
        )

        assert rows == []

    def test_support_below_one_is_refused(self, run):
        with pytest.raises(ValueError, match="min_group_support must be at least 1"):
            run(input_data=["TP53"], min_group_support=0)


class TestGroupTypes:
    def test_a_misspelled_group_type_is_refused_rather_than_returning_nothing(
        self, run
    ):
        with pytest.raises(ValueError, match="Unknown group_types"):
            run(input_data=["TP53"], group_types=["Pathway"])

    def test_the_error_lists_what_the_bundle_has(self, run):
        with pytest.raises(ValueError, match="Pathways"):
            run(input_data=["TP53"], group_types=["Nonsense"])

    def test_gene_ontology_is_named_as_unusable(self, run):
        """It has entities and no relationships, so it cannot link genes."""
        with pytest.raises(ValueError, match="Gene Ontology carries"):
            run(input_data=["TP53"], group_types=["Nonsense"])

    def test_an_empty_list_is_refused(self, run):
        with pytest.raises(ValueError, match="at least one entity group"):
            run(input_data=["TP53"], group_types=[])


class TestTheVariantsPerGeneCap:
    def test_it_bounds_the_pairs(self, run):
        """Pairs grow with the square, so this is the real size control."""
        rows, _ = run(input_data=["TP53", "BRCA1"], max_variants_per_gene=1)

        assert len(rows) == 1

    def test_a_variant_named_by_hand_is_never_dropped(self, run):
        """
        The cap keeps the commonest variants. One the caller asked for by
        name is not a candidate for being dropped.
        """
        rows, _ = run(
            input_data=["17:300", "17:5200"], max_variants_per_gene=1
        )

        assert _pairs(rows) == {("17:300:A:G", "17:5200:A:G")}

    def test_zero_means_no_cap(self, run):
        _, result = run(input_data=["TP53", "BRCA1"], max_variants_per_gene=0)

        assert result.provenance["pairing"]["max_variants_per_gene"] is None
        assert result.provenance["pairing"]["variants_kept_per_gene"] == "all of them"


class TestThePairCap:
    def test_truncation_is_reported(self, run):
        _, result = run(input_data=["TP53", "BRCA1"], max_pairs=2)
        cut = result.provenance["truncation"]

        assert cut["applied"] is True
        assert cut["returned"] == 2
        assert "not every pair" in cut["means"]

    def test_an_untruncated_run_says_so(self, run):
        _, result = run(input_data=["TP53", "BRCA1"])

        assert result.provenance["truncation"]["applied"] is False


class TestPairIdentity:
    def test_a_pair_is_never_returned_in_both_orientations(self, run):
        rows, _ = run(
            input_data=["TP53", "BRCA1"],
            group_types=["Pathways", "Diseases"],
            max_group_size=10,
        )
        unordered = [
            tuple(sorted((r["variant_1_key"], r["variant_2_key"]))) for r in rows
        ]

        assert len(unordered) == len(set(unordered))

    def test_a_pair_is_always_across_two_distinct_genes(self, run):
        rows, _ = run(input_data=["TP53", "BRCA1"])

        assert all(r["gene_1_id"] != r["gene_2_id"] for r in rows)

    def test_no_variant_is_paired_with_itself(self, run):
        rows, _ = run(input_data=["TP53", "BRCA1"], membership="either")

        assert all(r["variant_1_key"] != r["variant_2_key"] for r in rows)


class TestTheWindow:
    def test_it_widens_which_genes_a_variant_belongs_to(self, run):
        """17:9000 lies outside every gene; a wide window reaches BRCA1."""
        narrow, _ = run(input_data=["17:9000"], membership="either")
        wide, _ = run(input_data=["17:9000"], membership="either", window_bp=4_000)

        assert narrow == []
        assert wide

    def test_a_negative_window_is_refused(self, run):
        with pytest.raises(ValueError, match="window_bp must be 0 or positive"):
            run(input_data=["TP53"], window_bp=-1)


class TestTheContract:
    def test_declared_columns_match_what_comes_back(self, run):
        from biofilter.modules.report.reports.report_pair_variants import (
            PairVariantsReport,
        )

        _, result = run(input_data=["TP53", "BRCA1"])

        assert list(result.table.column_names) == list(
            PairVariantsReport.available_columns()
        )

    def test_provenance_records_how_the_pairs_were_made(self, run):
        _, result = run(input_data=["TP53", "BRCA1"])
        pairing = result.provenance["pairing"]

        assert pairing["group_types"] == ["Pathways"]
        assert "Both members come from the input." in pairing["means"]
