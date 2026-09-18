"""
pair_variants: candidate pairs whose genes share biology.

Runs against `pairing_bundle` — the shared fixture plus a connector that
reaches two different genes, since without one there is no pair to make.

The shape to keep in mind: PATHWAY reaches TP53 and BRCA1 (two genes).
DISEASE reaches TP53, BRCA1 and DGENE1. TP53 holds variants at 17:150,
200 and 300; BRCA1 holds 17:5100 and 17:5200; 17:9000 sits in no gene.

Only variants go in. The gene path, the gene-to-variant expansion and
`membership` were removed in ADR-005 D13, so a good part of this file is
about refusing them clearly rather than ignoring them.
"""

from __future__ import annotations

import pytest

from biofilter.modules.report import Bundle, ReportManager

TP53_VARIANTS = ["17:150", "17:200", "17:300"]
BRCA1_VARIANTS = ["17:5100", "17:5200"]


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
    def test_variants_in_two_linked_genes_pair(self, run):
        """3 variants in TP53 x 2 in BRCA1 = 6 pairs, one gene pair."""
        rows, _ = run(input_data=TP53_VARIANTS + BRCA1_VARIANTS)

        assert len(rows) == 6
        assert _pairs(rows) == {
            (a, b)
            for a in ("17:150:A:G", "17:200:A:G", "17:300:A:G")
            for b in ("17:5100:A:G", "17:5200:A:G")
        }

    def test_variants_in_one_gene_do_not_pair_with_each_other(self, run):
        """A pair is across two genes; two variants in TP53 are not one."""
        rows, _ = run(input_data=TP53_VARIANTS)

        assert rows == []

    def test_a_variant_in_no_gene_cannot_pair(self, run):
        """17:9000 sits outside every gene's range."""
        rows, _ = run(input_data=["17:9000"] + BRCA1_VARIANTS)

        assert rows == []

    def test_a_variant_the_bundle_does_not_have(self, run):
        rows, _ = run(input_data=["17:424242"] + BRCA1_VARIANTS)

        assert rows == []

    def test_an_empty_input_is_refused(self, run):
        with pytest.raises(ValueError, match="at least one variant"):
            run(input_data=[])


class TestItTakesOnlyVariants:
    """
    ADR-005 D13. The gene path was an implicit chain and it hid which of
    a gene's thousands of variants reached the pairing.
    """

    def test_a_gene_name_is_refused_by_name(self, run):
        with pytest.raises(ValueError, match="pair_variants takes variants"):
            run(input_data=["TP53", "BRCA1"])

    def test_the_refusal_names_the_offending_values(self, run):
        with pytest.raises(ValueError, match="'TP53'"):
            run(input_data=["TP53", "17:5100"])

    def test_the_refusal_points_at_the_report_that_does_it(self, run):
        with pytest.raises(ValueError, match="expand_gene_to_variant"):
            run(input_data=["TP53"])

    def test_one_bad_value_among_good_ones_still_refuses(self, run):
        with pytest.raises(ValueError, match="pair_variants takes variants"):
            run(input_data=TP53_VARIANTS + ["BRCA1"])


class TestParametersThatWereRemoved:
    """
    Silently ignoring one would hand a caller a different answer to the
    question they think they asked, which is worse than failing.
    """

    @pytest.mark.parametrize(
        "name,value",
        [
            ("membership", "either"),
            ("max_variants_per_gene", 10),
            ("output_grain", "gene_pairs"),
        ],
    )
    def test_each_is_refused(self, run, name, value):
        with pytest.raises(ValueError, match="no longer takes"):
            run(input_data=TP53_VARIANTS + BRCA1_VARIANTS, **{name: value})

    def test_membership_says_what_replaced_it(self, run):
        with pytest.raises(ValueError, match="pair_genes"):
            run(input_data=TP53_VARIANTS, membership="either")

    def test_max_variants_per_gene_says_where_it_went(self, run):
        with pytest.raises(ValueError, match="expand_gene_to_variant"):
            run(input_data=TP53_VARIANTS, max_variants_per_gene=10)

    def test_several_at_once_are_all_named(self, run):
        with pytest.raises(ValueError, match="membership, max_variants_per_gene"):
            run(
                input_data=TP53_VARIANTS,
                membership="both",
                max_variants_per_gene=5,
            )


class TestInputShapes:
    def test_a_position_without_alleles(self, run):
        rows, _ = run(input_data=["17:150", "17:5100"])

        assert _pairs(rows) == {("17:150:A:G", "17:5100:A:G")}

    def test_a_position_with_alleles(self, run):
        rows, _ = run(input_data=["17:150:A:G", "17:5100:A:G"])

        assert _pairs(rows) == {("17:150:A:G", "17:5100:A:G")}

    def test_an_rsid(self, run):
        """rs101 is 17:150 in the fixture."""
        rows, _ = run(input_data=["rs101", "17:5100"])

        assert _pairs(rows) == {("17:150:A:G", "17:5100:A:G")}

    def test_the_text_that_named_each_side_comes_back(self, run):
        rows, _ = run(input_data=["rs101", "17:5100"])

        assert {rows[0]["input_1"], rows[0]["input_2"]} == {"rs101", "17:5100"}


class TestGroupSize:
    """
    The parameter that decides both the size and the meaning of the
    answer. A pathway naming 2,615 genes links its members by saying
    almost nothing about them.
    """

    def test_a_group_reaching_too_many_genes_is_excluded(self, run):
        """DISEASE reaches three genes; PATHWAY reaches two."""
        rows, _ = run(
            input_data=TP53_VARIANTS + BRCA1_VARIANTS,
            group_types=["Pathways", "Diseases"],
            max_group_size=2,
        )

        assert {t for r in rows for t in r["group_support_types"]} == {"Pathways"}

    def test_raising_it_lets_the_larger_group_back_in(self, run):
        rows, _ = run(
            input_data=TP53_VARIANTS + BRCA1_VARIANTS,
            group_types=["Pathways", "Diseases"],
            max_group_size=10,
        )

        assert {t for r in rows for t in r["group_support_types"]} == {
            "Pathways",
            "Diseases",
        }

    def test_zero_means_no_limit_rather_than_the_default(self, run):
        _, result = run(
            input_data=TP53_VARIANTS + BRCA1_VARIANTS,
            group_types=["Diseases"],
            max_group_size=0,
        )

        assert result.provenance["group_filter"]["max_group_size"] is None
        assert result.provenance["group_filter"]["groups_excluded_by_size"] == 0

    def test_an_empty_result_says_the_filter_caused_it(self, run):
        rows, result = run(
            input_data=TP53_VARIANTS + BRCA1_VARIANTS,
            group_types=["Diseases"],
            max_group_size=2,
        )
        excluded = result.provenance["group_filter"]

        assert rows == []
        assert excluded["groups_excluded_by_size"] >= 1
        assert "raising max_group_size" in excluded["means"]

    def test_a_negative_size_is_refused(self, run):
        with pytest.raises(ValueError, match="max_group_size must be 0 or positive"):
            run(input_data=TP53_VARIANTS, max_group_size=-1)


class TestSupport:
    def test_the_count_is_the_number_of_distinct_groups(self, run):
        rows, _ = run(
            input_data=TP53_VARIANTS + BRCA1_VARIANTS,
            group_types=["Pathways", "Diseases"],
            max_group_size=10,
        )

        assert {r["group_support_count"] for r in rows} == {2}

    def test_min_group_support_filters_on_it(self, run):
        rows, _ = run(
            input_data=TP53_VARIANTS + BRCA1_VARIANTS,
            group_types=["Pathways"],
            min_group_support=2,
        )

        assert rows == []

    def test_the_curation_behind_a_pair_is_named(self, run):
        """Read from the bundle, not guessed from an accession prefix."""
        rows, _ = run(input_data=TP53_VARIANTS + BRCA1_VARIANTS)

        assert rows[0]["group_support_sources"]
        assert rows[0]["group_support_source_count"] == len(
            set(rows[0]["group_support_sources"])
        )

    def test_min_group_sources_is_a_stronger_claim(self, run):
        rows, _ = run(
            input_data=TP53_VARIANTS + BRCA1_VARIANTS,
            group_types=["Pathways", "Diseases"],
            max_group_size=10,
            min_group_sources=3,
        )

        assert rows == []

    def test_support_below_one_is_refused(self, run):
        with pytest.raises(ValueError, match="min_group_support must be at least 1"):
            run(input_data=TP53_VARIANTS, min_group_support=0)

    def test_sources_below_one_is_refused(self, run):
        with pytest.raises(ValueError, match="min_group_sources must be at least 1"):
            run(input_data=TP53_VARIANTS, min_group_sources=0)


class TestGroupTypes:
    def test_a_misspelled_group_type_is_refused_rather_than_empty(self, run):
        with pytest.raises(ValueError, match="Unknown group_types"):
            run(input_data=TP53_VARIANTS, group_types=["Pathway"])

    def test_gene_ontology_is_named_as_unusable(self, run):
        with pytest.raises(ValueError, match="Gene Ontology carries"):
            run(input_data=TP53_VARIANTS, group_types=["Nonsense"])


class TestTheWindow:
    def test_it_widens_which_genes_a_variant_belongs_to(self, run):
        """17:9000 lies outside every gene; a wide window reaches BRCA1."""
        narrow, _ = run(input_data=["17:9000"] + TP53_VARIANTS)
        wide, _ = run(input_data=["17:9000"] + TP53_VARIANTS, window_bp=4_000)

        assert narrow == []
        assert wide

    def test_a_negative_window_is_refused(self, run):
        with pytest.raises(ValueError, match="window_bp must be 0 or positive"):
            run(input_data=TP53_VARIANTS, window_bp=-1)


class TestThePairCap:
    def test_truncation_is_reported(self, run):
        _, result = run(input_data=TP53_VARIANTS + BRCA1_VARIANTS, max_pairs=2)
        cut = result.provenance["truncation"]

        assert cut["applied"] is True
        assert cut["returned"] == 2
        assert "not every pair" in cut["means"]

    def test_an_untruncated_run_says_so(self, run):
        _, result = run(input_data=TP53_VARIANTS + BRCA1_VARIANTS)

        assert result.provenance["truncation"]["applied"] is False


class TestPairIdentity:
    def test_a_pair_is_never_returned_in_both_orientations(self, run):
        rows, _ = run(
            input_data=TP53_VARIANTS + BRCA1_VARIANTS,
            group_types=["Pathways", "Diseases"],
            max_group_size=10,
        )
        unordered = [
            tuple(sorted((r["variant_1_key"], r["variant_2_key"]))) for r in rows
        ]

        assert len(unordered) == len(set(unordered))

    def test_a_pair_is_always_across_two_distinct_genes(self, run):
        rows, _ = run(input_data=TP53_VARIANTS + BRCA1_VARIANTS)

        assert all(r["gene_1_id"] != r["gene_2_id"] for r in rows)

    def test_no_variant_is_paired_with_itself(self, run):
        rows, _ = run(input_data=TP53_VARIANTS + BRCA1_VARIANTS)

        assert all(r["variant_1_key"] != r["variant_2_key"] for r in rows)


class TestAlleleFrequency:
    def test_a_filter_that_excludes_everything_leaves_no_pair(self, run):
        rows, _ = run(input_data=TP53_VARIANTS + BRCA1_VARIANTS, af_min=0.99)

        assert rows == []


class TestTheContract:
    def test_declared_columns_match_what_comes_back(self, run):
        from biofilter.modules.report.reports.report_pair_variants import (
            PairVariantsReport,
        )

        _, result = run(input_data=TP53_VARIANTS + BRCA1_VARIANTS)

        assert list(result.columns) == list(
            PairVariantsReport.available_columns()
        )

    def test_provenance_records_how_the_pairs_were_made(self, run):
        _, result = run(input_data=TP53_VARIANTS + BRCA1_VARIANTS)
        pairing = result.provenance["pairing"]

        assert pairing["group_types"] == ["Pathways"]
        assert "variants you named" in pairing["means"]
        assert "expand_gene_to_variant" in pairing["means"]

    def test_it_declares_no_membership(self):
        from biofilter.modules.report.reports import report_pair_variants as mod

        assert not hasattr(mod, "MEMBERSHIPS")
        assert not hasattr(mod, "DEFAULT_MAX_VARIANTS_PER_GENE")
