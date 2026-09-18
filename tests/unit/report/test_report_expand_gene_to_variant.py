"""
expand_gene_to_variant: the variants belonging to a list of genes.

"Belonging to" is two different questions, and the report makes the
caller pick. The fixture is built so the two disagree: `17:200` sits
inside TP53's range (100-400) but VEP attributed it to BRCA1. So TP53
finds it by position and not by annotation, and BRCA1 — which lives at
5000-5400 — finds it by annotation and not by position.

That is the same divergence measured on the real bundle: across all 958
chr22 genes with coordinates, 144,488 gene-variant pairs exist only by
position and some 749,680 only by annotation.
"""

from __future__ import annotations

import pytest

from biofilter.modules.report import Bundle, ReportManager


@pytest.fixture
def run(fixture_bundle):
    with Bundle.open(fixture_bundle) as bundle:
        manager = ReportManager(bundle=bundle)

        def _run(**params):
            result = manager.run("expand_gene_to_variant", **params)
            return result.table.to_pylist(), result

        yield _run


def _variants(rows, gene=None):
    return {
        r["variant_key"]
        for r in rows
        if r["status"] == "ok" and (gene is None or r["input_gene"] == gene)
    }


class TestTheTwoMechanismsAreDifferentQuestions:
    """The reason the caller has to choose, rather than the report guessing."""

    def test_position_finds_a_variant_vep_gave_to_another_gene(self, run):
        rows, _ = run(input_data=["TP53"], mapping="position")

        # 17:200 is inside TP53's range; VEP called it BRCA1's.
        assert _variants(rows) == {"17:150:A:G", "17:200:A:G"}

    def test_annotation_follows_vep_and_leaves_it_out(self, run):
        rows, _ = run(input_data=["TP53"], mapping="annotation")

        assert _variants(rows) == {"17:150:A:G"}

    def test_the_same_variant_belongs_to_the_other_gene_by_annotation(self, run):
        rows, _ = run(input_data=["BRCA1"], mapping="annotation")

        assert _variants(rows) == {"17:200:A:G"}

    def test_and_to_neither_gene_by_position(self, run):
        """BRCA1 lives at 5000-5400. Nothing annotated sits there."""
        rows, _ = run(input_data=["BRCA1"], mapping="position")

        assert _variants(rows) == set()
        assert [r["status"] for r in rows] == ["no_variants"]

    def test_an_unknown_mechanism_is_refused_by_name(self, run):
        with pytest.raises(ValueError, match="mapping must be one of"):
            run(input_data=["TP53"], mapping="overlap")


class TestTheChoiceIsRecorded:
    """
    Nothing in the rows reveals which question was asked, so the answer
    has to travel with them.
    """

    def test_provenance_names_the_mechanism_and_what_it_means(self, run):
        _, result = run(input_data=["TP53"], mapping="annotation")
        mapping = result.provenance["mapping"]

        assert mapping["mechanism"] == "annotation"
        assert "VEP" in mapping["means"]

    def test_position_records_the_build_and_the_window(self, run):
        _, result = run(input_data=["TP53"], mapping="position", window_bp=50)
        mapping = result.provenance["mapping"]

        assert mapping["mechanism"] == "position"
        assert mapping["build"] == 38
        assert mapping["window_bp"] == 50

    def test_the_mechanism_is_a_column_too(self, run):
        """So a CSV that outlived its provenance file still says which."""
        rows, _ = run(input_data=["TP53"], mapping="position")

        assert {r["mapping"] for r in rows} == {"position"}


class TestTheWindow:
    def test_it_reaches_backwards_past_the_start(self, run):
        """
        BRCA1 starts at 5000 and nothing annotated lies in its body. A
        window of 4800 reaches back to 200, where `17:200` sits.
        """
        narrow, _ = run(input_data=["BRCA1"], mapping="position", window_bp=0)
        wide, _ = run(input_data=["BRCA1"], mapping="position", window_bp=4800)

        assert _variants(narrow) == set()
        assert _variants(wide) == {"17:200:A:G"}

    def test_it_reaches_forwards_past_the_end(self, run):
        """TP53 ends at 400; widening cannot lose what it already had."""
        narrow, _ = run(input_data=["TP53"], mapping="position", window_bp=0)
        wide, _ = run(input_data=["TP53"], mapping="position", window_bp=10_000)

        assert _variants(narrow) == {"17:150:A:G", "17:200:A:G"}
        assert _variants(wide) >= _variants(narrow)

    def test_it_is_refused_for_annotation_with_a_reason(self, run):
        """
        VEP's association already reaches beyond the gene body. Silently
        ignoring the window would let a caller believe it applied.
        """
        with pytest.raises(ValueError, match="mapping='position' only"):
            run(input_data=["TP53"], mapping="annotation", window_bp=1000)

    def test_a_negative_window_is_refused(self, run):
        with pytest.raises(ValueError, match="window_bp must be 0 or positive"):
            run(input_data=["TP53"], mapping="position", window_bp=-5)


class TestTheCapIsVisible:
    """
    A capped gene looks exactly like a complete answer: a round number of
    rows and nothing admitting more existed.
    """

    def test_truncation_is_reported_per_gene_with_the_total(self, run):
        _, result = run(input_data=["TP53"], mapping="position", max_variants_per_gene=1)
        cut = result.provenance["truncation"]

        assert cut["applied"] is True
        assert cut["genes"]["TP53"] == {"returned": 1, "available": 2}
        assert "max_variants_per_gene" in cut["means"]

    def test_an_untruncated_run_says_so(self, run):
        _, result = run(input_data=["TP53"], mapping="position")
        cut = result.provenance["truncation"]

        assert cut["applied"] is False
        assert cut["genes"] is None

    def test_zero_means_no_cap_rather_than_the_default(self, run):
        """
        Regression: `int(param or DEFAULT)` reads well and is wrong — it
        turns the caller's explicit 0 back into the cap.
        """
        _, result = run(
            input_data=["TP53"], mapping="position", max_variants_per_gene=0
        )

        assert result.provenance["truncation"]["max_variants_per_gene"] is None
        assert result.provenance["truncation"]["applied"] is False

    def test_the_rows_kept_are_the_most_severe(self, run):
        rows, _ = run(
            input_data=["TP53"],
            mapping="position",
            max_variants_per_gene=1,
            most_severe_only=True,
        )
        kept = [r for r in rows if r["status"] == "ok"]

        assert len(kept) == 1
        assert kept[0]["consequence"] == "missense_variant"

    def test_a_negative_cap_is_refused(self, run):
        with pytest.raises(ValueError, match="max_variants_per_gene must be 0"):
            run(input_data=["TP53"], max_variants_per_gene=-1)


class TestGenesThatProduceNothing:
    def test_an_unresolvable_input_says_so_rather_than_vanishing(self, run):
        rows, _ = run(input_data=["NOT_A_GENE"], mapping="annotation")

        assert [r["status"] for r in rows] == ["not_found"]
        assert "did not resolve" in rows[0]["note"]

    def test_a_gene_with_no_coordinates_is_not_a_gene_with_no_variants(self, run):
        """
        NOLOC1 carries no `entity_locations` row. Under position mapping
        the honest answer is that the bundle cannot place it.
        """
        rows, _ = run(input_data=["NOLOC1"], mapping="position")

        assert [r["status"] for r in rows] == ["no_location"]
        assert rows[0]["gene_symbol"] == "NOLOC1"
        assert "no build-38 coordinates" in rows[0]["note"]
        assert "annotation" in rows[0]["note"]

    def test_the_same_gene_under_annotation_is_simply_unannotated(self, run):
        """Missing coordinates are irrelevant when VEP does the mapping."""
        rows, _ = run(input_data=["NOLOC1"], mapping="annotation")

        assert [r["status"] for r in rows] == ["no_variants"]
        assert "VEP associated no variant" in rows[0]["note"]

    def test_a_placed_gene_with_nothing_in_range_is_not_confused_with_it(self, run):
        """BRCA1 has coordinates; nothing annotated lies in them."""
        rows, _ = run(input_data=["BRCA1"], mapping="position")

        assert [r["status"] for r in rows] == ["no_variants"]
        assert "resolved and placed" in rows[0]["note"]

    def test_they_can_be_suppressed(self, run):
        rows, _ = run(
            input_data=["TP53", "NOT_A_GENE"],
            mapping="annotation",
            emit_not_found_rows=False,
        )

        assert {r["status"] for r in rows} == {"ok"}

    def test_an_empty_input_is_refused(self, run):
        with pytest.raises(ValueError, match="at least one gene"):
            run(input_data=[], mapping="annotation")


class TestWhatTravelsWithEachVariant:
    def test_the_rsid_when_the_bundle_carries_one(self, run):
        rows, _ = run(input_data=["TP53"], mapping="annotation")

        assert {r["rsid"] for r in rows if r["status"] == "ok"} == {"rs101"}

    def test_the_in_silico_predictors(self, run):
        rows, _ = run(input_data=["TP53"], mapping="annotation")
        row = next(r for r in rows if r["status"] == "ok")

        assert row["cadd_phred"] == pytest.approx(23.6)
        assert row["sift_max"] == pytest.approx(0.01)

    def test_alphamissense_joins_across_the_transcript_version(self, run):
        """
        AlphaMissense writes `ENST00000001.9`; VEP writes `ENST00000001`.
        Joining on the raw string silently scores nothing.
        """
        rows, _ = run(
            input_data=["TP53"], mapping="annotation", most_severe_only=False
        )
        scored = [r for r in rows if r["alphamissense_score"] is not None]

        assert [r["transcript_id"] for r in scored] == ["ENST00000001"]
        assert scored[0]["alphamissense_classification"] == "likely_benign"

    def test_a_variant_row_gets_the_variant_score_not_its_transcript_s(self, run):
        """
        AlphaMissense scored `17:200` on ENST00000099, which VEP never
        reports for it. Requiring the transcripts to agree drops the
        score — silently, as a null. On the real bundle that lost ~97%
        of the AlphaMissense scores the data actually held.
        """
        rows, result = run(
            input_data=["BRCA1"], mapping="annotation", most_severe_only=True
        )
        row = next(r for r in rows if r["variant_key"] == "17:200:A:G")

        assert row["transcript_id"] == "ENST00000009"
        assert row["alphamissense_score"] == pytest.approx(0.91)
        assert result.provenance["alphamissense"]["joined_on"] == "variant"

    def test_a_transcript_row_only_gets_its_own_transcript_s_score(self, run):
        """The other grain: one row per transcript means a per-transcript score."""
        rows, result = run(
            input_data=["BRCA1"], mapping="annotation", most_severe_only=False
        )
        row = next(r for r in rows if r["variant_key"] == "17:200:A:G")

        assert row["alphamissense_score"] is None
        assert result.provenance["alphamissense"]["joined_on"] == "variant and transcript"

    def test_severity_comes_from_the_consequence_catalogue(self, run):
        rows, _ = run(
            input_data=["TP53"], mapping="annotation", most_severe_only=False
        )
        by_consequence = {r["consequence"]: r["severity_rank"] for r in rows}

        assert by_consequence["missense_variant"] == 13
        assert by_consequence["intron_variant"] == 25


class TestOneRowPerVariant:
    def test_most_severe_only_collapses_the_transcripts(self, run):
        """17:150 is annotated on three transcripts of TP53."""
        rows, _ = run(input_data=["TP53"], mapping="annotation", most_severe_only=True)

        assert len(rows) == 1
        assert rows[0]["consequence"] == "missense_variant"

    def test_turning_it_off_returns_every_transcript(self, run):
        rows, _ = run(input_data=["TP53"], mapping="annotation", most_severe_only=False)

        assert len(rows) == 3
        assert len({r["transcript_id"] for r in rows}) == 3


class TestFilters:
    def test_impact_filter(self, run):
        rows, _ = run(
            input_data=["TP53"],
            mapping="annotation",
            most_severe_only=False,
            impact_filter=["MODIFIER"],
        )

        assert {r["consequence"] for r in rows} == {"intron_variant"}

    def test_consequence_filter(self, run):
        rows, _ = run(
            input_data=["TP53"],
            mapping="annotation",
            most_severe_only=False,
            consequence_type_filter=["synonymous_variant"],
        )

        assert {r["impact"] for r in rows} == {"LOW"}

    def test_a_predictor_threshold(self, run):
        rows, _ = run(
            input_data=["TP53"], mapping="annotation", cadd_phred_min=99
        )

        assert {r["status"] for r in rows} == {"no_variants"}

    def test_a_filter_leaving_nothing_still_names_the_gene(self, run):
        rows, _ = run(input_data=["TP53"], mapping="annotation", cadd_phred_min=99)

        assert rows[0]["gene_symbol"] == "TP53"
        assert "no variant meeting the criteria" in rows[0]["note"]


class TestTheContract:
    def test_declared_columns_match_what_comes_back(self, run):
        from biofilter.modules.report.reports.report_expand_gene_to_variant import (
            ExpandGeneToVariantReport,
        )

        _, result = run(input_data=["TP53"], mapping="annotation")

        assert list(result.table.column_names) == list(
            ExpandGeneToVariantReport.available_columns()
        )

    def test_the_example_input_runs(self, run):
        from biofilter.modules.report.reports.report_expand_gene_to_variant import (
            ExpandGeneToVariantReport,
        )

        example = dict(ExpandGeneToVariantReport.example_input())
        example["input_data"] = ["TP53"]
        rows, _ = run(**example)

        assert rows
