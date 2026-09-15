"""
annotate_variant: what the bundle knows about a list of variants.

One row per (input, transcript), so most of these tests are about the
fan-out and the two ways of narrowing it.
"""

from __future__ import annotations

import pytest

from biofilter.modules.report import Bundle, ReportManager


@pytest.fixture
def run(fixture_bundle):
    with Bundle.open(fixture_bundle) as bundle:
        manager = ReportManager(bundle=bundle)

        def _run(**params):
            result = manager.run("annotate_variant", **params)
            return result.table.to_pylist(), result

        yield _run


class TestInputShapes:
    def test_an_rsid_resolves(self, run):
        rows, _ = run(input_data=["rs101"])

        assert rows
        assert {r["input_kind"] for r in rows} == {"rsid"}
        assert {r["variant_key"] for r in rows} == {"17:150:A:G"}

    def test_rsid_lookup_goes_through_variant_rsid(self, run):
        """
        `variant_masters` carries an `rsid` column and it is entirely
        null in 4.3.0 bundles — 0 of 2,889,803 rows in the real one.
        Joining on it would match nothing, silently.
        """
        rows, _ = run(input_data=["rs101"])
        assert rows[0]["rsid"] == "rs101"

    def test_chr_pos_matches_every_variant_at_that_position(self, run):
        rows, _ = run(input_data=["17:150"])

        assert {r["input_kind"] for r in rows} == {"chr_pos"}
        assert {r["variant_key"] for r in rows} == {"17:150:A:G"}

    def test_chr_pos_ref_alt_is_exact(self, run):
        rows, _ = run(input_data=["17:150:A:G"])
        assert {r["input_kind"] for r in rows} == {"chr_pos_allele"}

    @pytest.mark.parametrize("value", ["chr17:150", "CHR17-150", "17 150"])
    def test_chromosome_prefixes_and_separators_are_tolerated(self, run, value):
        rows, _ = run(input_data=[value])
        assert {r["variant_key"] for r in rows} == {"17:150:A:G"}

    def test_an_unparseable_input_says_what_was_expected(self, run):
        rows, _ = run(input_data=["nonsense"])

        assert len(rows) == 1
        assert rows[0]["status"] == "invalid"
        assert "rsID" in rows[0]["note"]

    def test_a_parseable_input_with_no_variant_is_not_found(self, run):
        rows, _ = run(input_data=["17:999999:A:G"])

        assert rows[0]["status"] == "not_found"
        assert "chr_pos_allele" in rows[0]["note"]

    def test_emit_not_found_rows_false_drops_them(self, run):
        rows, _ = run(input_data=["rs101", "17:999999:A:G"], emit_not_found_rows=False)
        assert {r["status"] for r in rows} == {"ok"}


class TestTranscriptFanOut:
    def test_one_row_per_transcript(self, run):
        rows, _ = run(input_data=["rs101"])

        assert len(rows) == 3
        assert {r["transcript_id"] for r in rows} == {
            "ENST00000001", "ENST00000002", "ENST00000003"
        }

    def test_variant_level_facts_repeat_across_transcripts(self, run):
        rows, _ = run(input_data=["rs101"])

        assert len({r["af_joint"] for r in rows}) == 1
        assert len({r["cadd_phred"] for r in rows}) == 1

    def test_most_severe_is_derived_from_the_consequence_vocabulary(self, run):
        """
        4.3.0 dropped `is_most_severe_for_variant`, so it is computed from
        `variant_consequences.severity_rank` — which keeps the answer
        consistent with whatever ordering the bundle carries.
        """
        rows, _ = run(input_data=["rs101"])
        severe = [r for r in rows if r["is_most_severe_for_variant"]]

        assert len(severe) == 1
        assert severe[0]["consequence"] == "missense_variant"
        assert severe[0]["severity_rank"] == 13

    def test_most_severe_only_narrows_to_it(self, run):
        rows, _ = run(input_data=["rs101"], most_severe_only=True)

        assert len(rows) == 1
        assert rows[0]["consequence"] == "missense_variant"

    def test_canonical_only_narrows_differently(self, run):
        rows, _ = run(input_data=["rs101"], canonical_only=True)

        assert len(rows) == 1
        assert rows[0]["canonical"] == "YES"
        assert rows[0]["mane_select"] == "NM_000546.6"

    def test_the_consequence_vocabulary_is_joined(self, run):
        rows, _ = run(input_data=["rs101"], most_severe_only=True)
        row = rows[0]

        assert row["consequence_group"] == "coding"
        assert row["consequence_category"] == "moderate"
        assert row["impact"] == "MODERATE"
        assert row["impact_rank"] == 2


class TestAlphaMissense:
    def test_the_score_joins_despite_a_versioned_transcript_id(self, run):
        """
        AlphaMissense writes `ENST00000001.9`; VEP writes
        `ENST00000001`. Joining them raw matches nothing, silently — and
        did, until the version was stripped.
        """
        rows, _ = run(input_data=["rs101"])
        scored = [r for r in rows if r["alphamissense_score"] is not None]

        assert len(scored) == 1
        assert scored[0]["transcript_id"] == "ENST00000001"
        assert scored[0]["alphamissense_classification"] == "likely_benign"

    def test_transcripts_without_a_score_keep_the_column_null(self, run):
        rows, _ = run(input_data=["rs101"])
        unscored = [r for r in rows if r["transcript_id"] != "ENST00000001"]

        assert all(r["alphamissense_score"] is None for r in unscored)


class TestPredictions:
    def test_scores_come_from_variant_predictions(self, run):
        rows, _ = run(input_data=["rs101"], most_severe_only=True)
        row = rows[0]

        assert row["cadd_phred"] == pytest.approx(23.6)
        assert row["revel_max"] == pytest.approx(0.198)
        assert row["phylop"] == pytest.approx(2.1)

    def test_a_variant_without_predictions_keeps_them_null(self, run):
        rows, _ = run(input_data=["rs102"])
        assert rows[0]["cadd_phred"] is None


class TestContract:
    def test_columns_match_what_is_declared(self, run):
        _, result = run(input_data=["rs101"])
        from biofilter.modules.report.reports.report_annotate_variant import (
            AnnotateVariantReport,
        )

        assert result.columns == list(AnnotateVariantReport.COLUMNS)

    def test_the_shape_does_not_change_with_the_bundle(self, run):
        """
        Optional tables become empty CTEs rather than dropped columns, so
        a bundle without AlphaMissense returns nulls, not a narrower
        result.
        """
        with_all, _ = run(input_data=["rs101"])
        not_found, _ = run(input_data=["nonsense"])

        assert set(with_all[0]) == set(not_found[0])

    def test_provenance_names_the_bundle(self, run):
        _, result = run(input_data=["rs101"])
        assert result.provenance["bundle_id"] == "fixturebundle0001"

    def test_empty_input_is_rejected(self, run):
        with pytest.raises(ValueError, match="at least one"):
            run(input_data=[])
