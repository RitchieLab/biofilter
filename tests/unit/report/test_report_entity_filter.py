"""
entity_filter: does the bundle know this name, and unambiguously?

Not an annotation report. It returns one row per **match**, so an input
that resolves to three entities produces three rows, each flagged — that
ambiguity is the answer, not a problem to hide.
"""

from __future__ import annotations

import pytest

from biofilter.modules.report import Bundle, ReportManager


@pytest.fixture
def run(fixture_bundle):
    with Bundle.open(fixture_bundle) as bundle:
        manager = ReportManager(bundle=bundle)

        def _run(**params):
            result = manager.run("entity_filter", **params)
            return result.table.to_pylist(), result

        yield _run


class TestExactMode:
    def test_a_known_name_resolves(self, run):
        rows, _ = run(input_data=["TP53"])

        assert len(rows) == 1
        row = rows[0]
        assert row["entity_id"] == 1
        assert row["primary_name"] == "TP53"
        assert row["group_name"] == "Genes"
        assert row["is_primary"] is True
        assert row["observation"] == ""

    def test_matching_is_case_insensitive(self, run):
        rows, _ = run(input_data=["tp53"])
        assert rows[0]["entity_id"] == 1

    def test_an_unknown_name_is_kept_and_flagged(self, run):
        """
        Dropping it would leave no way to tell "the bundle does not know
        this name" from "you did not ask about it".
        """
        rows, _ = run(input_data=["TP53", "NOT_A_GENE"])
        by_input = {r["input_original"]: r for r in rows}

        assert by_input["NOT_A_GENE"]["observation"] == "not found"
        assert by_input["NOT_A_GENE"]["entity_id"] is None

    def test_an_ambiguous_name_returns_every_match_flagged(self, run):
        """
        The fixture puts the same alias on two entities. Both rows come
        back, both marked — the report's job is to report the ambiguity,
        not to pick a winner.
        """
        rows, _ = run(input_data=["AMBIGUOUS"])

        assert len(rows) == 2
        assert {r["entity_id"] for r in rows} == {3, 4}
        assert all(r["observation"] == "multiple matches" for r in rows)

    def test_is_deactive_is_the_negation_of_is_active(self, run):
        rows, _ = run(input_data=["TP53"])
        assert rows[0]["is_active"] is True
        assert rows[0]["is_deactive"] is False

    def test_similarity_score_is_null_outside_fuzzy(self, run):
        rows, _ = run(input_data=["TP53"])
        assert rows[0]["similarity_score"] is None


class TestLikeMode:
    def test_matches_aliases_containing_the_input(self, run):
        rows, _ = run(input_data=["BRCA"], match_mode="like")
        assert {r["input"] for r in rows} == {"BRCA1"}

    def test_the_input_must_be_inside_the_alias_not_the_reverse(self, run):
        """
        Only one direction. Matching an alias inside an input would make
        every one-character alias match every input containing it — a
        search for BRCA1 came back with the aliases "1" and "a1" while
        this was symmetric.
        """
        rows, _ = run(input_data=["P04637-2"], match_mode="like")

        # P04637 is *inside* the input, and must not match on that basis.
        assert {r["input"] for r in rows} == {"P04637-2"}


class TestFuzzyMode:
    def test_a_near_miss_matches_and_is_scored(self, run):
        rows, _ = run(input_data=["TP54"], match_mode="fuzzy", similarity_threshold=80)
        by_alias = {r["input"]: r for r in rows}

        assert "TP53" in by_alias
        assert 80 <= by_alias["TP53"]["similarity_score"] < 100

    def test_an_exact_hit_scores_100(self, run):
        rows, _ = run(input_data=["TP53"], match_mode="fuzzy", similarity_threshold=99)
        assert any(r["similarity_score"] == 100 for r in rows)

    def test_the_threshold_excludes(self, run):
        loose, _ = run(input_data=["TP54"], match_mode="fuzzy", similarity_threshold=50)
        strict, _ = run(input_data=["TP54"], match_mode="fuzzy", similarity_threshold=99)

        assert len(loose) > len(strict)

    def test_it_needs_no_optional_dependency(self, run):
        """
        The relational version imported rapidfuzz and raised without it,
        which is why this mode was untestable here. It is now a DuckDB
        function.
        """
        rows, _ = run(input_data=["TP53"], match_mode="fuzzy")
        assert rows


class TestFilteringAndValidation:
    def test_group_filter_restricts_the_search(self, run):
        everywhere, _ = run(input_data=["P04637"], match_mode="like")
        proteins, _ = run(input_data=["P04637"], match_mode="like", group_filter="Proteins")
        genes, _ = run(input_data=["P04637"], match_mode="like", group_filter="Genes")

        assert everywhere
        assert all(r["group_name"] == "Proteins" for r in proteins)
        assert [r["observation"] for r in genes] == ["not found"]

    def test_an_unknown_match_mode_is_rejected(self, run):
        with pytest.raises(ValueError, match="match_mode"):
            run(input_data=["TP53"], match_mode="approximately")

    def test_empty_input_is_rejected(self, run):
        with pytest.raises(ValueError, match="at least one"):
            run(input_data=[])


class TestContract:
    def test_columns_are_the_same_in_every_mode(self, run):
        """
        The relational version added `similarity_score` only in fuzzy
        mode, so the result's shape depended on a parameter. It is always
        present now, and null where it does not apply.
        """
        shapes = {
            mode: run(input_data=["TP53"], match_mode=mode)[1].columns
            for mode in ("exact", "like", "fuzzy")
        }

        assert len(set(map(tuple, shapes.values()))) == 1
        assert shapes["exact"] == list(
            ReportManager().get_class("entity_filter").COLUMNS
        )

    def test_provenance_names_the_bundle(self, run):
        _, result = run(input_data=["TP53"])
        assert result.provenance["bundle_id"] == "fixturebundle0001"
