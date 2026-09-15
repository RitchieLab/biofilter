"""
expand_entity_relationship: which links the bundle holds for these entities.

One row per (input entity, relationship). A relationship with an input on
both sides is reached from each, which is what the scope and dedup
parameters are about.
"""

from __future__ import annotations

import pytest

from biofilter.modules.report import Bundle, ReportManager


@pytest.fixture
def run(fixture_bundle):
    with Bundle.open(fixture_bundle) as bundle:
        manager = ReportManager(bundle=bundle)

        def _run(**params):
            result = manager.run("expand_entity_relationship", **params)
            return result.table.to_pylist(), result

        yield _run


class TestExpansion:
    def test_an_input_is_expanded_into_its_relationships(self, run):
        """TP53 is on the left of two relationships and the right of a third."""
        rows, _ = run(input_data=["TP53"])
        assert len(rows) == 3
        assert {r["related_primary_name"] for r in rows} == {"P04637", "R-HSA-0001"}

    def test_both_sides_are_reported_with_direction(self, run):
        rows, _ = run(input_data=["TP53"])
        by_side = {r["match_side"] for r in rows}
        directions = {r["direction"] for r in rows}

        assert by_side == {"entity_1", "entity_2"}
        assert directions == {"input->related", "related->input"}

    def test_the_relationship_type_is_named(self, run):
        rows, _ = run(input_data=["TP53"])
        types = {r["relationship_type"] for r in rows}

        assert types == {"interacts_with", "in_pathway"}
        assert all(r["relationship_description"] for r in rows)

    def test_both_endpoints_are_named_not_only_the_related_one(self, run):
        rows, _ = run(input_data=["TP53"])
        row = next(r for r in rows if r["related_primary_name"] == "P04637")

        assert row["entity_1_primary_name"] == "TP53"
        assert row["entity_2_primary_name"] == "P04637"


class TestScope:
    def test_input_to_any_reaches_outside_the_input_list(self, run):
        rows, _ = run(input_data=["TP53"], relationship_scope="input_to_any")
        assert any(r["related_primary_name"] == "P04637" for r in rows)

    def test_between_inputs_keeps_only_links_inside_the_list(self, run):
        """
        TP53's neighbours are a protein and a pathway. Asking only about
        TP53 in that scope leaves nothing — which is an answer, not an
        error.
        """
        rows, _ = run(input_data=["TP53"], relationship_scope="between_inputs")
        assert [r["observation"] for r in rows] == ["no relationships in scope"]

    def test_between_inputs_finds_a_link_when_both_ends_are_given(self, run):
        rows, _ = run(
            input_data=["TP53", "P04637"], relationship_scope="between_inputs"
        )
        real = [r for r in rows if r["relationship_id"] is not None]

        assert real
        assert {r["input_original"] for r in real} <= {"TP53", "P04637"}

    def test_an_unknown_scope_is_rejected(self, run):
        with pytest.raises(ValueError, match="relationship_scope"):
            run(input_data=["TP53"], relationship_scope="sideways")


class TestDeduplication:
    def test_between_inputs_collapses_the_two_anchors_by_default(self, run):
        """
        A relationship with an input at both ends is reached twice, once
        from each side. In `between_inputs` that is always the case, so
        collapsing is the default.
        """
        both, _ = run(input_data=["TP53", "P04637"], relationship_scope="between_inputs")
        kept, _ = run(
            input_data=["TP53", "P04637"],
            relationship_scope="between_inputs",
            deduplicate_pairs=False,
        )

        real_both = [r for r in both if r["relationship_id"] is not None]
        real_kept = [r for r in kept if r["relationship_id"] is not None]
        assert len(real_both) < len(real_kept)

    def test_input_to_any_keeps_both_anchors_by_default(self, run):
        rows, _ = run(input_data=["TP53"], relationship_scope="input_to_any")
        assert len(rows) == 3


class TestFilters:
    def test_output_entity_groups_restricts_the_far_side(self, run):
        rows, _ = run(input_data=["TP53"], output_entity_groups=["Proteins"])
        real = [r for r in rows if r["relationship_id"] is not None]

        assert real
        assert {r["related_group_name"] for r in real} == {"Proteins"}

    def test_input_entity_groups_restricts_resolution(self, run):
        rows, _ = run(input_data=["TP53"], input_entity_groups=["Diseases"])
        assert [r["observation"] for r in rows] == ["not found"]

    def test_relationship_types_restricts_by_code(self, run):
        rows, _ = run(input_data=["TP53"], relationship_types=["in_pathway"])
        real = [r for r in rows if r["relationship_id"] is not None]

        assert real
        assert {r["relationship_type"] for r in real} == {"in_pathway"}


class TestWhatCameBackEmpty:
    def test_an_unresolved_input_is_flagged(self, run):
        rows, _ = run(input_data=["TP53", "ZZZ_NOT_A_THING"])
        missing = [r for r in rows if r["input_original"] == "ZZZ_NOT_A_THING"]

        assert [r["observation"] for r in missing] == ["not found"]

    def test_resolved_with_nothing_in_scope_is_distinguished(self, run):
        """
        The gap this migration closes. The relational version emitted
        rows only for unresolved inputs, so an entity with no links in
        the requested scope vanished — indistinguishable from one never
        asked about.
        """
        rows, _ = run(input_data=["TP53"], output_entity_groups=["Diseases"])

        assert [r["observation"] for r in rows] == ["no relationships in scope"]
        assert rows[0]["input_entity_id"] == 1
        assert rows[0]["input_group_name"] == "Genes"

    def test_emit_not_found_rows_false_drops_both_kinds(self, run):
        rows, _ = run(
            input_data=["TP53", "ZZZ_NOT_A_THING"],
            output_entity_groups=["Diseases"],
            emit_not_found_rows=False,
        )
        assert rows == []


class TestContract:
    def test_columns_match_what_is_declared(self, run):
        _, result = run(input_data=["TP53"])
        from biofilter.modules.report.reports.report_expand_entity_relationship import (
            ExpandEntityRelationshipReport,
        )

        assert result.columns == list(ExpandEntityRelationshipReport.COLUMNS)

    def test_provenance_names_the_bundle(self, run):
        _, result = run(input_data=["TP53"])
        assert result.provenance["bundle_id"] == "fixturebundle0001"

    def test_empty_input_is_rejected(self, run):
        with pytest.raises(ValueError, match="at least one"):
            run(input_data=[])
