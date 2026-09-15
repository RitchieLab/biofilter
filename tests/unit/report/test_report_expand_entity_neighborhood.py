"""
expand_entity_neighborhood: what sits one hop from each entity.

The report takes a heterogeneous list, so most of what is tested here is
how an input is *read* before anything is looked up — which is where the
relational version went wrong.
"""

from __future__ import annotations

import pytest

from biofilter.modules.report import Bundle, ReportManager


@pytest.fixture
def run(fixture_bundle):
    with Bundle.open(fixture_bundle) as bundle:
        manager = ReportManager(bundle=bundle)

        def _run(**params):
            result = manager.run("expand_entity_neighborhood", **params)
            return {r["input_value"]: r for r in result.table.to_pylist()}, result

        yield _run


class TestReadingTheInput:
    def test_a_known_prefix_is_a_type_hint(self, run):
        rows, _ = run(input_data=["gene:TP53"])
        row = rows["gene:TP53"]

        assert row["input_type_hint"] == "gene"
        assert row["entity_id"] == 1
        assert row["entity_group"] == "Genes"

    def test_a_hint_restricts_the_search(self, run):
        """TP53 is a gene; asking for it as a disease finds nothing."""
        rows, _ = run(input_data=["disease:TP53"])
        row = rows["disease:TP53"]

        assert row["status"] == "not_found"
        assert "diseases" in row["note"]

    def test_an_identifier_prefix_is_not_a_hint(self, run):
        """
        The defect this report was rewritten for. `GO:0006915` is a GO
        identifier, not the hint `go` plus the term `0006915` — and the
        relational version read it the second way, resolving a GO id to a
        **disease** without failing.
        """
        rows, _ = run(input_data=["GO:0000002"])
        row = rows["GO:0000002"]

        assert row["input_type_hint"] is None
        assert row["entity_group"] == "Gene Ontology"
        assert row["entity_id"] == 41

    @pytest.mark.parametrize("value", ["MONDO:0001", "HGNC:11998"])
    def test_every_curie_survives_intact(self, run, value):
        rows, _ = run(input_data=[value])
        assert rows[value]["input_type_hint"] is None
        assert rows[value]["status"] == "ok"

    def test_a_bare_name_needs_no_hint(self, run):
        rows, _ = run(input_data=["TP53"])
        assert rows["TP53"]["entity_id"] == 1
        assert rows["TP53"]["input_type_hint"] is None

    def test_hints_come_from_the_bundle_not_a_hardcoded_map(self, run):
        """
        The relational map named `GO Terms`, a group that does not exist
        in any 4.3.0 bundle, so `go:` had resolved nothing even before the
        parsing defect. Group names are read from the bundle now.
        """
        rows, _ = run(input_data=["gene ontology:GO:0000002"])
        assert rows["gene ontology:GO:0000002"]["entity_group"] == "Gene Ontology"

    def test_an_unknown_name_is_kept(self, run):
        rows, _ = run(input_data=["TP53", "ZZZ_NOT_A_THING"])
        assert rows["ZZZ_NOT_A_THING"]["status"] == "not_found"
        assert "not resolved" in rows["ZZZ_NOT_A_THING"]["note"]

    def test_emit_not_found_rows_false_drops_it(self, run):
        rows, _ = run(input_data=["TP53", "ZZZ_NOT_A_THING"], emit_not_found_rows=False)
        assert set(rows) == {"TP53"}


class TestNeighbourhood:
    def test_degree_counts_both_directions(self, run):
        """
        TP53 is on the left of two relationships and the right of a third.
        All three are hops.
        """
        rows, _ = run(input_data=["TP53"])
        assert rows["TP53"]["degree_total"] == 3

    def test_neighbours_are_grouped_by_kind_commonest_first(self, run):
        rows, _ = run(input_data=["TP53"])
        by_type = rows["TP53"]["neighbors_by_type"]

        assert [e["group_name"] for e in by_type] == ["Pathways", "Proteins"]
        assert [e["count"] for e in by_type] == [2, 1]

    def test_neighbours_are_named_not_numbered(self, run):
        rows, _ = run(input_data=["TP53"])
        names = {e["group_name"]: list(e["names"]) for e in rows["TP53"]["neighbors_by_type"]}

        assert names["Proteins"] == ["P04637"]
        assert names["Pathways"] == ["R-HSA-0001"]

    def test_the_schema_does_not_depend_on_the_bundle(self, run):
        """
        One nested column, not one column per entity group. The relational
        version added a column per group present in the bundle — 29
        columns against the real one, 14 of them unknowable in advance.
        """
        _, result = run(input_data=["TP53"])
        from biofilter.modules.report.reports.report_expand_entity_neighborhood import (
            ExpandEntityNeighborhoodReport,
        )

        assert result.columns == list(ExpandEntityNeighborhoodReport.COLUMNS)

    def test_an_entity_with_nothing_linked_says_so(self, run):
        """Resolved and isolated is not the same as not found."""
        rows, _ = run(input_data=["GO:0000002"])
        row = rows["GO:0000002"]

        assert row["status"] == "ok"
        assert row["degree_total"] == 0
        assert row["neighbors_by_type"] == []
        assert "nothing is linked" in row["note"]

    def test_neighbours_per_type_can_be_capped(self, run):
        rows, _ = run(input_data=["TP53"], neighbors_top_n_per_type=1)
        for entry in rows["TP53"]["neighbors_by_type"]:
            assert len(entry["names"]) <= 1


class TestAliasesAndModes:
    def test_alias_count_and_sample(self, run):
        rows, _ = run(input_data=["TP53"])
        row = rows["TP53"]

        assert row["alias_count"] == 5
        assert "TP53" in row["aliases_top"]

    def test_aliases_can_be_capped(self, run):
        rows, _ = run(input_data=["TP53"], aliases_top_n=2)
        assert len(rows["TP53"]["aliases_top"]) == 2
        assert rows["TP53"]["alias_count"] == 5  # the count is not capped

    @pytest.mark.parametrize("mode", ["exact", "like", "fuzzy"])
    def test_every_match_mode_resolves_a_known_name(self, run, mode):
        rows, _ = run(input_data=["TP53"], match_mode=mode)
        assert rows["TP53"]["entity_id"] == 1
        assert rows["TP53"]["match_mode"] == mode

    def test_fuzzy_scores_and_the_others_do_not(self, run):
        exact, _ = run(input_data=["TP53"], match_mode="exact")
        fuzzy, _ = run(input_data=["TP53"], match_mode="fuzzy")

        assert exact["TP53"]["similarity_score"] is None
        assert fuzzy["TP53"]["similarity_score"] == 100

    def test_an_unknown_match_mode_is_rejected(self, run):
        with pytest.raises(ValueError, match="match_mode"):
            run(input_data=["TP53"], match_mode="approximately")


class TestContract:
    def test_provenance_names_the_bundle(self, run):
        _, result = run(input_data=["TP53"])
        assert result.provenance["bundle_id"] == "fixturebundle0001"

    def test_csv_export_renders_the_nested_column(self, run, tmp_path):
        _, result = run(input_data=["TP53"])
        written = result.write(tmp_path / "neighbourhood.csv")
        text = written[0].read_text()

        assert "Pathways" in text
        assert written[1].name.endswith(".provenance.json")
