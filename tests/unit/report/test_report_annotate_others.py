"""
The other annotation reports: disease, GO, pathway, protein.

They share a shape with `annotate_gene` — resolve an input to an
entity, hang facts off it — so what is tested here is what each does
*differently*, plus the parts the shared scaffolding has to get right for
all of them.
"""

from __future__ import annotations

import pytest

from biofilter.modules.report import Bundle, ReportManager


@pytest.fixture
def run(fixture_bundle):
    with Bundle.open(fixture_bundle) as bundle:
        manager = ReportManager(bundle=bundle)

        def _run(report, **params):
            result = manager.run(report, **params)
            return {r["input_value"]: r for r in result.table.to_pylist()}, result

        yield _run


class TestDisease:
    REPORT = "annotate_disease"

    def test_resolves_and_annotates(self, run):
        rows, _ = run(self.REPORT, input_data=["MONDO:0001"])
        row = rows["MONDO:0001"]

        assert row["entity_id"] == 30
        assert row["disease_label"] == "breast cancer"
        assert row["disease_description"] == "a tumour of the breast"
        assert row["disease_groups"] == ["neoplasm"]
        assert row["status"] == "ok"

    def test_provenance_names_the_source_system(self, run):
        rows, _ = run(self.REPORT, input_data=["MONDO:0001"])
        row = rows["MONDO:0001"]

        assert row["disease_source_system"] == "MONDO"
        assert row["disease_data_source"] == "mondo"
        assert row["disease_etl_package_id"] == 71

    def test_clingen_counts_genes_not_assertions(self, run):
        """
        The fixture gives this disease two ClinGen assertions naming the
        same gene, plus a MONDO link to a protein. One gene asserted
        through two lines of evidence is one gene.
        """
        rows, _ = run(self.REPORT, input_data=["MONDO:0001"])
        row = rows["MONDO:0001"]

        assert row["clingen_gene_count"] == 1
        assert row["clingen_relationship_count"] == 2
        # Everything, not only ClinGen's share.
        assert row["total_entity_relationships"] == 3

    def test_clingen_summary_can_be_switched_off(self, run):
        rows, _ = run(
            self.REPORT, input_data=["MONDO:0001"], include_clingen_summary=False
        )
        assert rows["MONDO:0001"]["clingen_gene_count"] is None

    def test_xrefs_are_grouped_by_the_source_that_issued_them(self, run):
        rows, _ = run(self.REPORT, input_data=["MONDO:0001"])
        by_source = {x["source"]: x["ids"] for x in rows["MONDO:0001"]["xref_ids_by_source"]}

        assert by_source == {"MONDO": ["MONDO:0001"]}

    def test_a_disease_without_groups_still_resolves(self, run):
        rows, _ = run(self.REPORT, input_data=["MONDO:0002"])
        row = rows["MONDO:0002"]

        assert row["status"] == "ok"
        assert row["disease_groups"] == []
        assert row["clingen_gene_count"] == 0

    def test_unresolved_input_is_kept(self, run):
        rows, _ = run(self.REPORT, input_data=["MONDO:0001", "NOPE"])
        assert rows["NOPE"]["status"] == "not_found"


class TestGeneOntology:
    REPORT = "annotate_go"

    def test_a_child_term_reports_its_parent(self, run):
        rows, _ = run(self.REPORT, input_data=["GO:0000002"])
        row = rows["GO:0000002"]

        assert row["go_name"] == "apoptotic process"
        assert row["go_namespace"] == "biological_process"
        assert row["go_parent_count"] == 1
        assert row["go_child_count"] == 0
        assert row["go_parent_relation_types"] == ["is_a"]
        assert row["go_parent_ids"] == ["GO:0000001"]

    def test_a_parent_term_reports_its_child(self, run):
        """
        The same edge, seen from the other end. A term is a parent in one
        row and a child in another, and both sides have to be counted.
        """
        rows, _ = run(self.REPORT, input_data=["GO:0000001"])
        row = rows["GO:0000001"]

        assert row["go_child_count"] == 1
        assert row["go_parent_count"] == 0
        assert row["go_child_ids"] == ["GO:0000002"]

    def test_relation_details_can_be_switched_off(self, run):
        rows, _ = run(
            self.REPORT, input_data=["GO:0000002"], include_go_relation_details=False
        )
        row = rows["GO:0000002"]

        # The counts stay; only the id lists go.
        assert row["go_parent_count"] == 1
        assert row["go_parent_ids"] is None

    def test_a_synonym_resolves(self, run):
        rows, _ = run(self.REPORT, input_data=["apoptosis"])
        assert rows["apoptosis"]["go_id"] == "GO:0000002"


class TestPathway:
    REPORT = "annotate_pathway"

    def test_resolves_and_annotates(self, run):
        rows, _ = run(self.REPORT, input_data=["R-HSA-0001"])
        row = rows["R-HSA-0001"]

        assert row["pathway_id"] == "R-HSA-0001"
        assert row["pathway_description"] == "apoptosis signalling"
        assert row["pathway_source_system"] == "Reactome"
        assert row["status"] == "ok"

    def test_counts_relationships_both_ways(self, run):
        """The pathway is entity_2 of one relationship and entity_1 of another."""
        rows, _ = run(self.REPORT, input_data=["R-HSA-0001"])
        assert rows["R-HSA-0001"]["total_entity_relationships"] == 2


class TestProtein:
    REPORT = "annotate_protein"

    def test_canonical_input_annotates_itself(self, run):
        rows, _ = run(self.REPORT, input_data=["P04637"])
        row = rows["P04637"]

        assert row["entity_id"] == 10
        assert row["canonical_entity_id"] == 10
        assert row["input_is_isoform"] is False
        assert row["function"] == "tumour suppressor"
        assert row["location"] == "nucleus"
        assert row["isoform_count"] == 1
        assert row["status"] == "ok"

    def test_an_isoform_input_is_annotated_as_the_canonical_protein(self, run):
        """
        The reason this report has a resolution step of its own. The
        isoform entity has no relationships and no Pfam links; the protein
        does, and that is what the user is asking about.
        """
        rows, _ = run(self.REPORT, input_data=["P04637-2"])
        row = rows["P04637-2"]

        assert row["entity_id"] == 11           # what the input matched
        assert row["canonical_entity_id"] == 10  # what the annotation describes
        assert row["input_is_isoform"] is True
        assert row["input_isoform_accession"] == "P04637-2"
        assert row["protein_id"] == "P04637"
        assert row["pfam_total_count"] == 2
        assert row["total_entity_relationships"] == 2
        assert "isoform" in row["note"]

    def test_pfam_domains_are_grouped_by_type(self, run):
        rows, _ = run(self.REPORT, input_data=["P04637"])
        row = rows["P04637"]

        assert row["pfam_total_count"] == 2
        by_type = {e["type"]: e["count"] for e in row["pfam_count_by_type"]}
        assert by_type == {"Domain": 1, "Family": 1}

        ids = {e["type"]: e["ids"] for e in row["pfam_ids_by_type"]}
        assert ids == {"Domain": ["PF00870"], "Family": ["PF08563"]}

    def test_pfam_summary_can_be_switched_off(self, run):
        rows, _ = run(self.REPORT, input_data=["P04637"], include_pfam_summary=False)
        row = rows["P04637"]

        assert row["pfam_total_count"] == 0
        assert row["pfam_ids_by_type"] is None


class TestSharedBehaviour:
    """What the scaffolding has to get right for every one of them."""

    REPORTS = [
        ("annotate_disease", "MONDO:0001"),
        ("annotate_go", "GO:0000002"),
        ("annotate_pathway", "R-HSA-0001"),
        ("annotate_protein", "P04637"),
    ]

    @pytest.mark.parametrize("report,value", REPORTS)
    def test_columns_match_what_is_declared(self, run, report, value):
        _, result = run(report, input_data=[value])
        manager_class = ReportManager().get_class(report)

        assert result.columns == list(manager_class.COLUMNS)

    @pytest.mark.parametrize("report,value", REPORTS)
    def test_provenance_names_the_bundle(self, run, report, value):
        _, result = run(report, input_data=[value])
        assert result.provenance["bundle_id"] == "fixturebundle0001"

    @pytest.mark.parametrize("report,value", REPORTS)
    def test_an_unresolved_input_is_kept_by_default(self, run, report, value):
        rows, _ = run(report, input_data=[value, "NOT_A_THING"])
        assert rows["NOT_A_THING"]["status"] == "not_found"

    @pytest.mark.parametrize("report,value", REPORTS)
    def test_emit_not_found_rows_false_drops_it(self, run, report, value):
        rows, _ = run(report, input_data=[value, "NOT_A_THING"], emit_not_found_rows=False)
        assert set(rows) == {value}

    @pytest.mark.parametrize("report,expected", [
        ("annotate_disease", {"MONDO:0001", "MONDO:0002"}),
        ("annotate_go", {"GO:0000001", "GO:0000002"}),
        ("annotate_pathway", {"R-HSA-0001"}),
    ])
    def test_all_returns_every_entity_of_the_group(self, run, report, expected):
        rows, _ = run(report, input_data="__ALL__")
        assert set(rows) == expected

    @pytest.mark.parametrize("report,value", REPORTS)
    def test_csv_export_renders_the_list_columns(self, run, report, value, tmp_path):
        _, result = run(report, input_data=[value])
        written = result.write(tmp_path / f"{report}.csv")

        assert written[1].name.endswith(".provenance.json")
        assert written[0].read_text().count("\n") >= 2
