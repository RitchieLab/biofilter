"""
Case never matters, and that is a promise rather than an accident.

Every report accepts an option value, an input value or a mapping key in
any case. The behaviour is consistent today; this file is what keeps it
consistent, since each report resolves its own parameters and one of them
could quietly start caring.

Cases that have bitten before, and are therefore checked by name: allele
letters in a variant key, the optional `chr` prefix, and an rsID's `rs`.
"""

from __future__ import annotations

import pytest

from biofilter.modules.report import Bundle, ReportManager


#: A two-line cohort, written once, for the one report that needs a file.
COHORT = ""


@pytest.fixture(autouse=True)
def _cohort_file(tmp_path):
    global COHORT
    path = tmp_path / "cohort.txt"
    path.write_text("17:150:A:G\n17:5100:A:G\n")
    COHORT = str(path)
    return COHORT


@pytest.fixture
def run(pairing_bundle):
    with Bundle.open(pairing_bundle) as bundle:
        manager = ReportManager(bundle=bundle)

        def _run(report, **params):
            return manager.run(report, **params).table.to_pylist()

        yield _run


def _answer(rows):
    """
    The rows without the columns that echo what the caller typed.

    A report hands back the text it was given — `input_value`, `input_1`
    and friends — so comparing whole rows would compare the question
    rather than the answer.
    """
    return [
        {k: v for k, v in row.items() if not k.startswith("input")}
        for row in rows
    ]


def _same(run, report, param, values, **fixed):
    """Every spelling of `param` gives the same answer."""
    answers = [
        _answer(run(report, **{param: value}, **fixed)) for value in values
    ]
    first = answers[0]
    for value, answer in zip(values[1:], answers[1:]):
        assert answer == first, f"{report}.{param}={value!r} differed"
    return first


class TestOptionValues:
    def test_group_types(self, run):
        rows = _same(
            run, "pair_genes", "group_types",
            [["Pathways"], ["pathways"], ["PATHWAYS"]],
            input_data=["TP53", "BRCA1"],
        )
        assert rows

    def test_gene_identifier(self, run):
        rows = _same(
            run, "pair_genes", "gene_identifier",
            ["entity_id", "ENTITY_ID", "Entity_Id"],
            input_data=["1", "2"],
        )
        assert rows

    def test_gene_identifier_naming_a_code_system(self, run):
        rows = _same(
            run, "pair_genes", "gene_identifier",
            ["ensembl", "ENSEMBL", "Ensembl"],
            input_data=["ENSG00000141510", "ENSG00000012048"],
        )
        assert rows

    def test_match_mode(self, run):
        rows = _same(
            run, "resolve_entity", "match_mode",
            ["exact", "EXACT", "Exact"],
            input_data=["TP53"],
        )
        assert rows

    def test_mapping_mode(self, run):
        rows = _same(
            run, "expand_gene_to_variant", "mapping",
            ["position", "POSITION", "Position"],
            input_data=["TP53"],
        )
        assert rows

    def test_membership(self, run):
        rows = _same(
            run, "pair_genes", "membership",
            ["both", "BOTH", "Both"],
            input_data=["TP53", "BRCA1"],
        )
        assert rows

    def test_output_grain(self, run):
        rows = _same(
            run, "aggregate_cohort_variants", "output_grain",
            ["variants", "VARIANTS", "Variants"],
            cohort_file=COHORT,
        )
        assert rows

    def test_sections(self, run):
        rows = _same(
            run, "platform_data_statistics", "sections",
            [["bundle"], ["BUNDLE"], ["Bundle"]],
        )
        assert rows


class TestFilterValues:
    def test_impact_filter(self, run):
        rows = _same(
            run, "expand_gene_to_variant", "impact_filter",
            [["MODERATE"], ["moderate"], ["Moderate"]],
            input_data=["TP53"], most_severe_only=False,
        )
        assert rows

    def test_a_filter_that_matches_nothing_still_agrees(self, run):
        """The insensitivity must not come from the filter being a no-op."""
        matched = run(
            "expand_gene_to_variant", input_data=["TP53"],
            impact_filter=["MODERATE"], most_severe_only=False,
        )
        unmatched = run(
            "expand_gene_to_variant", input_data=["TP53"],
            impact_filter=["NOT_AN_IMPACT"], most_severe_only=False,
        )

        assert [r for r in matched if r["status"] == "ok"]
        assert not [r for r in unmatched if r["status"] == "ok"]


class TestInputValues:
    def test_a_gene_name(self, run):
        rows = _same(
            run, "pair_genes", "input_data",
            [["TP53", "BRCA1"], ["tp53", "brca1"], ["Tp53", "BrCa1"]],
            group_types=["Pathways"],
        )
        assert rows

    def test_the_alleles_of_a_variant_key(self, run):
        """`17:150:A:G` and `17:150:a:g` are the same variant."""
        rows = _same(
            run, "annotate_variant", "input_data",
            [["17:150:A:G"], ["17:150:a:g"], ["17:150:A:g"]],
        )
        assert [r for r in rows if r["variant_key"]]

    def test_the_chr_prefix_is_optional_and_uncased(self, run):
        rows = _same(
            run, "annotate_variant", "input_data",
            [["17:150"], ["chr17:150"], ["CHR17:150"], ["Chr17:150"]],
        )
        assert [r for r in rows if r["variant_key"]]

    def test_an_rsid(self, run):
        rows = _same(
            run, "annotate_variant", "input_data",
            [["rs101"], ["RS101"], ["Rs101"]],
        )
        assert [r for r in rows if r["variant_key"]]

    def test_a_group_hint(self, run):
        """
        Hints belong to `expand_entity_neighborhood`; `resolve_entity`
        never claimed them, and `gene:TP53` is simply a name it cannot
        find.
        """
        rows = _same(
            run, "expand_entity_neighborhood", "input_data",
            [["gene:TP53"], ["GENE:TP53"], ["Gene:tp53"]],
        )
        assert rows
        assert [r for r in rows if r.get("entity_id")]


class TestTheMapping:
    def test_the_gene_column_of_a_pair_genes_mapping(self, run):
        rows = _same(
            run, "pair_genes", "mapping",
            [
                {"TP53": ["A"], "BRCA1": ["B"]},
                {"tp53": ["A"], "brca1": ["B"]},
                {"Tp53": ["A"], "BrCa1": ["B"]},
            ],
            input_data=["TP53", "BRCA1"], group_types=["Pathways"],
        )
        assert rows


class TestWhatCaseDoesAffect:
    def test_the_answer_carries_the_bundle_spelling_not_yours(self, run):
        """
        Another report will recognise the bundle's spelling. Echoing the
        caller's would make a result that does not round-trip.
        """
        rows = run("pair_genes", input_data=["tp53", "brca1"],
                   group_types=["Pathways"])

        assert {rows[0]["gene_1_symbol"], rows[0]["gene_2_symbol"]} == {
            "TP53",
            "BRCA1",
        }
