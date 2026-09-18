"""
pair_genes: which of these genes are related, and by what.

Runs against `pairing_bundle`. PATHWAY reaches TP53 and BRCA1 — two
genes, so one pair. DISEASE reaches TP53, BRCA1 and DGENE.

The expansion is the part with rules. A worked example, the one from
ADR-005 D7: given TP53 → 111, 222, 333 and BRCA1 → 444, 555, the pair
TP53-BRCA1 implies six item pairs. Everything interesting happens when
that example stops being so clean.
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
            result = manager.run("pair_genes", **params)
            return result.table.to_pylist(), result

        yield _run


def _items(rows):
    return sorted((r["item_1"], r["item_2"]) for r in rows)


class TestGenePairsOnTheirOwn:
    def test_two_genes_sharing_a_group_are_a_pair(self, run):
        rows, _ = run(input_data=["TP53", "BRCA1"])

        assert len(rows) == 1
        assert {rows[0]["gene_1_symbol"], rows[0]["gene_2_symbol"]} == {
            "TP53",
            "BRCA1",
        }

    def test_the_support_counts_the_groups_behind_the_pair(self, run):
        rows, _ = run(
            input_data=["TP53", "BRCA1"],
            group_types=["Pathways", "Diseases"],
            max_group_size=10,
        )

        assert rows[0]["group_support_count"] == 2

    def test_a_gene_alone_pairs_with_nothing_under_both(self, run):
        rows, _ = run(input_data=["TP53"])

        assert rows == []

    def test_either_admits_a_partner_never_named(self, run):
        rows, _ = run(input_data=["TP53"], membership="either")

        assert rows
        assert rows[0]["gene_1_from_input"] is True
        assert rows[0]["gene_2_from_input"] is False

    def test_an_empty_input_is_refused(self, run):
        with pytest.raises(ValueError, match="at least one gene"):
            run(input_data=[])


class TestItDoesNotTouchVariants:
    """
    ADR-005 D1 and §1.1: placing a variant on a gene is the assumption
    this report exists to avoid making. A variant is not a gene, so it
    resolves to nothing rather than being placed.
    """

    def test_a_variant_is_not_resolved_into_a_gene(self, run):
        rows, _ = run(input_data=["17:150", "17:5100"], membership="either")

        assert rows == []

    def test_it_declares_no_variant_table(self):
        from biofilter.modules.report.reports.report_pair_genes import (
            PairGenesReport,
        )

        assert not [t for t in PairGenesReport.requires if t.startswith("variant_")]
        assert not [t for t in PairGenesReport.optional if t.startswith("variant_")]


class TestTheWorkedExample:
    """TP53 → 111, 222, 333 and BRCA1 → 444, 555 is six pairs."""

    MAPPING = {"TP53": ["111", "222", "333"], "BRCA1": ["444", "555"]}

    def test_three_items_by_two_is_six_pairs(self, run):
        rows, _ = run(input_data=["TP53", "BRCA1"], mapping=self.MAPPING)

        assert _items(rows) == [
            ("111", "444"),
            ("111", "555"),
            ("222", "444"),
            ("222", "555"),
            ("333", "444"),
            ("333", "555"),
        ]

    def test_the_item_pairs_are_the_primary_table(self, run):
        """
        `write()` exports the primary one, so the grain the caller asked
        for has to be it (ADR-005 D6).
        """
        _, result = run(input_data=["TP53", "BRCA1"], mapping=self.MAPPING)

        assert result.num_rows == 6
        assert set(result.tables) == {"result", "gene_pairs"}
        assert result.extra_tables["gene_pairs"].num_rows == 1

    def test_without_a_mapping_the_gene_pairs_are_the_only_table(self, run):
        _, result = run(input_data=["TP53", "BRCA1"])

        assert set(result.tables) == {"result"}

    def test_the_pair_carries_the_genes_it_came_from(self, run):
        rows, _ = run(input_data=["TP53", "BRCA1"], mapping=self.MAPPING)

        assert {r["gene_1_symbol"] for r in rows} | {
            r["gene_2_symbol"] for r in rows
        } == {"TP53", "BRCA1"}


class TestTheThreeRulesTheExampleDoesNotShow:
    """ADR-005 D5. Each of these inflates a count silently when missed."""

    def test_an_item_on_both_genes_does_not_pair_with_itself(self, run):
        rows, _ = run(
            input_data=["TP53", "BRCA1"],
            mapping={"TP53": ["X", "111"], "BRCA1": ["X", "444"]},
        )

        assert not [r for r in rows if r["item_1"] == r["item_2"]]
        assert _items(rows) == [("111", "444"), ("111", "X"), ("444", "X")]

    def test_the_same_item_pair_through_two_gene_pairs_is_one_pair(self, run):
        """
        Deduplication is global, not per gene pair. On one real run this
        was 4.3% of the answer.
        """
        plain, _ = run(
            input_data=["TP53", "BRCA1", "DGENE1"],
            group_types=["Pathways", "Diseases"],
            max_group_size=10,
        )
        # Without three gene pairs the rule is vacuous, so assert them.
        assert len(plain) == 3

        rows, _ = run(
            input_data=["TP53", "BRCA1", "DGENE1"],
            group_types=["Pathways", "Diseases"],
            max_group_size=10,
            mapping={"TP53": ["A"], "BRCA1": ["B"], "DGENE1": ["A"]},
        )

        # A-B arrives through TP53-BRCA1 and again through BRCA1-DGENE1;
        # TP53-DGENE1 would give A-A, which is a self-pair.
        assert _items(rows) == [("A", "B")]

    def test_a_pair_is_unordered(self, run):
        rows, _ = run(
            input_data=["TP53", "BRCA1"],
            mapping={"TP53": ["zzz"], "BRCA1": ["aaa"]},
        )

        assert _items(rows) == [("aaa", "zzz")]

    def test_a_gene_named_but_carrying_nothing_contributes_nothing(self, run):
        """
        The rule is items on both sides, not genes in the input. A gene
        can be named and carry nothing.
        """
        rows, _ = run(
            input_data=["TP53", "BRCA1", "DGENE1"],
            group_types=["Pathways", "Diseases"],
            max_group_size=10,
            mapping={"TP53": ["A"], "BRCA1": ["B"]},
        )

        assert _items(rows) == [("A", "B")]


class TestTheOpaquePayload:
    """
    ADR-005 D3. The item is a label carried from input to output, and
    the report never reads it.
    """

    def test_an_item_can_be_anything(self, run):
        rows, _ = run(
            input_data=["TP53", "BRCA1"],
            mapping={"TP53": ["exposure:smoking"], "BRCA1": ["probe_0042"]},
        )

        assert _items(rows) == [("exposure:smoking", "probe_0042")]

    def test_a_build_37_position_passes_through_untouched(self, run):
        """
        Biofilter is build 38 and does no mapping here, so a build-37
        payload is neither interpreted nor corrupted.
        """
        rows, _ = run(
            input_data=["TP53", "BRCA1"],
            mapping={"TP53": ["17:7579472"], "BRCA1": ["17:41244936"]},
        )

        assert _items(rows) == [("17:41244936", "17:7579472")]

    def test_two_spellings_of_one_thing_are_two_things(self, run):
        """
        The limit of what deduplication can promise under an opaque
        payload, stated as a test so it is not discovered as a surprise.
        """
        rows, _ = run(
            input_data=["TP53", "BRCA1"],
            mapping={"TP53": ["22:100:A:G", "chr22:100:A:G"], "BRCA1": ["x"]},
        )

        assert len(rows) == 2


class TestWhenExpansionCannotWork:
    def test_either_is_refused_with_a_mapping(self, run):
        """
        The partner gene came from the bundle, not from the caller's
        list, so there is nothing on that side to pair.
        """
        with pytest.raises(ValueError, match="cannot be combined with a mapping"):
            run(
                input_data=["TP53"],
                membership="either",
                mapping={"TP53": ["A"]},
            )

    def test_an_empty_mapping_is_refused(self, run):
        with pytest.raises(ValueError, match="mapping is empty"):
            run(input_data=["TP53", "BRCA1"], mapping={})

    def test_both_a_file_and_an_inline_list_is_refused(self, run, tmp_path):
        path = tmp_path / "m.tsv"
        path.write_text("TP53\tA\n")

        with pytest.raises(ValueError, match="not both"):
            run(
                input_data=["TP53"],
                mapping={"TP53": ["A"]},
                mapping_file=str(path),
            )

    def test_a_missing_file_says_which(self, run):
        with pytest.raises(FileNotFoundError, match="nope.tsv"):
            run(input_data=["TP53"], mapping_file="/tmp/nope.tsv")

    def test_a_malformed_inline_entry_is_refused(self, run):
        with pytest.raises(ValueError, match=r"must be \(gene, item\)"):
            run(input_data=["TP53"], mapping=["just-a-gene"])


class TestTheMappingFile:
    def test_two_columns_tab_separated(self, run, tmp_path):
        path = tmp_path / "m.tsv"
        path.write_text("TP53\t111\nTP53\t222\nBRCA1\t444\n")
        rows, _ = run(input_data=["TP53", "BRCA1"], mapping_file=str(path))

        assert _items(rows) == [("111", "444"), ("222", "444")]

    def test_comma_separated_works_too(self, run, tmp_path):
        path = tmp_path / "m.csv"
        path.write_text("TP53,111\nBRCA1,444\n")
        rows, _ = run(input_data=["TP53", "BRCA1"], mapping_file=str(path))

        assert _items(rows) == [("111", "444")]

    def test_a_header_line_is_skipped(self, run, tmp_path):
        path = tmp_path / "m.csv"
        path.write_text("gene,item\nTP53,111\nBRCA1,444\n")
        rows, _ = run(input_data=["TP53", "BRCA1"], mapping_file=str(path))

        assert _items(rows) == [("111", "444")]

    def test_a_pair_sequence_inline(self, run):
        rows, _ = run(
            input_data=["TP53", "BRCA1"],
            mapping=[("TP53", "111"), ("BRCA1", "444")],
        )

        assert _items(rows) == [("111", "444")]


class TestTheGroupSource:
    """ADR-005 D8: the bundle carries it, so the report returns it."""

    def test_the_source_is_named_not_derived_from_an_accession(self, run):
        rows, _ = run(input_data=["TP53", "BRCA1"])

        assert rows[0]["group_support_sources"]
        assert all(isinstance(s, str) for s in rows[0]["group_support_sources"])

    def test_min_group_sources_is_a_stronger_claim_than_min_support(self, run):
        """Two curations agreeing is not one curation saying it twice."""
        rows, _ = run(
            input_data=["TP53", "BRCA1"],
            group_types=["Pathways", "Diseases"],
            max_group_size=10,
            min_group_sources=3,
        )

        assert rows == []

    def test_it_counts_distinct_sources(self, run):
        rows, _ = run(
            input_data=["TP53", "BRCA1"],
            group_types=["Pathways", "Diseases"],
            max_group_size=10,
        )

        assert rows[0]["group_support_source_count"] == len(
            set(rows[0]["group_support_sources"])
        )

    def test_below_one_is_refused(self, run):
        with pytest.raises(ValueError, match="min_group_sources must be at least 1"):
            run(input_data=["TP53"], min_group_sources=0)


class TestTheContract:
    def test_declared_columns_are_the_gene_pair_ones(self, run):
        from biofilter.modules.report.reports.report_pair_genes import (
            PairGenesReport,
        )

        _, result = run(input_data=["TP53", "BRCA1"])

        assert list(result.columns) == list(PairGenesReport.available_columns())

    def test_the_expansion_has_its_own_declared_columns(self, run):
        from biofilter.modules.report.reports.report_pair_genes import (
            PairGenesReport,
        )

        _, result = run(
            input_data=["TP53", "BRCA1"], mapping={"TP53": ["A"], "BRCA1": ["B"]}
        )

        assert list(result.columns) == list(PairGenesReport.ITEM_COLUMNS)

    def test_provenance_says_whether_it_expanded(self, run):
        _, plain = run(input_data=["TP53", "BRCA1"])
        _, expanded = run(
            input_data=["TP53", "BRCA1"], mapping={"TP53": ["A"], "BRCA1": ["B"]}
        )

        assert plain.provenance["pairing"]["expanded"] is False
        assert expanded.provenance["pairing"]["expanded"] is True

    def test_the_whole_result_round_trips(self, run, tmp_path):
        from biofilter.modules.report.result import ReportResult

        _, result = run(
            input_data=["TP53", "BRCA1"], mapping={"TP53": ["A"], "BRCA1": ["B"]}
        )
        result.save(tmp_path / "pairs")
        back = ReportResult.load(tmp_path / "pairs")

        assert set(back.tables) == {"result", "gene_pairs"}
        assert back.table.equals(result.table)

    def test_an_empty_result_still_says_what_the_filter_removed(self, run):
        _, result = run(
            input_data=["TP53", "BRCA1"], group_types=["Diseases"], max_group_size=2
        )

        assert result.provenance["group_filter"]["groups_excluded_by_size"] >= 1
