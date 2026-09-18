"""
Which of these genes are related, and by what — and optionally, what that
implies about a list of your own.

Two stages (ADR-005):

1. **Connect.** Seed genes reach partner genes through a shared entity: a
   pathway, a disease, a protein. That entity is the *group*, and how many
   distinct groups link a pair is the pair's support.
2. **Expand**, when asked. Given a gene → item mapping, every gene pair
   becomes the item pairs it implies.

**This report performs no variant → gene mapping at all.** No overlap, no
window, none of the machinery `pair_variants` uses to go from a variant
to a gene and back. It takes genes, and for the expansion it takes the
link the caller supplied and nothing else.

That is what makes the payload opaque, and the opacity is the design.
Biofilter is build 38 throughout; a caller pairing build-37 positions
gets build-37 positions out, correctly, because nothing here reads the
coordinate. The same mechanism serves rsIDs, exposures, probe ids or
anything else — they are labels carried from input to output.

The price is stated rather than hidden: the report cannot filter by
allele frequency, resolve an rsID, or validate an item. Two spellings of
the same thing are two things. Correctness of the items is the caller's;
reports that interpret variants already exist and this is not one.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Optional, Sequence

import pyarrow as pa

from biofilter.modules.report.reports import _pairing
from biofilter.modules.report.reports._resolution import ALIAS_KEY
from biofilter.modules.report.reports.base_report import ReportBase
from biofilter.modules.report.result import ReportResult

DEFAULT_MAX_PAIRS = 1_000_000


class PairGenesReport(ReportBase):
    name = "pair_genes"
    description = (
        "Gene pairs linked by shared biology — a pathway, a disease, a protein — "
        "with the support behind each. Given a gene-to-item mapping, also returns "
        "the item pairs those gene pairs imply, whatever the items are."
    )

    requires = (
        "entities",
        "entity_aliases",
        "entity_groups",
        "entity_relationships",
        "gene_masters",
        "etl_data_sources",
    )

    optional = ()

    #: The gene-pair table, which is what the report returns when no
    #: mapping is given.
    COLUMNS = (
        "gene_1_id",
        "gene_1_symbol",
        "gene_1_from_input",
        "gene_2_id",
        "gene_2_symbol",
        "gene_2_from_input",
        "group_support_count",
        "group_support_source_count",
        "group_support_types",
        "group_support_names",
        "group_support_sources",
        "membership",
    )

    #: The item-pair table. It becomes the primary one when a mapping is
    #: given (ADR-005 D6), which `available_columns()` cannot express —
    #: it is a classmethod and cannot see the parameters (D10).
    ITEM_COLUMNS = (
        "item_1",
        "item_2",
        "gene_1_id",
        "gene_1_symbol",
        "gene_2_id",
        "gene_2_symbol",
        "group_support_count",
        "group_support_source_count",
        "group_support_names",
        "group_support_sources",
    )

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.COLUMNS

    @classmethod
    def example_input(cls):
        return {
            "input_data": ["CHEK2", "SMARCB1", "NF2"],
            "group_types": ["Pathways"],
            "max_group_size": 300,
        }

    # ------------------------------------------------------------------
    def _int_param(self, name: str, default: int) -> int:
        """An explicit zero is an answer, not an absence."""
        value = self.param(name, default)
        if value is None or (isinstance(value, str) and not value.strip()):
            return default
        return int(value)

    # ------------------------------------------------------------------
    def run(self) -> ReportResult:
        genes = self.resolve_input_list(
            self.param("input_data", required=True), param_name="input_data"
        )
        if not genes:
            raise ValueError("input_data must contain at least one gene.")

        membership = _pairing.choice(
            "membership", self.param("membership"), _pairing.MEMBERSHIPS, "both"
        )
        identifier = _pairing.resolve_gene_identifier(
            self.con, self.param("gene_identifier")
        )
        group_types = _pairing.resolve_group_types(
            self.con, self.param("group_types")
        )
        max_group_size = self._int_param(
            "max_group_size", _pairing.DEFAULT_MAX_GROUP_SIZE
        )
        min_support = self._int_param("min_group_support", 1)
        min_sources = self._int_param("min_group_sources", 1)
        max_pairs = self._int_param("max_pairs", DEFAULT_MAX_PAIRS)

        for name, value in (
            ("max_group_size", max_group_size),
            ("max_pairs", max_pairs),
        ):
            if value < 0:
                raise ValueError(f"{name} must be 0 or positive. Got: {value}.")
        if min_support < 1:
            raise ValueError(
                f"min_group_support must be at least 1. Got: {min_support}."
            )
        if min_sources < 1:
            raise ValueError(
                f"min_group_sources must be at least 1. Got: {min_sources}."
            )

        mapping = self._read_mapping()
        if mapping is not None and membership == "either":
            raise ValueError(
                "membership='either' cannot be combined with a mapping. The "
                "partner gene comes from the bundle, not from your list, so "
                "there is nothing on that side to pair. Use membership='both', "
                "or drop the mapping to get gene pairs alone."
            )

        self.register_input(genes, name="pg_input", column="input_gene")

        self.note_provenance(
            "pairing",
            {
                "membership": membership,
                "group_types": group_types,
                "max_group_size": max_group_size or None,
                "min_group_support": min_support,
                "min_group_sources": min_sources,
                "gene_identifier": identifier,
                "expanded": mapping is not None,
                "means": (
                    "Both genes come from the input."
                    if membership == "both"
                    else "One gene comes from the input; the other is any gene "
                    "it reaches through a shared group."
                ),
            },
        )

        common = self._common_cte(
            membership, group_types, max_group_size, min_support, min_sources,
            identifier,
        )
        self._note_group_filter(common, max_group_size)

        gene_pairs = self.sql(
            common
            + f"""
            SELECT
                gene_1_id, gene_1_symbol, gene_1_from_input,
                gene_2_id, gene_2_symbol, gene_2_from_input,
                group_support_count, group_support_source_count,
                group_support_types, group_support_names, group_support_sources,
                '{membership}' AS membership
            FROM named_pairs
            ORDER BY group_support_count DESC, gene_1_symbol, gene_2_symbol
            LIMIT {max_pairs if max_pairs > 0 else 9223372036854775807}
            """
        )

        if mapping is None:
            self._note_truncation(gene_pairs.num_rows, max_pairs, "gene pairs")
            return ReportResult(table=gene_pairs, provenance={}, artifacts=[])

        items = self._expand(gene_pairs, mapping, max_pairs, identifier)
        self._note_truncation(items.num_rows, max_pairs, "item pairs")

        # The expansion is what the caller asked for, so it is the table
        # `write()` exports. Gene pairs travel beside it (ADR-005 D6).
        return ReportResult(
            table=items,
            provenance={},
            artifacts=[],
            extra_tables={"gene_pairs": gene_pairs},
        )

    # ------------------------------------------------------------------
    def _common_cte(
        self,
        membership: str,
        group_types: list[str],
        max_group_size: int,
        min_support: int,
        min_sources: int,
        identifier: str,
    ) -> str:
        """Stage 1 here is only gene resolution: no variant is placed."""
        side_2_source = "seed_genes" if membership == "both" else "all_genes"
        return f"""
            WITH seed_genes AS (
                {_pairing.gene_resolution(identifier, "pg_input", "input_gene")}
            ),
            {_pairing.links_cte(group_types, max_group_size)},
            {_pairing.gene_pairs_cte(
                side_2_source=side_2_source,
                min_support=min_support,
                min_sources=min_sources,
            )}
        """

    # ------------------------------------------------------------------
    # The mapping
    # ------------------------------------------------------------------
    def _read_mapping(self) -> Optional[pa.Table]:
        """
        The caller's gene → item list, from a file or inline, or None.

        Two columns, gene then item, many-to-many in both directions. The
        item is never read: it is a label this report carries from one
        side of a pair to the other.
        """
        path = self.param("mapping_file")
        inline = self.param("mapping")
        if path and inline:
            raise ValueError(
                "Pass mapping or mapping_file, not both — two lists would have "
                "to be reconciled and there is no rule for doing that."
            )
        if path:
            rows = self._rows_from_file(Path(str(path)).expanduser())
        elif inline is not None:
            rows = self._rows_from_inline(inline)
        else:
            return None

        if not rows:
            raise ValueError(
                "The mapping is empty. Without it this report returns gene "
                "pairs; with an empty one it can only return nothing."
            )
        return pa.table(
            {
                "gene": pa.array([r[0] for r in rows], pa.string()),
                "item": pa.array([r[1] for r in rows], pa.string()),
            }
        )

    @staticmethod
    def _rows_from_file(path: Path) -> list[tuple[str, str]]:
        if not path.is_file():
            raise FileNotFoundError(f"mapping_file not found: {path}")
        rows: list[tuple[str, str]] = []
        with path.open(encoding="utf-8", errors="replace") as handle:
            sample = handle.read(8192)
            handle.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t| ")
            except csv.Error:
                dialect = csv.excel_tab
            for fields in csv.reader(handle, dialect):
                if len(fields) < 2:
                    continue
                gene, item = fields[0].strip(), fields[1].strip()
                if not gene or not item or gene.startswith("#"):
                    continue
                if gene.lower() in {"gene", "gene_symbol"} and not rows:
                    continue  # a header line, skipped once
                rows.append((gene, item))
        return rows

    @staticmethod
    def _rows_from_inline(value: Any) -> list[tuple[str, str]]:
        """
        A dict of gene → items, or a sequence of (gene, item) pairs.

        Both shapes turn up: a dict is what a notebook builds, and pairs
        are what a CSV read into a list looks like.
        """
        rows: list[tuple[str, str]] = []
        if isinstance(value, dict):
            for gene, items in value.items():
                listed = items if isinstance(items, (list, tuple, set)) else [items]
                rows.extend((str(gene).strip(), str(i).strip()) for i in listed)
        elif isinstance(value, (list, tuple)):
            for entry in value:
                if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                    rows.append((str(entry[0]).strip(), str(entry[1]).strip()))
                elif isinstance(entry, str) and "," in entry:
                    gene, item = entry.split(",", 1)
                    rows.append((gene.strip(), item.strip()))
                else:
                    raise ValueError(
                        f"A mapping entry must be (gene, item). Got: {entry!r}."
                    )
        else:
            raise ValueError(
                "mapping must be a dict of gene -> items, or a sequence of "
                "(gene, item) pairs. Use mapping_file for anything large."
            )
        return [(g, i) for g, i in rows if g and i]

    # ------------------------------------------------------------------
    def _expand(
        self, gene_pairs: pa.Table, mapping: pa.Table, max_pairs: int,
        identifier: str,
    ) -> pa.Table:
        """
        The item pairs a set of gene pairs implies.

        The cross product is not the work. These three rules are, and
        they are the same for every caller:

        - a pair is unordered, because testing (X, Y) is testing (Y, X);
        - deduplicate across the whole answer, not per gene pair, because
          the same item pair arrives through every gene pair that links
          it — 4.3% of one real run;
        - drop self-pairs, since an item attached to both genes of a pair
          would otherwise pair with itself.

        Both genes must carry at least one item. Not "both were named" —
        a gene can be named and carry nothing, and then there is nothing
        on its side to pair.
        """
        self.con.register("pg_gene_pairs", gene_pairs)
        self.con.register("pg_mapping", mapping)
        self._resolve_mapping_genes(identifier)

        return self.sql(
            f"""
            WITH pairs AS (
                SELECT
                    m1.item AS item_a, m2.item AS item_b,
                    p.gene_1_id, p.gene_1_symbol, p.gene_2_id, p.gene_2_symbol,
                    p.group_support_count, p.group_support_source_count,
                    p.group_support_names, p.group_support_sources
                FROM pg_gene_pairs p
                JOIN pg_mapped m1 ON m1.gene_id = p.gene_1_id
                JOIN pg_mapped m2 ON m2.gene_id = p.gene_2_id
                -- An item on both genes would otherwise pair with itself.
                WHERE m1.item <> m2.item
            )
            SELECT
                least(item_a, item_b)    AS item_1,
                greatest(item_a, item_b) AS item_2,
                gene_1_id, gene_1_symbol, gene_2_id, gene_2_symbol,
                group_support_count, group_support_source_count,
                group_support_names, group_support_sources
            FROM pairs
            -- Unordered and global: the same item pair reaches here
            -- through every gene pair linking it, and through both
            -- orientations of each.
            QUALIFY row_number() OVER (
                PARTITION BY least(item_a, item_b), greatest(item_a, item_b)
                ORDER BY group_support_count DESC, gene_1_symbol, gene_2_symbol
            ) = 1
            ORDER BY group_support_count DESC, item_1, item_2
            LIMIT {max_pairs if max_pairs > 0 else 9223372036854775807}
            """
        )

    def _resolve_mapping_genes(self, identifier: str) -> None:
        """
        Name the mapping's genes the same way `input_data` is named.

        A caller who wrote `ENSG00000141510` in one column and `TP53` in
        the other would otherwise get nothing, silently — and joining on
        the symbol alone assumes the caller spells genes the way the
        bundle does, which is the assumption the alias table exists to
        remove.

        A mapping gene the bundle cannot resolve is reported rather than
        dropped in silence: it is the caller's data going missing.
        """
        # The same rule the input follows: the caller says which column
        # they are naming, and nothing here guesses from the shape of the
        # string. Guessing is wrong 14,335 ways — that many bare-number
        # aliases are also the entity id of a different gene.
        self.con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE pg_mapped AS
            SELECT gene AS gene, gene_id, item FROM (
                SELECT m.item, r.input_value AS gene, r.gene_id
                FROM ({_pairing.gene_resolution(identifier, "pg_mapping", "gene")}) r
                JOIN pg_mapping m ON m.gene = r.input_value
            )
            """
        )

        # A name answering to several genes attaches its items to all of
        # them. That is what the rest of Biofilter does with an ambiguous
        # name, and it is worth saying: the caller meant one gene.
        ambiguous = self.con.execute(
            """
            SELECT gene, count(DISTINCT gene_id) AS genes
            FROM pg_mapped GROUP BY 1 HAVING count(DISTINCT gene_id) > 1
            ORDER BY 2 DESC, 1
            """
        ).fetchall()
        if ambiguous:
            self.warn(
                f"{len(ambiguous)} name(s) in the mapping answer to more than "
                f"one gene, so their items attach to each. Supply an entity id "
                f"to say which you meant.",
                names=[f"{r[0]} ({r[1]})" for r in ambiguous[:20]],
                total=len(ambiguous),
            )
        # Against what actually resolved, by either route. Checking only
        # the alias route would call every id-resolved gene missing.
        unresolved = self.con.execute(
            """
            SELECT DISTINCT m.gene FROM pg_mapping m
            WHERE m.gene NOT IN (SELECT gene FROM pg_mapped)
            ORDER BY 1
            """
        ).fetchall()
        if unresolved:
            names = [r[0] for r in unresolved]
            self.warn(
                f"{len(names)} gene(s) in the mapping do not resolve to a gene "
                f"in this bundle, so their items cannot be paired.",
                genes=names[:20],
                total=len(names),
            )

    # ------------------------------------------------------------------
    def _note_group_filter(self, common: str, max_group_size: int) -> None:
        rows = self.sql(
            common + _pairing.group_filter_select(max_group_size)
        ).to_pylist()
        self.note_provenance(
            "group_filter",
            _pairing.group_filter_block(rows[0] if rows else {}, max_group_size),
        )

    def _note_truncation(self, returned: int, max_pairs: int, what: str) -> None:
        hit = max_pairs > 0 and returned >= max_pairs
        self.note_provenance(
            "truncation",
            {
                "max_pairs": max_pairs or None,
                "applied": hit,
                "returned": returned,
                "means": (
                    f"The cap was reached, so this is not every one of the "
                    f"{what}. The ones kept have the most group support."
                    if hit
                    else f"Every one of the {what} that met the criteria is here."
                ),
            },
        )
