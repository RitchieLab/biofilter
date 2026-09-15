"""
Gene annotation: everything the bundle knows about a list of genes.

First report migrated to the native module (ADR-004 §6). The relational
version issued nine queries and merged them with Python dictionaries;
this asks the same question in two statements and lets DuckDB plan the
joins. It also reads `variant_masters` as the bundle actually spells it,
which the old one could not — it selected `variant_id`, a column 4.3.0
bundles do not carry, so the report failed outright.
"""

from __future__ import annotations

from typing import Any, Sequence

import pyarrow as pa

from biofilter.modules.report.reports.base_report import ReportBase

ALL_TOKEN = "__ALL__"

#: Which alias wins when an input matches several. Mirrors the ranking
#: the relational report applied in Python.
_ALIAS_RANK = """
    CASE
        WHEN a.is_primary THEN 0
        WHEN lower(a.alias_type) IN ('symbol', 'preferred') THEN 1
        WHEN lower(a.alias_type) = 'code' THEN 2
        WHEN lower(a.alias_type) IN ('synonym', 'name') THEN 3
        ELSE 4
    END
"""


def _parse_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


class AnnotateGeneReport(ReportBase):
    name = "annotate_gene"
    description = (
        "Gene annotation for input genes or aliases: canonical IDs, gene "
        "metadata, build 38 coordinates, relationship counts by related entity "
        "group, and optionally the number of variants in the gene's range."
    )

    requires = (
        "entities",
        "entity_aliases",
        "entity_groups",
        "entity_locations",
        "entity_relationships",
        "gene_masters",
        "gene_groups",
        "gene_group_memberships",
        "gene_locus_groups",
        "gene_locus_types",
        "omic_status",
    )

    #: Used for `variant_count_in_gene_range`. Without it the column is
    #: null, which the provenance explains.
    optional = ("variant_masters",)

    COLUMNS = (
        "input_value",
        "input_matched_alias",
        "entity_id",
        "gene_symbol",
        "hgnc_id",
        "ensembl_id",
        "entrez_id",
        "hgnc_status",
        "omic_status",
        "gene_locus_group",
        "gene_locus_type",
        "gene_groups",
        "build",
        "chromosome",
        "start_position",
        "end_position",
        "entity_relationships_by_group",
        "total_entity_relationships",
        "variant_count_in_gene_range",
        "other_aliases",
        "status",
        "note",
    )

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.COLUMNS

    @classmethod
    def example_input(cls):
        return {
            "input_data": ["TP53", "BRCA1", "EGFR"],
            "include_relationships": True,
            "include_variant_summary": True,
            "emit_not_found_rows": True,
        }

    # ------------------------------------------------------------------
    def run(self) -> pa.Table:
        raw_input = self.param("input_data", required=True)
        include_relationships = _parse_bool(self.param("include_relationships"), True)
        include_variants = _parse_bool(self.param("include_variant_summary"), True)
        emit_not_found = _parse_bool(self.param("emit_not_found_rows"), True)

        if self._is_all(raw_input):
            self._resolve_every_gene()
        else:
            values = self.resolve_input_list(raw_input, param_name="input_data")
            if not values:
                raise ValueError("input_data must contain at least one value.")
            self._resolve_inputs(values)

        return self._annotate(
            include_relationships=include_relationships,
            include_variants=include_variants,
            emit_not_found=emit_not_found,
        )

    # ------------------------------------------------------------------
    # Resolution — input to gene entity
    # ------------------------------------------------------------------
    @staticmethod
    def _is_all(raw: Any) -> bool:
        if isinstance(raw, str):
            return raw.strip().upper() == ALL_TOKEN
        if isinstance(raw, (list, tuple, set)):
            items = [str(v).strip() for v in raw if str(v).strip()]
            return len(items) == 1 and items[0].upper() == ALL_TOKEN
        return False

    def _gene_entities_cte(self) -> str:
        return """
            gene_entities AS (
                SELECT e.id AS entity_id
                FROM entities e
                JOIN entity_groups g ON g.id = e.group_id
                WHERE lower(g.name) IN ('gene', 'genes')
            ),
            primary_alias AS (
                SELECT entity_id, alias_value
                FROM entity_aliases
                WHERE is_primary
                QUALIFY row_number() OVER (
                    PARTITION BY entity_id ORDER BY alias_value
                ) = 1
            )
        """

    def _resolve_inputs(self, values: list[str]) -> None:
        """
        Match each input against the aliases of gene entities.

        The input is a registered relation on the left of a LEFT JOIN, so
        a value that matches nothing survives to the output with
        `status = 'not_found'` rather than disappearing. Matching is on
        the pre-lowercased input column against `alias_norm`, which is an
        equality join — wrapping `lower()` around the bundle's column
        would cost the parquet statistics.
        """
        self.register_input(values, name="input_genes", column="input_value")
        self.con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE resolved AS
            WITH {self._gene_entities_cte()},
            candidates AS (
                SELECT
                    i.input_value,
                    a.entity_id,
                    a.alias_value AS matched_alias,
                    {_ALIAS_RANK} AS alias_rank
                FROM input_genes i
                JOIN entity_aliases a
                  ON lower(coalesce(a.alias_norm, a.alias_value)) = i.input_value_norm
                JOIN gene_entities ge ON ge.entity_id = a.entity_id
                QUALIFY row_number() OVER (
                    PARTITION BY i.input_value
                    ORDER BY alias_rank, a.alias_value, a.entity_id
                ) = 1
            )
            SELECT
                i.input_value,
                c.entity_id,
                c.matched_alias,
                p.alias_value AS primary_name
            FROM input_genes i
            LEFT JOIN candidates c ON c.input_value = i.input_value
            LEFT JOIN primary_alias p ON p.entity_id = c.entity_id
            """
        )

    def _resolve_every_gene(self) -> None:
        """`input_data='__ALL__'`: every gene entity in the bundle."""
        self.con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE resolved AS
            WITH {self._gene_entities_cte()}
            SELECT
                coalesce(
                    p.alias_value, gm.symbol, 'ENTITY:' || CAST(ge.entity_id AS VARCHAR)
                ) AS input_value,
                ge.entity_id,
                coalesce(
                    p.alias_value, gm.symbol, 'ENTITY:' || CAST(ge.entity_id AS VARCHAR)
                ) AS matched_alias,
                p.alias_value AS primary_name
            FROM gene_entities ge
            JOIN gene_masters gm ON gm.entity_id = ge.entity_id
            LEFT JOIN primary_alias p ON p.entity_id = ge.entity_id
            ORDER BY ge.entity_id
            """
        )
        if self.con.execute("SELECT count(*) FROM resolved").fetchone()[0] == 0:
            raise ValueError(
                "No gene entities in this bundle for input_data='__ALL__'."
            )

    # ------------------------------------------------------------------
    # Annotation — one statement
    # ------------------------------------------------------------------
    def _annotate(
        self,
        *,
        include_relationships: bool,
        include_variants: bool,
        emit_not_found: bool,
    ) -> pa.Table:
        # Each optional section is a CTE that returns no rows when it is
        # switched off, so the shape of the result never changes — only
        # what is in it. The planner prunes the unreachable scan.
        rel_counts = (
            """
            rel_pairs AS (
                SELECT r.entity_1_id AS entity_id, r.entity_2_group_id AS other_group_id
                FROM entity_relationships r
                JOIN resolved x ON x.entity_id = r.entity_1_id
                UNION ALL
                SELECT r.entity_2_id AS entity_id, r.entity_1_group_id AS other_group_id
                FROM entity_relationships r
                JOIN resolved x ON x.entity_id = r.entity_2_id
                WHERE r.entity_2_id <> r.entity_1_id
            ),
            rel_by_group AS (
                SELECT
                    entity_id,
                    coalesce(g.name, 'Unknown') AS group_name,
                    count(*) AS n
                FROM rel_pairs p
                LEFT JOIN entity_groups g ON g.id = p.other_group_id
                GROUP BY 1, 2
            ),
            rels AS (
                SELECT
                    entity_id,
                    list(struct_pack(group_name := group_name, count := n)
                         ORDER BY n DESC, group_name) AS by_group,
                    CAST(sum(n) AS BIGINT) AS total
                FROM rel_by_group
                GROUP BY entity_id
            )
            """
            if include_relationships
            else """
            rels AS (
                SELECT
                    CAST(NULL AS BIGINT) AS entity_id,
                    CAST([] AS STRUCT(group_name VARCHAR, count BIGINT)[]) AS by_group,
                    CAST(NULL AS BIGINT) AS total
                WHERE false
            )
            """
        )

        # A gene with no build-38 location has no range to count in, so
        # the count is null rather than zero — "unknown", not "none".
        variants = (
            """
            variant_counts AS (
                SELECT l.entity_id, CAST(count(*) AS BIGINT) AS n
                FROM loc l
                JOIN variant_masters v
                  ON v.chromosome = l.chromosome
                 AND v.position BETWEEN l.start_pos AND l.end_pos
                GROUP BY l.entity_id
            )
            """
            if include_variants and self.bundle.has("variant_masters")
            else """
            variant_counts AS (
                SELECT CAST(NULL AS BIGINT) AS entity_id, CAST(NULL AS BIGINT) AS n
                WHERE false
            )
            """
        )

        having = "" if emit_not_found else "WHERE r.entity_id IS NOT NULL"

        # Null when the section is switched off, and null when the gene
        # has no range to count in. Zero would claim we counted and found
        # none; these two cases are "not asked" and "cannot be asked".
        variant_column = (
            "CASE WHEN l.entity_id IS NULL THEN NULL ELSE coalesce(vc.n, 0) END"
            if include_variants
            else "CAST(NULL AS BIGINT)"
        )

        return self.sql(
            f"""
            WITH loc AS (
                SELECT entity_id, build, chromosome, start_pos, end_pos
                FROM entity_locations
                WHERE build = 38
                QUALIFY row_number() OVER (
                    PARTITION BY entity_id ORDER BY start_pos
                ) = 1
            ),
            core AS (
                SELECT
                    gm.entity_id,
                    gm.id AS gene_master_id,
                    gm.symbol,
                    gm.hgnc_status,
                    os.name AS omic_status,
                    lg.name AS locus_group,
                    lt.name AS locus_type
                FROM gene_masters gm
                LEFT JOIN omic_status os ON os.id = gm.omic_status_id
                LEFT JOIN gene_locus_groups lg ON lg.id = gm.locus_group_id
                LEFT JOIN gene_locus_types lt ON lt.id = gm.locus_type_id
                QUALIFY row_number() OVER (
                    PARTITION BY gm.entity_id ORDER BY gm.id
                ) = 1
            ),
            -- min() is how the relational report broke ties too
            -- (`sorted(set(candidates))[0]`), so the choice is the same
            -- and no longer depends on row order.
            xref AS (
                SELECT
                    entity_id,
                    coalesce(
                        min(CASE WHEN upper(xref_source) = 'HGNC'
                                  AND lower(alias_type) = 'code'
                                  AND alias_value LIKE 'HGNC:%'
                                 THEN alias_value END),
                        min(CASE WHEN upper(xref_source) = 'HGNC'
                                  AND lower(alias_type) = 'code'
                                 THEN alias_value END)
                    ) AS hgnc_id,
                    min(CASE WHEN upper(xref_source) = 'ENSEMBL'
                              AND lower(alias_type) = 'code'
                             THEN alias_value END) AS ensembl_id,
                    min(CASE WHEN upper(xref_source) = 'ENTREZ'
                              AND lower(alias_type) = 'code'
                             THEN alias_value END) AS entrez_id
                FROM entity_aliases
                WHERE entity_id IN (SELECT entity_id FROM resolved)
                GROUP BY entity_id
            ),
            all_aliases AS (
                SELECT entity_id, list(DISTINCT trim(alias_value)) AS values
                FROM entity_aliases
                WHERE entity_id IN (SELECT entity_id FROM resolved)
                  AND trim(coalesce(alias_value, '')) <> ''
                GROUP BY entity_id
            ),
            groups AS (
                SELECT gm.entity_id, list(DISTINCT gg.name ORDER BY gg.name) AS names
                FROM gene_masters gm
                JOIN gene_group_memberships m ON m.gene_id = gm.id
                JOIN gene_groups gg ON gg.id = m.group_id
                WHERE gm.entity_id IN (SELECT entity_id FROM resolved)
                GROUP BY gm.entity_id
            ),
            {rel_counts},
            {variants}
            SELECT
                r.input_value,
                r.matched_alias                       AS input_matched_alias,
                r.entity_id,
                coalesce(c.symbol, r.primary_name, r.matched_alias) AS gene_symbol,
                x.hgnc_id,
                x.ensembl_id,
                x.entrez_id,
                c.hgnc_status,
                c.omic_status,
                c.locus_group                         AS gene_locus_group,
                c.locus_type                          AS gene_locus_type,
                coalesce(g.names, [])                 AS gene_groups,
                l.build,
                l.chromosome,
                l.start_pos                           AS start_position,
                l.end_pos                             AS end_position,
                coalesce(rel.by_group, [])            AS entity_relationships_by_group,
                coalesce(rel.total, 0)                AS total_entity_relationships,
                {variant_column}                      AS variant_count_in_gene_range,
                coalesce(
                    list_sort(list_filter(
                        a.values,
                        v -> v NOT IN [
                            coalesce(c.symbol, r.primary_name, r.matched_alias, ''),
                            coalesce(x.hgnc_id, ''),
                            coalesce(x.ensembl_id, ''),
                            coalesce(x.entrez_id, '')
                        ]
                    )), []
                )                                     AS other_aliases,
                CASE
                    WHEN r.entity_id IS NULL THEN 'not_found'
                    WHEN c.entity_id IS NULL OR l.entity_id IS NULL THEN 'partial'
                    ELSE 'ok'
                END                                   AS status,
                nullif(concat_ws(' ',
                    CASE WHEN r.entity_id IS NULL
                         THEN 'Input not resolved to a Gene entity.' END,
                    CASE WHEN r.entity_id IS NOT NULL AND c.entity_id IS NULL
                         THEN 'Gene resolved but no gene_masters row found.' END,
                    CASE WHEN r.entity_id IS NOT NULL AND l.entity_id IS NULL
                         THEN 'No build 38 location found for this gene.' END
                ), '')                                AS note
            FROM resolved r
            LEFT JOIN core            c  ON c.entity_id  = r.entity_id
            LEFT JOIN loc             l  ON l.entity_id  = r.entity_id
            LEFT JOIN xref            x  ON x.entity_id  = r.entity_id
            LEFT JOIN all_aliases     a  ON a.entity_id  = r.entity_id
            LEFT JOIN groups          g  ON g.entity_id  = r.entity_id
            LEFT JOIN rels            rel ON rel.entity_id = r.entity_id
            LEFT JOIN variant_counts  vc ON vc.entity_id = r.entity_id
            {having}
            ORDER BY r.input_value, r.entity_id
            """
        )
