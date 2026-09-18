"""
Gene Ontology annotation: what the bundle knows about a list of GO terms.

Migrated from the relational layer (ADR-004 §6).
"""

from __future__ import annotations

from typing import Sequence

import pyarrow as pa

from biofilter.modules.report.reports._annotation import (
    BASE_REQUIRES,
    AnnotationReportBase,
    aliases_cte,
    other_aliases_expr,
    parse_bool,
    provenance_cte,
    relationships_cte,
)

DEFAULT_MAX_TERMS_PER_SIDE = 25


class AnnotateGOReport(AnnotationReportBase):
    name = "annotate_go"
    description = (
        "Gene Ontology annotation for input terms or aliases: GO id, name and "
        "namespace, how many parents and children the term has in the ontology, "
        "by which relation types, and relationship counts by related entity group."
    )

    entity_groups = ("gene ontology", "go")

    requires = BASE_REQUIRES + ("go_masters", "go_relations")

    COLUMNS = (
        "input_value",
        "input_matched_alias",
        "entity_id",
        "go_id",
        "go_name",
        "go_namespace",
        "go_source_system",
        "go_data_source",
        "go_etl_package_id",
        "go_parent_count",
        "go_child_count",
        "go_parent_relation_types",
        "go_child_relation_types",
        "go_parent_ids",
        "go_child_ids",
        "entity_relationships_by_group",
        "total_entity_relationships",
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
            "input_data": ["GO:0006915", "apoptotic process"],
            "include_relationships": True,
            "include_go_relation_details": True,
            "max_go_terms_per_side": DEFAULT_MAX_TERMS_PER_SIDE,
            "emit_not_found_rows": True,
        }

    def run(self) -> pa.Table:
        include_relationships = parse_bool(self.param("include_relationships"), True)
        include_details = parse_bool(self.param("include_go_relation_details"), True)
        emit_not_found = parse_bool(self.param("emit_not_found_rows"), True)
        max_terms = int(
            self.param("max_go_terms_per_side", DEFAULT_MAX_TERMS_PER_SIDE) or 0
        )

        self.resolve(self.param("input_data", required=True))

        # `go_relations` joins go_masters.id, not GO:xxxxxxx, so each side
        # is resolved back to the GO id the reader recognises.
        ids = (
            f"list_sort(list_distinct(list(other_go_id)))[1:{max_terms}]"
            if include_details and max_terms > 0
            else (
                "list_sort(list_distinct(list(other_go_id)))"
                if include_details
                else "CAST(NULL AS VARCHAR[])"
            )
        )

        having = "" if emit_not_found else "WHERE r.entity_id IS NOT NULL"

        return self.sql(
            f"""
            WITH core AS (
                SELECT g.entity_id, g.id AS go_master_id, g.go_id, g.name, g.namespace
                FROM go_masters g
                WHERE g.entity_id IN (SELECT entity_id FROM resolved)
                QUALIFY row_number() OVER (
                    PARTITION BY g.entity_id ORDER BY g.id
                ) = 1
            ),
            -- One row per (term, neighbour), with the side it sits on.
            edges AS (
                SELECT
                    c.entity_id,
                    'parent' AS side,
                    rel.relation_type,
                    up.go_id AS other_go_id
                FROM core c
                JOIN go_relations rel ON rel.child_id = c.go_master_id
                LEFT JOIN go_masters up ON up.id = rel.parent_id
                UNION ALL
                SELECT
                    c.entity_id,
                    'child' AS side,
                    rel.relation_type,
                    down.go_id AS other_go_id
                FROM core c
                JOIN go_relations rel ON rel.parent_id = c.go_master_id
                LEFT JOIN go_masters down ON down.id = rel.child_id
            ),
            edge_summary AS (
                SELECT
                    entity_id,
                    side,
                    CAST(count(*) AS BIGINT) AS n,
                    list_sort(list_distinct(list(
                        coalesce(nullif(trim(relation_type), ''), 'unknown')
                    ))) AS relation_types,
                    {ids} AS ids
                FROM edges
                GROUP BY entity_id, side
            ),
            {provenance_cte("go_masters")},
            {aliases_cte()},
            {relationships_cte(include_relationships)}
            SELECT
                r.input_value,
                r.matched_alias              AS input_matched_alias,
                r.entity_id,
                c.go_id,
                coalesce(c.name, r.primary_name, r.matched_alias) AS go_name,
                c.namespace                  AS go_namespace,
                p.source_system              AS go_source_system,
                p.data_source                AS go_data_source,
                p.etl_package_id             AS go_etl_package_id,
                coalesce(up.n, 0)            AS go_parent_count,
                coalesce(down.n, 0)          AS go_child_count,
                coalesce(up.relation_types, []) AS go_parent_relation_types,
                coalesce(down.relation_types, []) AS go_child_relation_types,
                up.ids                       AS go_parent_ids,
                down.ids                     AS go_child_ids,
                coalesce(rel.by_group, [])   AS entity_relationships_by_group,
                coalesce(rel.total, 0)       AS total_entity_relationships,
                {other_aliases_expr(
                    "c.go_id", "c.name", "r.primary_name", "r.matched_alias"
                )}                           AS other_aliases,
                CASE
                    WHEN r.entity_id IS NULL THEN 'not_found'
                    WHEN c.entity_id IS NULL THEN 'partial'
                    ELSE 'ok'
                END                          AS status,
                nullif(concat_ws(' ',
                    CASE WHEN r.entity_id IS NULL
                         THEN 'Input not resolved to a Gene Ontology entity.' END,
                    CASE WHEN r.entity_id IS NOT NULL AND c.entity_id IS NULL
                         THEN 'Term resolved but no go_masters row found.' END
                ), '')                       AS note
            FROM resolved r
            LEFT JOIN core         c    ON c.entity_id = r.entity_id
            LEFT JOIN provenance   p    ON p.entity_id = r.entity_id
            LEFT JOIN all_aliases  a    ON a.entity_id = r.entity_id
            LEFT JOIN rels         rel  ON rel.entity_id = r.entity_id
            LEFT JOIN edge_summary up   ON up.entity_id = r.entity_id
                                       AND up.side = 'parent'
            LEFT JOIN edge_summary down ON down.entity_id = r.entity_id
                                       AND down.side = 'child'
            {having}
            ORDER BY r.input_value, r.entity_id
            """
        )
