"""
Pathway annotation: what the bundle knows about a list of pathways.

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


class AnnotatePathwayReport(AnnotationReportBase):
    name = "annotate_pathway"
    description = (
        "Pathway annotation for input pathways or aliases: canonical id and "
        "description, which source contributed it, and relationship counts by "
        "related entity group."
    )

    entity_groups = ("pathways", "pathway")

    requires = BASE_REQUIRES + ("pathway_masters",)

    COLUMNS = (
        "input_value",
        "input_matched_alias",
        "entity_id",
        "pathway_id",
        "pathway_description",
        "pathway_source_system",
        "pathway_data_source",
        "pathway_etl_package_id",
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
            "input_data": ["R-HSA-109581", "hsa04210"],
            "include_relationships": True,
            "emit_not_found_rows": True,
        }

    def run(self) -> pa.Table:
        include_relationships = parse_bool(self.param("include_relationships"), True)
        emit_not_found = parse_bool(self.param("emit_not_found_rows"), True)

        self.resolve(self.param("input_data", required=True))
        having = "" if emit_not_found else "WHERE r.entity_id IS NOT NULL"

        return self.sql(
            f"""
            WITH core AS (
                SELECT pm.entity_id, pm.pathway_id, pm.description
                FROM pathway_masters pm
                WHERE pm.entity_id IN (SELECT entity_id FROM resolved)
                QUALIFY row_number() OVER (
                    PARTITION BY pm.entity_id ORDER BY pm.id
                ) = 1
            ),
            {provenance_cte("pathway_masters")},
            {aliases_cte()},
            {relationships_cte(include_relationships)}
            SELECT
                r.input_value,
                r.matched_alias            AS input_matched_alias,
                r.entity_id,
                c.pathway_id,
                coalesce(c.description, r.primary_name) AS pathway_description,
                p.source_system            AS pathway_source_system,
                p.data_source              AS pathway_data_source,
                p.etl_package_id           AS pathway_etl_package_id,
                coalesce(rel.by_group, []) AS entity_relationships_by_group,
                coalesce(rel.total, 0)     AS total_entity_relationships,
                {other_aliases_expr(
                    "c.pathway_id", "c.description", "r.primary_name", "r.matched_alias"
                )}                         AS other_aliases,
                CASE
                    WHEN r.entity_id IS NULL THEN 'not_found'
                    WHEN c.entity_id IS NULL THEN 'partial'
                    ELSE 'ok'
                END                        AS status,
                nullif(concat_ws(' ',
                    CASE WHEN r.entity_id IS NULL
                         THEN 'Input not resolved to a Pathway entity.' END,
                    CASE WHEN r.entity_id IS NOT NULL AND c.entity_id IS NULL
                         THEN 'Pathway resolved but no pathway_masters row found.' END
                ), '')                     AS note
            FROM resolved r
            LEFT JOIN core        c   ON c.entity_id = r.entity_id
            LEFT JOIN provenance  p   ON p.entity_id = r.entity_id
            LEFT JOIN all_aliases a   ON a.entity_id = r.entity_id
            LEFT JOIN rels        rel ON rel.entity_id = r.entity_id
            {having}
            ORDER BY r.input_value, r.entity_id
            """
        )
