"""
Protein annotation: what the bundle knows about a list of proteins.

Migrated from the relational layer (ADR-004 §6).

The wrinkle here is isoforms. A protein can have several entities — one
canonical and one per isoform — and an input may name either. Facts about
the protein (function, location, Pfam domains, relationships, aliases)
belong to the canonical entity, so the report resolves the input, then
follows it to the canonical entity before annotating. Both ids are
reported: `entity_id` is what the input matched, `canonical_entity_id` is
what the annotation describes.
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


class AnnotationMasterProteinReport(AnnotationReportBase):
    name = "annotation_master_protein"
    description = (
        "Protein annotation for input proteins or aliases: canonical accession, "
        "function, location and tissue expression, isoform resolution, Pfam "
        "domains by type, and relationship counts by related entity group."
    )

    entity_groups = ("proteins", "protein")

    requires = BASE_REQUIRES + (
        "protein_masters",
        "protein_entities",
        "protein_pfams",
        "protein_pfam_links",
    )

    COLUMNS = (
        "input_value",
        "input_matched_alias",
        "entity_id",
        "canonical_entity_id",
        "protein_id",
        "input_is_isoform",
        "input_isoform_accession",
        "isoform_count",
        "function",
        "location",
        "tissue_expression",
        "pseudogene_note",
        "protein_source_system",
        "protein_data_source",
        "protein_etl_package_id",
        "pfam_total_count",
        "pfam_count_by_type",
        "pfam_ids_by_type",
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
            "input_data": ["P04637", "Q09472"],
            "include_relationships": True,
            "include_pfam_summary": True,
            "emit_not_found_rows": True,
        }

    def _follow_to_canonical(self) -> None:
        """
        Rewrite `resolved` so `entity_id` is the canonical protein entity.

        The shared CTEs join on `resolved.entity_id`; pointing that at the
        canonical entity is what makes an isoform input report the
        protein's relationships rather than the isoform's, which are
        usually none. The entity the input actually matched is kept as
        `input_entity_id`, and reported.
        """
        self.con.execute("CREATE OR REPLACE TEMP TABLE protein_input AS SELECT * FROM resolved")
        self.con.execute(
            """
            CREATE OR REPLACE TEMP TABLE resolved AS
            WITH matched AS (
                SELECT
                    pe.entity_id,
                    pe.protein_id AS protein_master_id,
                    pe.is_isoform,
                    pe.isoform_accession
                FROM protein_entities pe
                WHERE pe.entity_id IN (
                    SELECT entity_id FROM protein_input WHERE entity_id IS NOT NULL
                )
                QUALIFY row_number() OVER (
                    PARTITION BY pe.entity_id ORDER BY pe.id
                ) = 1
            ),
            canonical AS (
                SELECT pe.protein_id AS protein_master_id, min(pe.entity_id) AS entity_id
                FROM protein_entities pe
                WHERE NOT pe.is_isoform
                  AND pe.protein_id IN (SELECT protein_master_id FROM matched)
                GROUP BY pe.protein_id
            )
            SELECT
                i.input_value,
                i.matched_alias,
                i.primary_name,
                i.entity_id AS input_entity_id,
                m.protein_master_id,
                m.is_isoform      AS input_is_isoform,
                m.isoform_accession AS input_isoform_accession,
                c.entity_id       AS canonical_entity_id,
                -- What the annotation is about. Falls back to the matched
                -- entity when the protein has no canonical row at all.
                coalesce(c.entity_id, i.entity_id) AS entity_id
            FROM protein_input i
            LEFT JOIN matched   m ON m.entity_id = i.entity_id
            LEFT JOIN canonical c ON c.protein_master_id = m.protein_master_id
            """
        )

    def run(self) -> pa.Table:
        include_relationships = parse_bool(self.param("include_relationships"), True)
        include_pfam = parse_bool(self.param("include_pfam_summary"), True)
        emit_not_found = parse_bool(self.param("emit_not_found_rows"), True)

        self.resolve(self.param("input_data", required=True))
        self._follow_to_canonical()

        pfam = (
            """
            pfam_rows AS (
                SELECT
                    r.entity_id,
                    coalesce(nullif(trim(pf.type), ''), 'unknown') AS pfam_type,
                    pf.pfam_acc
                FROM resolved r
                JOIN protein_pfam_links link ON link.protein_id = r.protein_master_id
                JOIN protein_pfams pf ON pf.id = link.pfam_pk_id
            ),
            pfam_by_type AS (
                SELECT
                    entity_id,
                    pfam_type,
                    CAST(count(DISTINCT pfam_acc) AS BIGINT) AS n,
                    list_sort(list_distinct(list(pfam_acc))) AS accessions
                FROM pfam_rows
                GROUP BY 1, 2
            ),
            pfam AS (
                SELECT
                    entity_id,
                    CAST(sum(n) AS BIGINT) AS total,
                    list(struct_pack(type := pfam_type, count := n)
                         ORDER BY n DESC, pfam_type) AS count_by_type,
                    list(struct_pack(type := pfam_type, ids := accessions)
                         ORDER BY pfam_type) AS ids_by_type
                FROM pfam_by_type
                GROUP BY entity_id
            )
            """
            if include_pfam
            else """
            pfam AS (
                SELECT
                    CAST(NULL AS BIGINT) AS entity_id,
                    CAST(NULL AS BIGINT) AS total,
                    CAST([] AS STRUCT(type VARCHAR, count BIGINT)[]) AS count_by_type,
                    CAST([] AS STRUCT(type VARCHAR, ids VARCHAR[])[]) AS ids_by_type
                WHERE false
            )
            """
        )

        having = "" if emit_not_found else "WHERE r.input_entity_id IS NOT NULL"

        return self.sql(
            f"""
            WITH core AS (
                SELECT
                    pm.id AS protein_master_id,
                    pm.protein_id,
                    pm.function,
                    pm.location,
                    pm.tissue_expression,
                    pm.pseudogene_note
                FROM protein_masters pm
                WHERE pm.id IN (
                    SELECT protein_master_id FROM resolved
                    WHERE protein_master_id IS NOT NULL
                )
            ),
            isoforms AS (
                SELECT
                    pe.protein_id AS protein_master_id,
                    CAST(count(DISTINCT pe.entity_id) AS BIGINT) AS n
                FROM protein_entities pe
                WHERE pe.is_isoform
                  AND pe.protein_id IN (
                      SELECT protein_master_id FROM resolved
                      WHERE protein_master_id IS NOT NULL
                  )
                GROUP BY pe.protein_id
            ),
            {provenance_cte("protein_masters", key="id")},
            {aliases_cte()},
            {relationships_cte(include_relationships)},
            {pfam}
            SELECT
                r.input_value,
                r.matched_alias                 AS input_matched_alias,
                r.input_entity_id               AS entity_id,
                r.canonical_entity_id,
                c.protein_id,
                r.input_is_isoform,
                r.input_isoform_accession,
                coalesce(iso.n, 0)              AS isoform_count,
                c.function,
                c.location,
                c.tissue_expression,
                c.pseudogene_note,
                p.source_system                 AS protein_source_system,
                p.data_source                   AS protein_data_source,
                p.etl_package_id                AS protein_etl_package_id,
                coalesce(pf.total, 0)           AS pfam_total_count,
                coalesce(pf.count_by_type, [])  AS pfam_count_by_type,
                pf.ids_by_type                  AS pfam_ids_by_type,
                coalesce(rel.by_group, [])      AS entity_relationships_by_group,
                coalesce(rel.total, 0)          AS total_entity_relationships,
                {other_aliases_expr(
                    "c.protein_id", "r.primary_name", "r.matched_alias"
                )}                              AS other_aliases,
                CASE
                    WHEN r.input_entity_id IS NULL THEN 'not_found'
                    WHEN c.protein_master_id IS NULL THEN 'partial'
                    ELSE 'ok'
                END                             AS status,
                nullif(concat_ws(' ',
                    CASE WHEN r.input_entity_id IS NULL
                         THEN 'Input not resolved to a Protein entity.' END,
                    CASE WHEN r.input_entity_id IS NOT NULL
                          AND c.protein_master_id IS NULL
                         THEN 'Protein resolved but no protein_masters row found.' END,
                    CASE WHEN r.input_is_isoform
                         THEN 'Input named an isoform; annotation describes the '
                              || 'canonical protein.' END
                ), '')                          AS note
            FROM resolved r
            LEFT JOIN core        c   ON c.protein_master_id = r.protein_master_id
            LEFT JOIN isoforms    iso ON iso.protein_master_id = r.protein_master_id
            LEFT JOIN provenance  p   ON p.entity_id = r.protein_master_id
            LEFT JOIN all_aliases a   ON a.entity_id = r.entity_id
            LEFT JOIN rels        rel ON rel.entity_id = r.entity_id
            LEFT JOIN pfam        pf  ON pf.entity_id = r.entity_id
            {having}
            ORDER BY r.input_value, r.input_entity_id
            """
        )
