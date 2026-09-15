"""
Disease annotation: what the bundle knows about a list of diseases.

Migrated from the relational layer (ADR-004 §6). The shape is
`annotation_master_gene`'s, and the resolution step is shared with it —
see `_annotation.py`.
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
    xrefs_cte,
)

#: The source whose gene-disease assertions the summary counts.
CLINGEN = "clingen"


class AnnotationMasterDiseaseReport(AnnotationReportBase):
    name = "annotation_master_disease"
    description = (
        "Disease annotation for input diseases or aliases: canonical IDs, label "
        "and description, disease groups, cross-references by source, and "
        "relationship counts including the genes ClinGen links to the disease."
    )

    entity_groups = ("diseases", "disease")

    requires = BASE_REQUIRES + (
        "disease_masters",
        "disease_groups",
        "disease_group_memberships",
        "omic_status",
    )

    COLUMNS = (
        "input_value",
        "input_matched_alias",
        "entity_id",
        "disease_id",
        "disease_label",
        "disease_description",
        "omic_status",
        "disease_groups",
        "disease_source_system",
        "disease_data_source",
        "disease_etl_package_id",
        "xref_ids_by_source",
        "clingen_gene_count",
        "clingen_relationship_count",
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
            "input_data": ["MONDO:0007254", "breast cancer"],
            "include_relationships": True,
            "include_xref_summary": True,
            "include_clingen_summary": True,
            "emit_not_found_rows": True,
        }

    def run(self) -> pa.Table:
        include_relationships = parse_bool(self.param("include_relationships"), True)
        include_xrefs = parse_bool(self.param("include_xref_summary"), True)
        include_clingen = parse_bool(self.param("include_clingen_summary"), True)
        emit_not_found = parse_bool(self.param("emit_not_found_rows"), True)

        self.resolve(self.param("input_data", required=True))

        # ClinGen's contribution, isolated from every other source's. The
        # gene count is distinct genes, not relationship rows: one gene
        # asserted through three lines of evidence is one gene.
        clingen = (
            """
            clingen_pairs AS (
                SELECT
                    r.entity_1_id AS entity_id,
                    r.entity_2_id AS other_id,
                    r.entity_2_group_id AS other_group_id
                FROM entity_relationships r
                JOIN resolved x ON x.entity_id = r.entity_1_id
                JOIN etl_data_sources ds ON ds.id = r.data_source_id
                WHERE lower(ds.name) = '""" + CLINGEN + """'
                UNION ALL
                SELECT
                    r.entity_2_id AS entity_id,
                    r.entity_1_id AS other_id,
                    r.entity_1_group_id AS other_group_id
                FROM entity_relationships r
                JOIN resolved x ON x.entity_id = r.entity_2_id
                JOIN etl_data_sources ds ON ds.id = r.data_source_id
                WHERE lower(ds.name) = '""" + CLINGEN + """'
                  AND r.entity_2_id <> r.entity_1_id
            ),
            clingen AS (
                SELECT
                    p.entity_id,
                    CAST(count(*) AS BIGINT) AS relationship_count,
                    CAST(count(DISTINCT CASE
                        WHEN lower(g.name) IN ('gene', 'genes') THEN p.other_id
                    END) AS BIGINT) AS gene_count
                FROM clingen_pairs p
                LEFT JOIN entity_groups g ON g.id = p.other_group_id
                GROUP BY p.entity_id
            )
            """
            if include_clingen
            else """
            clingen AS (
                SELECT
                    CAST(NULL AS BIGINT) AS entity_id,
                    CAST(NULL AS BIGINT) AS relationship_count,
                    CAST(NULL AS BIGINT) AS gene_count
                WHERE false
            )
            """
        )

        having = "" if emit_not_found else "WHERE r.entity_id IS NOT NULL"
        clingen_genes = (
            "coalesce(cg.gene_count, 0)" if include_clingen else "CAST(NULL AS BIGINT)"
        )
        clingen_rels = (
            "coalesce(cg.relationship_count, 0)"
            if include_clingen
            else "CAST(NULL AS BIGINT)"
        )

        return self.sql(
            f"""
            WITH core AS (
                SELECT
                    d.entity_id,
                    d.disease_id,
                    d.label,
                    d.description,
                    os.name AS omic_status
                FROM disease_masters d
                LEFT JOIN omic_status os ON os.id = d.omic_status_id
                WHERE d.entity_id IN (SELECT entity_id FROM resolved)
                QUALIFY row_number() OVER (
                    PARTITION BY d.entity_id ORDER BY d.id
                ) = 1
            ),
            groups AS (
                SELECT d.entity_id, list(DISTINCT dg.name ORDER BY dg.name) AS names
                FROM disease_masters d
                JOIN disease_group_memberships m ON m.disease_id = d.id
                JOIN disease_groups dg ON dg.id = m.group_id
                WHERE d.entity_id IN (SELECT entity_id FROM resolved)
                GROUP BY d.entity_id
            ),
            {provenance_cte("disease_masters")},
            {aliases_cte()},
            {xrefs_cte(include_xrefs)},
            {relationships_cte(include_relationships)},
            {clingen}
            SELECT
                r.input_value,
                r.matched_alias                AS input_matched_alias,
                r.entity_id,
                c.disease_id,
                coalesce(c.label, r.primary_name, r.matched_alias) AS disease_label,
                c.description                  AS disease_description,
                c.omic_status,
                coalesce(g.names, [])          AS disease_groups,
                p.source_system                AS disease_source_system,
                p.data_source                  AS disease_data_source,
                p.etl_package_id               AS disease_etl_package_id,
                coalesce(x.by_source, [])      AS xref_ids_by_source,
                {clingen_genes}                AS clingen_gene_count,
                {clingen_rels}                 AS clingen_relationship_count,
                coalesce(rel.by_group, [])     AS entity_relationships_by_group,
                coalesce(rel.total, 0)         AS total_entity_relationships,
                {other_aliases_expr(
                    "c.disease_id",
                    "c.label",
                    "r.primary_name",
                    "r.matched_alias",
                )}                             AS other_aliases,
                CASE
                    WHEN r.entity_id IS NULL THEN 'not_found'
                    WHEN c.entity_id IS NULL THEN 'partial'
                    ELSE 'ok'
                END                            AS status,
                nullif(concat_ws(' ',
                    CASE WHEN r.entity_id IS NULL
                         THEN 'Input not resolved to a Disease entity.' END,
                    CASE WHEN r.entity_id IS NOT NULL AND c.entity_id IS NULL
                         THEN 'Disease resolved but no disease_masters row found.' END
                ), '')                         AS note
            FROM resolved r
            LEFT JOIN core        c   ON c.entity_id  = r.entity_id
            LEFT JOIN groups      g   ON g.entity_id  = r.entity_id
            LEFT JOIN provenance  p   ON p.entity_id  = r.entity_id
            LEFT JOIN all_aliases a   ON a.entity_id  = r.entity_id
            LEFT JOIN xrefs       x   ON x.entity_id  = r.entity_id
            LEFT JOIN rels        rel ON rel.entity_id = r.entity_id
            LEFT JOIN clingen     cg  ON cg.entity_id = r.entity_id
            {having}
            ORDER BY r.input_value, r.entity_id
            """
        )
