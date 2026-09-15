"""
Relationships: which links the bundle holds for these entities.

Migrated from `entity_relationship_model` (ADR-004 §6, §2.12). Renamed
because it does not model anything — it expands a list of entities into
the relationship rows they participate in.

One row per (input entity, relationship). A relationship with an input on
both sides yields two rows, one anchored on each, unless
`deduplicate_pairs` collapses them.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

import pyarrow as pa

from biofilter.modules.report.reports._resolution import (
    ALIAS_KEY,
    ALIAS_RANK,
)
from biofilter.modules.report.reports.base_report import ReportBase

SCOPES = ("input_to_any", "between_inputs")


class ExpandEntityRelationshipReport(ReportBase):
    name = "expand_entity_relationship"
    description = (
        "Relationship rows for a list of entities: every link where an input "
        "appears on either side, with the related entity named. Scope controls "
        "whether the other side must also be an input."
    )

    requires = (
        "entities",
        "entity_aliases",
        "entity_groups",
        "entity_relationships",
        "entity_relationship_types",
    )

    COLUMNS = (
        "input_original",
        "input_matched_alias",
        "input_entity_id",
        "input_primary_name",
        "input_group_name",
        "match_side",
        "direction",
        "relationship_id",
        "relationship_type",
        "relationship_description",
        "related_entity_id",
        "related_primary_name",
        "related_group_name",
        "entity_1_id",
        "entity_1_primary_name",
        "entity_2_id",
        "entity_2_primary_name",
        "data_source_id",
        "etl_package_id",
        "observation",
    )

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.COLUMNS

    @classmethod
    def example_input(cls):
        return {
            "input_data": ["TP53", "BRCA1"],
            "input_entity_groups": ["Genes"],
            "output_entity_groups": ["Pathways", "Proteins"],
            "relationship_scope": "input_to_any",
        }

    # ------------------------------------------------------------------
    @staticmethod
    def _parse_scope(value: Any) -> str:
        scope = str(value or "input_to_any").strip().lower()
        if scope not in SCOPES:
            raise ValueError(f"relationship_scope must be one of {SCOPES}. Got: {value!r}")
        return scope

    @staticmethod
    def _as_list(value: Any) -> Optional[list[str]]:
        if value is None:
            return None
        values = value if isinstance(value, (list, tuple, set)) else [value]
        cleaned = [str(v).strip().lower() for v in values if str(v).strip()]
        return cleaned or None

    @staticmethod
    def _parse_bool(value: Any, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
        return default

    @staticmethod
    def _sql_list(values: list[str]) -> str:
        return ", ".join("'" + v.replace("'", "''") + "'" for v in values)

    # ------------------------------------------------------------------
    def run(self) -> pa.Table:
        values = self.resolve_input_list(
            self.param("input_data", required=True), param_name="input_data"
        )
        if not values:
            raise ValueError("input_data must contain at least one value.")

        scope = self._parse_scope(self.param("relationship_scope", "input_to_any"))
        input_groups = self._as_list(self.param("input_entity_groups"))
        output_groups = self._as_list(self.param("output_entity_groups"))
        rel_types = self._as_list(self.param("relationship_types"))
        emit_not_found = self._parse_bool(self.param("emit_not_found_rows"), True)

        # A relationship whose two sides are both inputs is reached twice,
        # once from each. Collapsing is the sensible default only when the
        # scope guarantees that happens.
        dedupe = self._parse_bool(
            self.param("deduplicate_pairs"), scope == "between_inputs"
        )

        self.register_input(values, name="input_terms", column="input_value")

        input_group_clause = (
            f"AND lower(g.name) IN ({self._sql_list(input_groups)})"
            if input_groups
            else ""
        )
        output_group_clause = (
            f"WHERE lower(related_group_name) IN ({self._sql_list(output_groups)})"
            if output_groups
            else ""
        )
        rel_type_clause = (
            f"AND lower(rt.code) IN ({self._sql_list(rel_types)})" if rel_types else ""
        )
        scope_clause = (
            "AND r.entity_2_id IN (SELECT entity_id FROM resolved)"
            if scope == "between_inputs"
            else ""
        )
        dedupe_clause = (
            """
            QUALIFY row_number() OVER (
                PARTITION BY
                    relationship_id,
                    least(input_entity_id, related_entity_id),
                    greatest(input_entity_id, related_entity_id),
                    relationship_type
                ORDER BY match_side
            ) = 1
            """
            if dedupe
            else ""
        )

        return self.sql(
            f"""
            WITH primary_alias AS (
                SELECT entity_id, alias_value
                FROM entity_aliases
                WHERE is_primary
                QUALIFY row_number() OVER (
                    PARTITION BY entity_id ORDER BY alias_value
                ) = 1
            ),
            resolved AS (
                SELECT
                    i.input_value,
                    a.entity_id,
                    a.alias_value AS matched_alias,
                    g.name        AS group_name
                FROM input_terms i
                JOIN entity_aliases a ON {ALIAS_KEY} = i.input_value_norm
                JOIN entities e ON e.id = a.entity_id
                LEFT JOIN entity_groups g ON g.id = e.group_id
                WHERE true {input_group_clause}
                QUALIFY row_number() OVER (
                    PARTITION BY i.input_value
                    ORDER BY {ALIAS_RANK}, a.alias_value, a.entity_id
                ) = 1
            ),
            -- Anchored on each side the input appears on. A relationship
            -- with an input at both ends is reached twice, deliberately:
            -- the two rows differ in direction, and `deduplicate_pairs`
            -- is what collapses them when that is not wanted.
            anchored AS (
                SELECT
                    x.input_value,
                    x.entity_id            AS input_entity_id,
                    r.entity_2_id          AS related_entity_id,
                    'entity_1'             AS match_side,
                    'input->related'       AS direction,
                    r.id                   AS relationship_id,
                    r.relationship_type_id,
                    r.entity_1_id,
                    r.entity_2_id,
                    r.data_source_id,
                    r.etl_package_id
                FROM resolved x
                JOIN entity_relationships r ON r.entity_1_id = x.entity_id
                WHERE true {scope_clause}
                UNION ALL
                SELECT
                    x.input_value,
                    x.entity_id            AS input_entity_id,
                    r.entity_1_id          AS related_entity_id,
                    'entity_2'             AS match_side,
                    'related->input'       AS direction,
                    r.id                   AS relationship_id,
                    r.relationship_type_id,
                    r.entity_1_id,
                    r.entity_2_id,
                    r.data_source_id,
                    r.etl_package_id
                FROM resolved x
                JOIN entity_relationships r ON r.entity_2_id = x.entity_id
                WHERE r.entity_2_id <> r.entity_1_id
                  {"AND r.entity_1_id IN (SELECT entity_id FROM resolved)"
                    if scope == "between_inputs" else ""}
            ),
            named AS (
                SELECT
                    a.*,
                    rt.code               AS relationship_type,
                    rt.description        AS relationship_description,
                    rel_p.alias_value     AS related_primary_name,
                    rel_g.name            AS related_group_name,
                    e1_p.alias_value      AS entity_1_primary_name,
                    e2_p.alias_value      AS entity_2_primary_name
                FROM anchored a
                LEFT JOIN entity_relationship_types rt
                       ON rt.id = a.relationship_type_id
                LEFT JOIN entities rel_e ON rel_e.id = a.related_entity_id
                LEFT JOIN entity_groups rel_g ON rel_g.id = rel_e.group_id
                LEFT JOIN primary_alias rel_p ON rel_p.entity_id = a.related_entity_id
                LEFT JOIN primary_alias e1_p ON e1_p.entity_id = a.entity_1_id
                LEFT JOIN primary_alias e2_p ON e2_p.entity_id = a.entity_2_id
                WHERE true {rel_type_clause.replace("rt.code", "rt.code")}
            ),
            kept AS (
                SELECT * FROM named
                {output_group_clause}
                {dedupe_clause}
            )
            SELECT
                k.input_value              AS input_original,
                x.matched_alias            AS input_matched_alias,
                k.input_entity_id,
                p.alias_value              AS input_primary_name,
                x.group_name               AS input_group_name,
                k.match_side,
                k.direction,
                k.relationship_id,
                k.relationship_type,
                k.relationship_description,
                k.related_entity_id,
                k.related_primary_name,
                k.related_group_name,
                k.entity_1_id,
                k.entity_1_primary_name,
                k.entity_2_id,
                k.entity_2_primary_name,
                k.data_source_id,
                k.etl_package_id,
                ''                         AS observation
            FROM kept k
            JOIN resolved x ON x.input_value = k.input_value
            LEFT JOIN primary_alias p ON p.entity_id = k.input_entity_id

            UNION ALL

            -- Resolved, but nothing came back for it. The relational
            -- version dropped these rows, so an input with no links in
            -- scope looked exactly like one that was never asked about.
            SELECT
                x.input_value, x.matched_alias, x.entity_id,
                p.alias_value, x.group_name,
                NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                'no relationships in scope'
            FROM resolved x
            LEFT JOIN primary_alias p ON p.entity_id = x.entity_id
            WHERE {"true" if emit_not_found else "false"}
              AND x.input_value NOT IN (SELECT input_value FROM kept)

            UNION ALL

            SELECT
                i.input_value, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                'not found'
            FROM input_terms i
            WHERE {"true" if emit_not_found else "false"}
              AND i.input_value NOT IN (SELECT input_value FROM resolved)

            ORDER BY input_original, relationship_type,
                     related_group_name, related_primary_name
            """
        )
