"""
Resolution: which entities a list of names maps to, and how cleanly.

Migrated from the relational layer, where it was called `entity_filter`
(ADR-004 §6, §2.12). The old name described the wrong thing — it filters
nothing. A filtering report reduces a set; this one expands, turning one
input into every entity that answers to it, so ambiguity shows up as
extra rows rather than being resolved silently.

It is the step before everything else: which of your inputs the bundle
knows, which are ambiguous, and which it has never heard of.

Fuzzy matching moved into the engine. The relational version pulled every
alias in the bundle into Python and scored it with `rapidfuzz`; over 912
thousand aliases that is the pattern this module exists to remove, and it
made the mode unusable wherever the optional dependency was missing.
DuckDB scores it in the join instead. See `similarity_score` in the guide
for what changed.
"""

from __future__ import annotations

from typing import Any, Sequence

import pyarrow as pa

from biofilter.modules.report.reports._resolution import (
    ALIAS_KEY,
    DEFAULT_SIMILARITY_THRESHOLD,
    MATCH_MODES,
    match_clause,
    validate_match_mode,
)
from biofilter.modules.report.reports.base_report import ReportBase


class ResolveEntityReport(ReportBase):
    name = "resolve_entity"
    description = (
        "Resolve a list of names to entities, with conflict and status flags. "
        "match_mode: 'exact' (default), 'like' (substring either way), or "
        "'fuzzy' (Jaro-Winkler similarity above a threshold)."
    )

    requires = ("entities", "entity_aliases", "entity_groups")

    COLUMNS = (
        "input_original",
        "input",
        "is_primary",
        "entity_id",
        "primary_name",
        "group_id",
        "group_name",
        "has_conflict",
        "is_active",
        "is_deactive",
        "data_source_id",
        "similarity_score",
        "observation",
    )

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.COLUMNS

    @classmethod
    def example_input(cls):
        return {
            "input_data": ["TP53", "BRCA1", "NOT_A_GENE"],
            "match_mode": "exact",
            "group_filter": None,
            "similarity_threshold": DEFAULT_SIMILARITY_THRESHOLD,
        }

    # ------------------------------------------------------------------
    def run(self) -> pa.Table:
        values = self.resolve_input_list(
            self.param("input_data", required=True), param_name="input_data"
        )
        if not values:
            raise ValueError("input_data must contain at least one value.")

        match_mode = validate_match_mode(self.param("match_mode", "exact"))

        threshold = float(
            self.param("similarity_threshold", DEFAULT_SIMILARITY_THRESHOLD)
            or DEFAULT_SIMILARITY_THRESHOLD
        )
        group_filter = self.param("group_filter")

        self.register_input(values, name="input_names", column="input_value")

        alias_key = ALIAS_KEY
        join_on, score = match_clause(match_mode, threshold)
        group_clause = (
            "AND lower(g.name) = lower(?)" if group_filter else ""
        )
        params = [group_filter] if group_filter else None

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
            matches AS (
                SELECT
                    i.input_value        AS input_original,
                    a.alias_value        AS input,
                    a.is_primary,
                    e.id                 AS entity_id,
                    p.alias_value        AS primary_name,
                    e.group_id,
                    g.name               AS group_name,
                    e.has_conflict,
                    e.is_active,
                    a.data_source_id,
                    {alias_key}          AS matched_key,
                    {score}              AS similarity_score
                FROM input_names i
                JOIN entity_aliases a ON {join_on}
                JOIN entities e ON e.id = a.entity_id
                LEFT JOIN primary_alias p ON p.entity_id = e.id
                LEFT JOIN entity_groups g ON g.id = e.group_id
                WHERE true {group_clause}
            ),
            counted AS (
                SELECT
                    *,
                    -- Ambiguity is a property of the NAME, not of the
                    -- search: two entities answering to the same alias.
                    -- Partitioning by the input instead would flag every
                    -- row of a broad `like` search, which the row count
                    -- already tells you.
                    count(DISTINCT entity_id) OVER (PARTITION BY matched_key)
                        AS entities_for_key
                FROM matches
            )
            SELECT
                input_original,
                input,
                is_primary,
                entity_id,
                primary_name,
                group_id,
                group_name,
                has_conflict,
                is_active,
                CASE WHEN is_active IS NULL THEN NULL ELSE NOT is_active END
                                          AS is_deactive,
                data_source_id,
                similarity_score,
                CASE WHEN entities_for_key > 1 THEN 'multiple matches' ELSE '' END
                                          AS observation
            FROM counted

            UNION ALL

            -- Inputs that matched nothing stay in the result. Dropping
            -- them would leave no way to tell "the bundle does not know
            -- this name" from "you did not ask about it".
            SELECT
                i.input_value AS input_original,
                i.input_value AS input,
                CAST(NULL AS BOOLEAN) AS is_primary,
                CAST(NULL AS BIGINT)  AS entity_id,
                CAST(NULL AS VARCHAR) AS primary_name,
                CAST(NULL AS BIGINT)  AS group_id,
                CAST(NULL AS VARCHAR) AS group_name,
                CAST(NULL AS BOOLEAN) AS has_conflict,
                CAST(NULL AS BOOLEAN) AS is_active,
                CAST(NULL AS BOOLEAN) AS is_deactive,
                CAST(NULL AS BIGINT)  AS data_source_id,
                CAST(NULL AS DOUBLE)  AS similarity_score,
                'not found'           AS observation
            FROM input_names i
            WHERE i.input_value NOT IN (SELECT input_original FROM matches)

            ORDER BY input_original, primary_name, input
            """,
            params,
        )
