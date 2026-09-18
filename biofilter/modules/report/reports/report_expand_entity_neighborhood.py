"""
Neighbourhood: what sits one hop from each of these entities.

Migrated from `entity_neighborhood_summary` (ADR-004 §6, §2.12). It takes
a heterogeneous list — genes, diseases, proteins, in any mix — resolves
each to entities, and summarises what each one is connected to: how many
neighbours, of which kinds, and which ones.

The relational version had a defect worth naming, because it returned
wrong answers without failing. It treated any prefix before a colon as a
type hint, so `GO:0006915` became the term `0006915` — which resolved to
a **disease**. Every cross-reference code in the bundle was affected:
MONDO:, HGNC:, DOID:, EFO:. A prefix is a hint here only when it names an
entity group, and `go` is excluded from the hint vocabulary for exactly
this reason (`_resolution.py`).
"""

from __future__ import annotations

from typing import Any, Sequence

import pyarrow as pa

from biofilter.modules.report.reports._resolution import (
    ALIAS_KEY,
    ALIAS_RANK,
    DEFAULT_SIMILARITY_THRESHOLD,
    GROUP_SYNONYMS,
    hint_to_group,
    match_clause,
    split_type_hint,
    validate_match_mode,
)
from biofilter.modules.report.reports.base_report import ReportBase

DEFAULT_NEIGHBOURS_PER_TYPE = 50
DEFAULT_ALIASES_TOP_N = 20


class ExpandEntityNeighborhoodReport(ReportBase):
    name = "expand_entity_neighborhood"
    description = (
        "One-hop neighbourhood for a heterogeneous list of entities. Resolves "
        "genes, diseases, proteins and the rest — with optional 'gene:' style "
        "hints — and reports degree, degree by neighbour type, and the "
        "neighbours themselves."
    )

    requires = ("entities", "entity_aliases", "entity_groups", "entity_relationships")

    COLUMNS = (
        "input_value",
        "input_type_hint",
        "match_mode",
        "entity_id",
        "entity_group",
        "matched_alias",
        "primary_name",
        "similarity_score",
        "alias_count",
        "aliases_top",
        "degree_total",
        "neighbors_by_type",
        "status",
        "note",
    )

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.COLUMNS

    @classmethod
    def example_input(cls):
        return {
            "input_data": ["gene:BRCA1", "disease:breast cancer", "APOE"],
            "match_mode": "exact",
            "neighbors_top_n_per_type": DEFAULT_NEIGHBOURS_PER_TYPE,
            "aliases_top_n": DEFAULT_ALIASES_TOP_N,
            "emit_not_found_rows": True,
        }

    # ------------------------------------------------------------------
    def _known_hints(self) -> set[str]:
        """
        What counts as a type hint: the bundle's own group names, plus
        convenient spellings. Reading the groups from the bundle is what
        keeps this from going stale — the relational version hardcoded
        `GO Terms`, a group name that does not exist here, so `go:` had
        silently resolved nothing even before the parsing defect.
        """
        rows = self.con.execute("SELECT lower(name) FROM entity_groups").fetchall()
        return {r[0] for r in rows} | set(GROUP_SYNONYMS)

    def run(self) -> pa.Table:
        raw = self.param("input_data", required=True)
        if raw is None:
            raw = self.param("items", required=True)

        match_mode = validate_match_mode(self.param("match_mode", "exact"))
        threshold = float(
            self.param("similarity_threshold", DEFAULT_SIMILARITY_THRESHOLD)
            or DEFAULT_SIMILARITY_THRESHOLD
        )
        top_n = int(
            self.param("neighbors_top_n_per_type", DEFAULT_NEIGHBOURS_PER_TYPE) or 0
        )
        aliases_top_n = int(self.param("aliases_top_n", DEFAULT_ALIASES_TOP_N) or 0)
        emit_not_found = self._parse_bool(self.param("emit_not_found_rows"), True)

        values = self.resolve_input_list(raw, param_name="input_data")
        if not values:
            raise ValueError("input_data must contain at least one value.")

        self._register_typed_input(values)

        join_on, score = match_clause(match_mode, threshold)
        having = "" if emit_not_found else "WHERE r.entity_id IS NOT NULL"
        neighbour_names = (
            f"list_sort(list_distinct(list(neighbour_name)))[1:{top_n}]"
            if top_n > 0
            else "list_sort(list_distinct(list(neighbour_name)))"
        )
        alias_list = (
            f"list_sort(list_distinct(list(alias_value)))[1:{aliases_top_n}]"
            if aliases_top_n > 0
            else "list_sort(list_distinct(list(alias_value)))"
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
            -- One row per input, resolved to its best entity. A hint
            -- restricts the search to that group; without one, any group.
            resolved AS (
                SELECT
                    i.input_value,
                    i.type_hint,
                    a.entity_id,
                    a.alias_value AS matched_alias,
                    {score} AS similarity_score
                FROM typed_input i
                JOIN entity_aliases a ON {join_on}
                JOIN entities e ON e.id = a.entity_id
                JOIN entity_groups g ON g.id = e.group_id
                WHERE i.group_name IS NULL OR lower(g.name) = i.group_name
                QUALIFY row_number() OVER (
                    PARTITION BY i.input_value
                    ORDER BY {ALIAS_RANK}, a.alias_value, a.entity_id
                ) = 1
            ),
            aliases AS (
                SELECT
                    entity_id,
                    CAST(count(DISTINCT alias_value) AS BIGINT) AS alias_count,
                    {alias_list} AS aliases_top
                FROM entity_aliases
                WHERE entity_id IN (SELECT entity_id FROM resolved)
                GROUP BY entity_id
            ),
            -- Both directions: an entity is a neighbour whether it sits on
            -- the left or the right of a relationship.
            hops AS (
                SELECT r.entity_1_id AS entity_id, r.entity_2_id AS neighbour_id
                FROM entity_relationships r
                JOIN resolved x ON x.entity_id = r.entity_1_id
                UNION ALL
                SELECT r.entity_2_id AS entity_id, r.entity_1_id AS neighbour_id
                FROM entity_relationships r
                JOIN resolved x ON x.entity_id = r.entity_2_id
                WHERE r.entity_2_id <> r.entity_1_id
            ),
            hop_rows AS (
                SELECT
                    h.entity_id,
                    coalesce(g.name, 'Unknown') AS group_name,
                    h.neighbour_id,
                    coalesce(p.alias_value, 'ENTITY:' || CAST(h.neighbour_id AS VARCHAR))
                        AS neighbour_name
                FROM hops h
                JOIN entities e ON e.id = h.neighbour_id
                LEFT JOIN entity_groups g ON g.id = e.group_id
                LEFT JOIN primary_alias p ON p.entity_id = h.neighbour_id
            ),
            by_type AS (
                SELECT
                    entity_id,
                    group_name,
                    CAST(count(*) AS BIGINT) AS n,
                    {neighbour_names} AS names
                FROM hop_rows
                GROUP BY 1, 2
            ),
            -- One nested column instead of one column per entity group.
            -- The relational version added a column per group present in
            -- the bundle, so the result's schema depended on the data:
            -- 29 columns here, 14 of them unknowable before running it.
            neighbourhood AS (
                SELECT
                    entity_id,
                    CAST(sum(n) AS BIGINT) AS degree_total,
                    list(struct_pack(group_name := group_name, count := n, names := names)
                         ORDER BY n DESC, group_name) AS neighbors_by_type
                FROM by_type
                GROUP BY entity_id
            )
            SELECT
                i.input_value,
                i.type_hint                     AS input_type_hint,
                '{match_mode}'                  AS match_mode,
                r.entity_id,
                g.name                          AS entity_group,
                r.matched_alias,
                p.alias_value                   AS primary_name,
                r.similarity_score,
                coalesce(al.alias_count, 0)     AS alias_count,
                coalesce(al.aliases_top, [])    AS aliases_top,
                coalesce(n.degree_total, 0)     AS degree_total,
                coalesce(n.neighbors_by_type, []) AS neighbors_by_type,
                CASE WHEN r.entity_id IS NULL THEN 'not_found' ELSE 'ok' END AS status,
                nullif(concat_ws(' ',
                    CASE WHEN r.entity_id IS NULL AND i.type_hint IS NOT NULL
                         THEN 'Not resolved to a ' || i.group_name || ' entity.' END,
                    CASE WHEN r.entity_id IS NULL AND i.type_hint IS NULL
                         THEN 'Input not resolved to any entity.' END,
                    CASE WHEN r.entity_id IS NOT NULL AND n.entity_id IS NULL
                         THEN 'Resolved, but nothing is linked to it in this bundle.' END
                ), '')                          AS note
            FROM typed_input i
            LEFT JOIN resolved      r  ON r.input_value = i.input_value
            LEFT JOIN entities      e  ON e.id = r.entity_id
            LEFT JOIN entity_groups g  ON g.id = e.group_id
            LEFT JOIN primary_alias p  ON p.entity_id = r.entity_id
            LEFT JOIN aliases       al ON al.entity_id = r.entity_id
            LEFT JOIN neighbourhood n  ON n.entity_id = r.entity_id
            {having}
            ORDER BY i.input_value
            """
        )

    # ------------------------------------------------------------------
    def _register_typed_input(self, values: list[str]) -> str:
        """
        Register the inputs, each split into a hint and a term.

        Splitting in Python and registering the result keeps the SQL free
        of string surgery, and keeps the hint vocabulary in one place.
        """
        known = self._known_hints()
        originals, hints, groups, terms, norms = [], [], [], [], []
        for value in values:
            hint, term = split_type_hint(value, known)
            originals.append(str(value).strip())
            hints.append(hint)
            groups.append(hint_to_group(hint))
            terms.append(term)
            norms.append(term.lower())

        self.con.register(
            "typed_input",
            pa.table(
                {
                    "input_value": pa.array(originals, pa.string()),
                    "type_hint": pa.array(hints, pa.string()),
                    "group_name": pa.array(groups, pa.string()),
                    "term": pa.array(terms, pa.string()),
                    "input_value_norm": pa.array(norms, pa.string()),
                }
            ),
        )
        return "typed_input"

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
