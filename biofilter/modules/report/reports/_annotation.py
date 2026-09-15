"""
What the annotation reports have in common.

`annotation_master_gene` was written alone, and its shape turned out to
be the shape of all of them: resolve an input to an entity of some group,
then hang facts off that entity — its master row, its cross-references,
who it is related to, what else it is called.

This holds the parts that are identical across them. Each report still
writes its own query; what it inherits is the resolution step and a few
CTEs it would otherwise retype. ADR-004 §7 left composition open for the
pilots to answer — this is the answer, and it is deliberately small:
shared SQL text and one resolution method, not a framework.
"""

from __future__ import annotations

from typing import Any, Sequence

import pyarrow as pa

from biofilter.modules.report.reports.base_report import ReportBase

ALL_TOKEN = "__ALL__"

#: Which alias wins when an input matches several.
ALIAS_RANK = """
    CASE
        WHEN a.is_primary THEN 0
        WHEN lower(a.alias_type) IN ('symbol', 'preferred') THEN 1
        WHEN lower(a.alias_type) = 'code' THEN 2
        WHEN lower(a.alias_type) IN ('synonym', 'name') THEN 3
        ELSE 4
    END
"""

#: Tables every annotation report reads.
BASE_REQUIRES = (
    "entities",
    "entity_aliases",
    "entity_groups",
    "entity_relationships",
    "etl_data_sources",
    "etl_source_systems",
)


def parse_bool(value: Any, default: bool) -> bool:
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


def relationships_cte(include: bool) -> str:
    """
    Relationship counts per related entity group, both directions.

    When switched off it returns no rows rather than disappearing, so the
    shape of the result never changes — only what is in it.
    """
    if not include:
        return """
            rels AS (
                SELECT
                    CAST(NULL AS BIGINT) AS entity_id,
                    CAST([] AS STRUCT(group_name VARCHAR, count BIGINT)[]) AS by_group,
                    CAST(NULL AS BIGINT) AS total
                WHERE false
            )
        """
    return """
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


def aliases_cte() -> str:
    """Every alias an entity has, for subtracting the canonical ones from."""
    return """
        all_aliases AS (
            SELECT entity_id, list(DISTINCT trim(alias_value)) AS values
            FROM entity_aliases
            WHERE entity_id IN (SELECT entity_id FROM resolved)
              AND trim(coalesce(alias_value, '')) <> ''
            GROUP BY entity_id
        )
    """


def xrefs_cte(include: bool = True) -> str:
    """Cross-reference codes grouped by the source that issued them."""
    if not include:
        return """
            xrefs AS (
                SELECT
                    CAST(NULL AS BIGINT) AS entity_id,
                    CAST([] AS STRUCT(source VARCHAR, ids VARCHAR[])[]) AS by_source
                WHERE false
            )
        """
    return """
        xref_rows AS (
            SELECT
                entity_id,
                coalesce(nullif(trim(xref_source), ''), 'UNKNOWN') AS source,
                list(DISTINCT trim(alias_value) ORDER BY trim(alias_value)) AS ids
            FROM entity_aliases
            WHERE entity_id IN (SELECT entity_id FROM resolved)
              AND lower(alias_type) = 'code'
              AND trim(coalesce(alias_value, '')) <> ''
            GROUP BY 1, 2
        ),
        xrefs AS (
            SELECT
                entity_id,
                list(struct_pack(source := source, ids := ids) ORDER BY source)
                    AS by_source
            FROM xref_rows
            GROUP BY entity_id
        )
    """


def provenance_cte(master_table: str, key: str = "entity_id") -> str:
    """
    Which source system, data source and ETL package produced a row.

    Every master table carries `data_source_id` and `etl_package_id`, so
    the join is the same shape each time; only the table changes.
    """
    return f"""
        provenance AS (
            SELECT
                m.{key} AS entity_id,
                ss.name AS source_system,
                ds.name AS data_source,
                m.etl_package_id
            FROM {master_table} m
            LEFT JOIN etl_data_sources ds ON ds.id = m.data_source_id
            LEFT JOIN etl_source_systems ss ON ss.id = ds.source_system_id
            WHERE m.{key} IN (SELECT entity_id FROM resolved)
            QUALIFY row_number() OVER (PARTITION BY m.{key} ORDER BY m.id) = 1
        )
    """


def other_aliases_expr(*canonical: str) -> str:
    """
    Aliases minus the ones already shown in their own columns.

    Repeating a gene's symbol inside `other_aliases` is noise; the column
    is for what the other columns do not already say.
    """
    values = ",\n                            ".join(
        f"coalesce({c}, '')" for c in canonical
    )
    return f"""
        coalesce(
            list_sort(list_filter(
                a.values,
                v -> v NOT IN [
                            {values}
                ]
            )), []
        )
    """


class AnnotationReportBase(ReportBase):
    """
    An annotation report over one entity group.

    Subclasses set `entity_groups`, declare their `requires`, and write
    the annotating SELECT. Resolution — input to entity, or every entity
    of the group — is inherited.
    """

    #: Entity group names this report annotates, lowercased.
    entity_groups: tuple[str, ...] = ()

    # ------------------------------------------------------------------
    @staticmethod
    def is_all(raw: Any) -> bool:
        if isinstance(raw, str):
            return raw.strip().upper() == ALL_TOKEN
        if isinstance(raw, (list, tuple, set)):
            items = [str(v).strip() for v in raw if str(v).strip()]
            return len(items) == 1 and items[0].upper() == ALL_TOKEN
        return False

    def _group_filter(self) -> str:
        names = ", ".join(f"'{g}'" for g in self.entity_groups)
        return f"lower(g.name) IN ({names})"

    def _scaffold(self) -> str:
        return f"""
            group_entities AS (
                SELECT e.id AS entity_id
                FROM entities e
                JOIN entity_groups g ON g.id = e.group_id
                WHERE {self._group_filter()}
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

    def resolve(self, raw_input: Any) -> None:
        """
        Build the `resolved` temp table this report's query joins against.

        One row per input value, whether or not it matched — a report
        that drops what it could not resolve leaves no way to tell
        "absent from the bundle" from "never asked for".
        """
        if self.is_all(raw_input):
            self._resolve_all()
            return

        values = self.resolve_input_list(raw_input, param_name="input_data")
        if not values:
            raise ValueError("input_data must contain at least one value.")

        self.register_input(values, name="input_terms", column="input_value")
        self.con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE resolved AS
            WITH {self._scaffold()},
            candidates AS (
                SELECT
                    i.input_value,
                    a.entity_id,
                    a.alias_value AS matched_alias,
                    {ALIAS_RANK} AS alias_rank
                FROM input_terms i
                JOIN entity_aliases a
                  ON lower(coalesce(a.alias_norm, a.alias_value)) = i.input_value_norm
                JOIN group_entities ge ON ge.entity_id = a.entity_id
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
            FROM input_terms i
            LEFT JOIN candidates c ON c.input_value = i.input_value
            LEFT JOIN primary_alias p ON p.entity_id = c.entity_id
            """
        )

    def _resolve_all(self) -> None:
        self.con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE resolved AS
            WITH {self._scaffold()}
            SELECT
                coalesce(p.alias_value, 'ENTITY:' || CAST(ge.entity_id AS VARCHAR))
                    AS input_value,
                ge.entity_id,
                coalesce(p.alias_value, 'ENTITY:' || CAST(ge.entity_id AS VARCHAR))
                    AS matched_alias,
                p.alias_value AS primary_name
            FROM group_entities ge
            LEFT JOIN primary_alias p ON p.entity_id = ge.entity_id
            ORDER BY ge.entity_id
            """
        )
        if self.con.execute("SELECT count(*) FROM resolved").fetchone()[0] == 0:
            raise ValueError(
                f"This bundle carries no {'/'.join(self.entity_groups)} entities "
                f"for input_data='{ALL_TOKEN}'."
            )
