"""
Connecting genes through what they share.

The middle stage of both pairing reports: seed genes reach partner genes
through an entity that links them — a pathway, a disease, a protein —
and how many distinct such entities link a pair is the pair's support.

Stage 1 is not here. `pair_variants` places variants on genes and
`pair_genes` deliberately does not, so the two resolve their input
differently and only meet at the links.

`max_group_size` lives here because it is the parameter that decides both
the size and the meaning of an answer, and it would otherwise be written
twice and drift once. A pathway naming 2,615 genes links its members
while saying almost nothing about any of them.
"""

from __future__ import annotations

from typing import Any, Iterable, Sequence

MEMBERSHIPS = ("both", "either")

DEFAULT_MAX_GROUP_SIZE = 300
DEFAULT_GROUPS = ("Pathways",)

#: Group types that actually link genes in a bundle. `Gene Ontology` is
#: absent on purpose: the bundle carries 38,092 GO entities and no GO
#: relationships, so asking for it returns nothing and reads as a
#: finding rather than a gap.
KNOWN_GROUPS = ("Pathways", "Diseases", "Proteins", "Genes")


def sql_list(values: Sequence[Any]) -> str:
    """A SQL list of quoted literals."""
    return ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)


def resolve_group_types(con, raw: Any, default: Sequence[str] = DEFAULT_GROUPS) -> list[str]:
    """
    Which kinds of entity may link two genes, checked against the bundle.

    Validated rather than assumed: a misspelled group type would
    otherwise return nothing, which is indistinguishable from a real
    answer of "these genes share nothing".
    """
    values = raw if isinstance(raw, (list, tuple, set)) else [raw]
    wanted = [str(v).strip() for v in values if str(v).strip()]
    if raw is None:
        wanted = list(default)
    if not wanted:
        raise ValueError("group_types must name at least one entity group.")

    available = {
        row[0].lower(): row[0]
        for row in con.execute("SELECT DISTINCT name FROM entity_groups").fetchall()
        if row[0]
    }
    resolved, unknown = [], []
    for value in wanted:
        match = available.get(value.lower())
        (resolved.append(match) if match else unknown.append(value))
    if unknown:
        raise ValueError(
            f"Unknown group_types: {unknown}. This bundle has: "
            f"{sorted(available.values())}. Note that Gene Ontology carries "
            f"no relationships in this bundle, so it cannot link genes."
        )
    return resolved


def links_cte(group_types: Sequence[str], max_group_size: int) -> str:
    """
    Every (group, gene) link for the chosen group types, in both
    directions, with the groups too large to mean anything removed.

    The relationship is stored once and read from either end, so a gene
    can be `entity_1` or `entity_2`. Reading only one direction silently
    halves the graph.

    `data_source_id` travels with the link because the bundle has it:
    joining `etl_data_sources` names the curation that asserted it. The
    alternative — recovering Reactome from an `R-HSA-` prefix — works
    until a third source arrives and then fails without saying so.
    """
    chosen = sql_list(group_types)
    size_filter = (
        f"HAVING count(DISTINCT gene_id) <= {max_group_size}"
        if max_group_size > 0
        else ""
    )
    return f"""
        links_raw AS (
            SELECT r.entity_1_id AS group_id, r.entity_2_id AS gene_id,
                   r.data_source_id
            FROM entity_relationships r
            JOIN entities eg ON eg.id = r.entity_1_id
            JOIN entity_groups g ON g.id = eg.group_id
            JOIN entities ge ON ge.id = r.entity_2_id
            JOIN entity_groups gg ON gg.id = ge.group_id
            WHERE g.name IN ({chosen}) AND gg.name = 'Genes'
            UNION ALL
            SELECT r.entity_2_id, r.entity_1_id, r.data_source_id
            FROM entity_relationships r
            JOIN entities eg ON eg.id = r.entity_2_id
            JOIN entity_groups g ON g.id = eg.group_id
            JOIN entities ge ON ge.id = r.entity_1_id
            JOIN entity_groups gg ON gg.id = ge.group_id
            WHERE g.name IN ({chosen}) AND gg.name = 'Genes'
        ),
        links_distinct AS (
            SELECT DISTINCT group_id, gene_id, data_source_id
            FROM links_raw WHERE group_id <> gene_id
        ),
        -- A group naming half the genome links its members by saying
        -- nothing about them. This is where that is decided.
        sized AS (
            SELECT group_id FROM links_distinct GROUP BY 1 {size_filter}
        ),
        links AS (
            SELECT l.* FROM links_distinct l JOIN sized s ON s.group_id = l.group_id
        ),
        group_names AS (
            SELECT a.entity_id, a.alias_value AS name, g.name AS group_type
            FROM entity_aliases a
            JOIN entities e ON e.id = a.entity_id
            JOIN entity_groups g ON g.id = e.group_id
            WHERE a.is_primary
            QUALIFY row_number() OVER (
                PARTITION BY a.entity_id ORDER BY a.alias_value
            ) = 1
        ),
        source_names AS (
            SELECT id, name FROM etl_data_sources
        ),
        all_genes AS (
            SELECT DISTINCT gene_id FROM links
        )
    """


def gene_pairs_cte(
    *,
    side_2_source: str,
    min_support: int,
    min_sources: int = 1,
    seed_relation: str = "seed_genes",
) -> str:
    """
    Gene pairs, with the groups and the curations behind each.

    Side 1 is always a seed. Ordering the two by entity id instead would
    drop every pair whose seed happens to sort after its partner —
    silently, and for no reason but the order the ids were assigned in.

    `min_sources` is a stronger claim than `min_support`: two curations
    agreeing is not the same as one curation saying it twice.
    """
    sources_having = (
        f"AND count(DISTINCT p.data_source_id) >= {min_sources}"
        if min_sources > 1
        else ""
    )
    return f"""
        pair_links AS (
            SELECT l1.gene_id AS gene_1_id, l2.gene_id AS gene_2_id,
                   l1.group_id, l1.data_source_id
            FROM (SELECT DISTINCT gene_id FROM {seed_relation}) s1
            JOIN links l1 ON l1.gene_id = s1.gene_id
            JOIN links l2
              ON l2.group_id = l1.group_id AND l2.gene_id <> l1.gene_id
            JOIN {side_2_source} s2 ON s2.gene_id = l2.gene_id
        ),
        gene_pairs_raw AS (
            SELECT
                p.gene_1_id, p.gene_2_id,
                count(DISTINCT p.group_id) AS group_support_count,
                count(DISTINCT p.data_source_id) AS group_support_source_count,
                list_sort(list_distinct(list(gn.group_type))) AS group_support_types,
                list_sort(list_distinct(list(gn.name))) AS group_support_names,
                list_sort(list_distinct(list(sn.name))) AS group_support_sources
            FROM pair_links p
            LEFT JOIN group_names gn ON gn.entity_id = p.group_id
            LEFT JOIN source_names sn ON sn.id = p.data_source_id
            GROUP BY 1, 2
            HAVING count(DISTINCT p.group_id) >= {min_support} {sources_having}
        ),
        -- When both genes are seeds the pair arises twice, once from
        -- each side. Keep one, deterministically.
        gene_pairs AS (
            SELECT * FROM gene_pairs_raw
            QUALIFY row_number() OVER (
                PARTITION BY least(gene_1_id, gene_2_id),
                             greatest(gene_1_id, gene_2_id)
                ORDER BY (gene_1_id < gene_2_id) DESC
            ) = 1
        ),
        named_pairs AS (
            SELECT
                p.*,
                g1.symbol AS gene_1_symbol,
                g2.symbol AS gene_2_symbol,
                (p.gene_1_id IN (SELECT gene_id FROM {seed_relation}))
                    AS gene_1_from_input,
                (p.gene_2_id IN (SELECT gene_id FROM {seed_relation}))
                    AS gene_2_from_input
            FROM gene_pairs p
            LEFT JOIN gene_masters g1 ON g1.entity_id = p.gene_1_id
            LEFT JOIN gene_masters g2 ON g2.entity_id = p.gene_2_id
        )
    """


def group_filter_select(max_group_size: int, seed_relation: str = "seed_genes") -> str:
    """
    What the size cut removed, over the same CTEs the answer uses.

    Without it an empty result reads as "these genes share no biology",
    when the truth can be "the only things linking them are pathways of
    1,200 genes, which you asked to exclude".
    """
    size_expr = f"sz.n <= {max_group_size}" if max_group_size > 0 else "true"
    return f"""
        SELECT
            count(DISTINCT l.group_id) AS touching_input,
            count(DISTINCT CASE WHEN {size_expr} THEN l.group_id END) AS kept,
            min(CASE WHEN NOT ({size_expr}) THEN sz.n END) AS smallest_excluded,
            max(sz.n) AS largest_seen
        FROM links_distinct l
        JOIN (SELECT DISTINCT gene_id FROM {seed_relation}) s ON s.gene_id = l.gene_id
        JOIN (
            SELECT group_id, count(DISTINCT gene_id) AS n
            FROM links_distinct GROUP BY 1
        ) sz ON sz.group_id = l.group_id
    """


def group_filter_block(stats: dict[str, Any], max_group_size: int) -> dict[str, Any]:
    """The provenance entry `group_filter_select` feeds."""
    touching = stats.get("touching_input") or 0
    kept = stats.get("kept") or 0
    excluded = touching - kept
    return {
        "max_group_size": max_group_size or None,
        "groups_touching_input": touching,
        "groups_kept": kept,
        "groups_excluded_by_size": excluded,
        "smallest_excluded": stats.get("smallest_excluded"),
        "largest_seen": stats.get("largest_seen"),
        "means": (
            f"{excluded} of the {touching} groups that reach these genes name "
            f"more than {max_group_size} genes each and were excluded. The "
            f"smallest one excluded has {stats.get('smallest_excluded')} genes. "
            f"If the result is empty or thin, raising max_group_size is what "
            f"changes it."
            if excluded
            else "No group was excluded for its size."
        ),
    }


def parse_bool(value: Any, default: bool) -> bool:
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


def choice(name: str, value: Any, allowed: tuple[str, ...], default: str) -> str:
    resolved = str(value if value is not None else default).strip().lower()
    if resolved not in allowed:
        raise ValueError(f"{name} must be one of {allowed}. Got: {resolved!r}")
    return resolved


def unique_items(values: Iterable[Any]) -> list[str]:
    """Trimmed, de-duplicated, order preserved."""
    seen, out = set(), []
    for value in values:
        text = str(value).strip()
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out
