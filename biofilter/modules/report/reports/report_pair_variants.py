"""
Candidate variant pairs, linked by shared biology.

Replaces six reports that were one pipeline written in three eras:
`variant_gene_location_model` (stage 1 alone), `variant_single_gene_annotation`
(stages 1-2), `snp_snp_model` and `variant_modeling` (all three stages,
differing only in whether both sides had to come from the input).

Three stages:

1. **Place.** Each input is read as a gene name or as a variant (rsID,
   `chr:pos`, `chr:pos:ref:alt`). A variant is placed on the genes whose
   build-38 range contains it, optionally widened by `window_bp`.
2. **Connect.** Seed genes reach partner genes through a shared entity —
   a pathway, a disease, a protein. That entity is the *group*, and how
   many distinct groups link a pair is the pair's support.
3. **Pair.** Gene pairs become variant pairs.

`max_group_size` is the parameter that matters most, and it is not a
performance knob with a quality side effect — it is the other way round.
A pathway naming 2,615 genes, or a protein interacting with 5,338, links
its members to each other by saying almost nothing about any of them.
Measured on the current bundle, the groups generate this many gene pairs
between them:

| `max_group_size` | Pathways | Proteins | Diseases |
| --- | --- | --- | --- |
| 100 | 1.6 M (86% of groups kept) | 11.6 M (72%) | 14.9 K (100%) |
| **300** | **6.9 M (98%)** | 76.9 M (93%) | 56.2 K (100%) |
| no limit | 37.1 M | 469.8 M | 56.2 K |

300 is the default because it keeps 98% of pathways while cutting the
pairs they generate more than fivefold, and never touches diseases — the
largest disease in the bundle names 232 genes. It is also the difference
between 0.27 s and 30 s on a 300-variant input against protein groups.

Gene Ontology is not offered as a group type. The bundle carries 38,092
GO entities and **zero** GO relationships, so asking for it would return
nothing and look like a finding.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

import pyarrow as pa

from biofilter.modules.report.reports import _variants as _v
from biofilter.modules.report.reports._resolution import ALIAS_KEY
from biofilter.modules.report.reports.base_report import ReportBase

MEMBERSHIPS = ("both", "either")
GRAINS = ("variant_pairs", "gene_pairs")

DEFAULT_BUILD = 38
DEFAULT_MAX_GROUP_SIZE = 300
DEFAULT_MAX_PAIRS = 1_000_000
DEFAULT_MAX_VARIANTS_PER_GENE = 100
DEFAULT_GROUPS = ("Pathways",)

#: Group types that actually link genes in a bundle. `Gene Ontology` is
#: absent on purpose — see the module docstring.
KNOWN_GROUPS = ("Pathways", "Diseases", "Proteins", "Genes")


class PairVariantsReport(ReportBase):
    name = "pair_variants"
    description = (
        "Candidate variant x variant pairs whose genes share biology: places each "
        "input on its genes, connects those genes through shared pathways, diseases "
        "or proteins, and returns the pairs with the support behind them."
    )

    requires = (
        "entities",
        "entity_aliases",
        "entity_groups",
        "entity_locations",
        "entity_relationships",
        "gene_masters",
        "variant_masters",
    )

    optional = ("variant_rsid",)

    PAIR_COLUMNS = (
        "input_1",
        "variant_1_key",
        "variant_1_rsid",
        "variant_1_chromosome",
        "variant_1_position",
        "gene_1_id",
        "gene_1_symbol",
        "variant_1_from_input",
        "input_2",
        "variant_2_key",
        "variant_2_rsid",
        "variant_2_chromosome",
        "variant_2_position",
        "gene_2_id",
        "gene_2_symbol",
        "variant_2_from_input",
        "group_support_count",
        "group_support_types",
        "group_support_names",
        "membership",
    )

    GENE_COLUMNS = (
        "input_1",
        "gene_1_id",
        "gene_1_symbol",
        "gene_1_from_input",
        "gene_2_id",
        "gene_2_symbol",
        "gene_2_from_input",
        "group_support_count",
        "group_support_types",
        "group_support_names",
        "membership",
    )

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.PAIR_COLUMNS

    @classmethod
    def example_input(cls):
        return {
            "input_data": ["CHEK2", "SMARCB1", "NF2"],
            "membership": "both",
            "group_types": ["Pathways"],
            "max_group_size": 300,
            "window_bp": 0,
        }

    # ------------------------------------------------------------------
    # Parameters
    # ------------------------------------------------------------------
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

    def _int_param(self, name: str, default: int) -> int:
        """An explicit zero is an answer, not an absence."""
        value = self.param(name, default)
        if value is None or (isinstance(value, str) and not value.strip()):
            return default
        return int(value)

    @staticmethod
    def _sql_list(values: Sequence[str]) -> str:
        return ", ".join("'" + str(v).replace("'", "''") + "'" for v in values)

    def _choice(self, name: str, allowed: tuple[str, ...], default: str) -> str:
        value = str(self.param(name, default) or default).strip().lower()
        if value not in allowed:
            raise ValueError(f"{name} must be one of {allowed}. Got: {value!r}")
        return value

    def _group_types(self) -> list[str]:
        """
        Which kinds of entity may link two genes.

        Validated against the bundle rather than a hardcoded list: a
        misspelled group type would otherwise return nothing and read as
        a finding.
        """
        raw = self.param("group_types", list(DEFAULT_GROUPS))
        values = raw if isinstance(raw, (list, tuple, set)) else [raw]
        wanted = [str(v).strip() for v in values if str(v).strip()]
        if not wanted:
            raise ValueError("group_types must name at least one entity group.")

        available = {
            row[0].lower(): row[0]
            for row in self.con.execute(
                "SELECT DISTINCT name FROM entity_groups"
            ).fetchall()
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

    # ------------------------------------------------------------------
    def run(self) -> pa.Table:
        inputs = self.resolve_input_list(
            self.param("input_data", required=True), param_name="input_data"
        )
        if not inputs:
            raise ValueError("input_data must contain at least one gene or variant.")

        membership = self._choice("membership", MEMBERSHIPS, "both")
        grain = self._choice("output_grain", GRAINS, "variant_pairs")
        group_types = self._group_types()
        build = self._int_param("build", DEFAULT_BUILD)
        window = self._int_param("window_bp", 0)
        max_group_size = self._int_param("max_group_size", DEFAULT_MAX_GROUP_SIZE)
        min_support = self._int_param("min_group_support", 1)
        max_pairs = self._int_param("max_pairs", DEFAULT_MAX_PAIRS)
        max_per_gene = self._int_param(
            "max_variants_per_gene", DEFAULT_MAX_VARIANTS_PER_GENE
        )

        for name, value in (
            ("window_bp", window),
            ("max_group_size", max_group_size),
            ("max_pairs", max_pairs),
            ("max_variants_per_gene", max_per_gene),
        ):
            if value < 0:
                raise ValueError(f"{name} must be 0 or positive. Got: {value}.")
        if min_support < 1:
            raise ValueError(f"min_group_support must be at least 1. Got: {min_support}.")

        self.con.register("pv_input", _v.input_table(inputs))

        self.note_provenance(
            "pairing",
            {
                "membership": membership,
                "output_grain": grain,
                "group_types": group_types,
                "max_group_size": max_group_size or None,
                "min_group_support": min_support,
                "max_variants_per_gene": max_per_gene or None,
                "variants_kept_per_gene": (
                    "the most common by joint allele frequency; a variant "
                    "named in the input is never dropped"
                    if max_per_gene > 0
                    else "all of them"
                ),
                "means": (
                    "Both members come from the input."
                    if membership == "both"
                    else "One member comes from the input; the other is any "
                    "variant in a gene the input reaches through a shared group."
                ),
            },
        )

        common = self._query(
            membership=membership,
            grain=grain,
            group_types=group_types,
            build=build,
            window=window,
            max_group_size=max_group_size,
            min_support=min_support,
            max_pairs=max_pairs,
            max_per_gene=max_per_gene,
            diagnostic_only=True,
        )
        self._note_group_filter(common, max_group_size)

        table = self.sql(self._query(
            membership=membership,
            grain=grain,
            group_types=group_types,
            build=build,
            window=window,
            max_group_size=max_group_size,
            min_support=min_support,
            max_pairs=max_pairs,
            max_per_gene=max_per_gene,
        ))
        self._note_truncation(table, max_pairs)
        return table

    # ------------------------------------------------------------------
    def _query(
        self,
        *,
        membership: str,
        grain: str,
        group_types: list[str],
        build: int,
        window: int,
        max_group_size: int,
        min_support: int,
        max_pairs: int,
        max_per_gene: int,
        diagnostic_only: bool = False,
    ) -> str:
        has_rsid = self.bundle.has("variant_rsid")
        rsids = (
            "SELECT chromosome, position, reference_allele, alternate_allele, rsid "
            "FROM variant_rsid"
            if has_rsid
            else """
            SELECT CAST(NULL AS INTEGER) AS chromosome, CAST(NULL AS BIGINT) AS position,
                   CAST(NULL AS VARCHAR) AS reference_allele,
                   CAST(NULL AS VARCHAR) AS alternate_allele, CAST(NULL AS VARCHAR) AS rsid
            WHERE false
            """
        )
        af_clauses = []
        af_min, af_max = self.param("af_min"), self.param("af_max")
        if af_min is not None:
            af_clauses.append(f"v.af_joint >= {float(af_min)}")
        if af_max is not None:
            af_clauses.append(f"v.af_joint <= {float(af_max)}")
        af_filter = ("WHERE " + " AND ".join(af_clauses)) if af_clauses else ""
        per_gene_clause = (
            f"rn <= {max_per_gene}" if max_per_gene > 0 else "true"
        )

        size_filter = (
            f"HAVING count(DISTINCT gene_id) <= {max_group_size}"
            if max_group_size > 0
            else ""
        )
        # A pair is always across two distinct genes: `gene_1_id <
        # gene_2_id` in `gene_pairs` sees to that, so there is no
        # same-gene case left to exclude.
        #
        # Side 2 is either restricted to the input's own genes, or free.
        side_2_source = "seed_genes" if membership == "both" else "all_genes"

        common = f"""
            WITH rsids AS ({rsids}),
            -- Stage 1a: inputs that name a gene.
            gene_inputs AS (
                SELECT DISTINCT
                    i.input_value, gm.entity_id AS gene_id, gm.symbol AS gene_symbol
                FROM pv_input i
                JOIN entity_aliases a ON {ALIAS_KEY} = i.term
                JOIN gene_masters gm ON gm.entity_id = a.entity_id
                WHERE i.input_kind = 'gene'
            ),
            -- Stage 1b: inputs that name a variant, placed on the genome.
            variant_inputs AS (
                SELECT DISTINCT
                    i.input_value, v.variant_key, v.chromosome, v.position,
                    v.reference_allele, v.alternate_allele
                FROM pv_input i
                JOIN rsids r ON i.input_kind = 'rsid' AND lower(r.rsid) = i.rsid
                JOIN variant_masters v
                  ON v.chromosome = r.chromosome AND v.position = r.position
                 AND v.reference_allele = r.reference_allele
                 AND v.alternate_allele = r.alternate_allele
                UNION
                SELECT DISTINCT
                    i.input_value, v.variant_key, v.chromosome, v.position,
                    v.reference_allele, v.alternate_allele
                FROM pv_input i
                JOIN variant_masters v
                  ON v.chromosome = i.chromosome AND v.position = i.position
                WHERE i.input_kind IN ('chr_pos', 'chr_pos_allele')
                  AND (i.input_kind = 'chr_pos'
                       OR (v.reference_allele = i.reference_allele
                           AND v.alternate_allele = i.alternate_allele))
            ),
            -- Stage 1c: a variant belongs to the genes whose range holds it.
            variant_genes AS (
                SELECT DISTINCT
                    vi.input_value, vi.variant_key, vi.chromosome, vi.position,
                    l.entity_id AS gene_id, gm.symbol AS gene_symbol
                FROM variant_inputs vi
                JOIN entity_locations l
                  ON l.build = {build} AND l.chromosome = vi.chromosome
                 AND vi.position BETWEEN l.start_pos - {window} AND l.end_pos + {window}
                JOIN gene_masters gm ON gm.entity_id = l.entity_id
            ),
            -- The genes the input reaches, however it named them.
            seed_genes AS (
                SELECT input_value, gene_id, gene_symbol FROM gene_inputs
                UNION
                SELECT input_value, gene_id, gene_symbol FROM variant_genes
            ),
            -- Stage 2: every (group, gene) link, in both directions, for
            -- the group types asked for.
            links_raw AS (
                SELECT r.entity_1_id AS group_id, r.entity_2_id AS gene_id
                FROM entity_relationships r
                JOIN entities eg ON eg.id = r.entity_1_id
                JOIN entity_groups g ON g.id = eg.group_id
                JOIN entities ge ON ge.id = r.entity_2_id
                JOIN entity_groups gg ON gg.id = ge.group_id
                WHERE g.name IN ({self._sql_list(group_types)}) AND gg.name = 'Genes'
                UNION ALL
                SELECT r.entity_2_id, r.entity_1_id
                FROM entity_relationships r
                JOIN entities eg ON eg.id = r.entity_2_id
                JOIN entity_groups g ON g.id = eg.group_id
                JOIN entities ge ON ge.id = r.entity_1_id
                JOIN entity_groups gg ON gg.id = ge.group_id
                WHERE g.name IN ({self._sql_list(group_types)}) AND gg.name = 'Genes'
            ),
            links_distinct AS (
                SELECT DISTINCT group_id, gene_id FROM links_raw WHERE group_id <> gene_id
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
            all_genes AS (
                SELECT DISTINCT gene_id FROM links
            ),
            -- Stage 2b: gene pairs, with the groups that justify them.
            --
            -- Side 1 is always the seed. Ordering the two by entity id
            -- instead would drop every pair whose seed happens to sort
            -- after its partner — silently, and for no reason but the
            -- order the ids were assigned in.
            pair_links AS (
                SELECT l1.gene_id AS gene_1_id, l2.gene_id AS gene_2_id, l1.group_id
                FROM (SELECT DISTINCT gene_id FROM seed_genes) s1
                JOIN links l1 ON l1.gene_id = s1.gene_id
                JOIN links l2
                  ON l2.group_id = l1.group_id AND l2.gene_id <> l1.gene_id
                JOIN {side_2_source} s2 ON s2.gene_id = l2.gene_id
            ),
            gene_pairs_raw AS (
                SELECT
                    p.gene_1_id, p.gene_2_id,
                    count(DISTINCT p.group_id) AS group_support_count,
                    list_sort(list_distinct(list(gn.group_type))) AS group_support_types,
                    list_sort(list_distinct(list(gn.name))) AS group_support_names
                FROM pair_links p
                LEFT JOIN group_names gn ON gn.entity_id = p.group_id
                GROUP BY 1, 2
                HAVING count(DISTINCT p.group_id) >= {min_support}
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
                    (p.gene_1_id IN (SELECT gene_id FROM seed_genes)) AS gene_1_from_input,
                    (p.gene_2_id IN (SELECT gene_id FROM seed_genes)) AS gene_2_from_input
                FROM gene_pairs p
                LEFT JOIN gene_masters g1 ON g1.entity_id = p.gene_1_id
                LEFT JOIN gene_masters g2 ON g2.entity_id = p.gene_2_id
            )
        """

        if diagnostic_only:
            # What the size filter removed, over the same CTEs the real
            # query uses — so the two can never disagree.
            size_expr = (
                f"sz.n <= {max_group_size}" if max_group_size > 0 else "true"
            )
            return common + f"""
            SELECT
                count(DISTINCT l.group_id) AS touching_input,
                count(DISTINCT CASE WHEN {size_expr} THEN l.group_id END) AS kept,
                min(CASE WHEN NOT ({size_expr}) THEN sz.n END) AS smallest_excluded,
                max(sz.n) AS largest_seen
            FROM links_distinct l
            JOIN (SELECT DISTINCT gene_id FROM seed_genes) s ON s.gene_id = l.gene_id
            JOIN (
                SELECT group_id, count(DISTINCT gene_id) AS n
                FROM links_distinct GROUP BY 1
            ) sz ON sz.group_id = l.group_id
            """

        if grain == "gene_pairs":
            return common + f"""
            SELECT
                (SELECT min(input_value) FROM seed_genes s
                  WHERE s.gene_id = p.gene_1_id) AS input_1,
                p.gene_1_id, p.gene_1_symbol, p.gene_1_from_input,
                p.gene_2_id, p.gene_2_symbol, p.gene_2_from_input,
                p.group_support_count, p.group_support_types, p.group_support_names,
                '{membership}' AS membership
            FROM named_pairs p
            ORDER BY p.group_support_count DESC, p.gene_1_symbol, p.gene_2_symbol
            LIMIT {max_pairs if max_pairs > 0 else 9223372036854775807}
            """

        # Stage 3: gene pairs become variant pairs. A gene's variants are
        # the input's own when it named them, and everything in its range
        # when it did not.
        return common + f"""
            , pair_genes AS (
                SELECT gene_1_id AS gene_id FROM named_pairs
                UNION
                SELECT gene_2_id FROM named_pairs
            ),
            named_genes AS (
                SELECT gene_id, min(input_value) AS input_value
                FROM gene_inputs GROUP BY 1
            ),
            -- Stage 3: every variant of every gene a pair touches.
            --
            -- `from_input` depends on how the gene entered, and the two
            -- cases are not the same request. Naming a gene asks for its
            -- variants; naming a variant asks for that variant, not for
            -- the other 4,000 in the gene that happens to contain it.
            gene_variants_all AS (
                SELECT
                    pg.gene_id, v.variant_key, v.chromosome, v.position, v.af_joint,
                    coalesce(vg.input_value, ng.input_value) AS input_value,
                    (vg.variant_key IS NOT NULL) AS named,
                    (vg.variant_key IS NOT NULL OR ng.gene_id IS NOT NULL) AS from_input
                FROM pair_genes pg
                JOIN entity_locations l
                  ON l.entity_id = pg.gene_id AND l.build = {build}
                JOIN variant_masters v
                  ON v.chromosome = l.chromosome
                 AND v.position BETWEEN l.start_pos - {window} AND l.end_pos + {window}
                LEFT JOIN variant_genes vg
                       ON vg.gene_id = pg.gene_id AND vg.variant_key = v.variant_key
                LEFT JOIN named_genes ng ON ng.gene_id = pg.gene_id
                {af_filter}
            ),
            -- Pairs grow with the square of the variants per gene, so
            -- this is where the size of the answer is actually decided.
            -- A gene on chr22 carries about 4,000 variants; three such
            -- genes paired in full are 48 million rows, which is not an
            -- answer anybody reads.
            --
            -- The ones kept are the most common, because a pairwise
            -- interaction test has no power on a rare variant. A variant
            -- the caller named by hand is never dropped.
            gene_variants AS (
                SELECT gene_id, variant_key, chromosome, position, input_value,
                       from_input
                FROM (
                    SELECT *, row_number() OVER (
                        PARTITION BY gene_id
                        ORDER BY af_joint DESC NULLS LAST, variant_key
                    ) AS rn
                    FROM gene_variants_all
                )
                WHERE named OR {per_gene_clause}
            ),
            pairs AS (
                SELECT
                    v1.input_value AS input_1, v1.variant_key AS variant_1_key,
                    v1.chromosome AS variant_1_chromosome,
                    v1.position AS variant_1_position,
                    p.gene_1_id, p.gene_1_symbol, v1.from_input AS variant_1_from_input,
                    v2.input_value AS input_2, v2.variant_key AS variant_2_key,
                    v2.chromosome AS variant_2_chromosome,
                    v2.position AS variant_2_position,
                    p.gene_2_id, p.gene_2_symbol, v2.from_input AS variant_2_from_input,
                    p.group_support_count, p.group_support_types, p.group_support_names
                FROM named_pairs p
                JOIN gene_variants v1 ON v1.gene_id = p.gene_1_id
                JOIN gene_variants v2 ON v2.gene_id = p.gene_2_id
                -- Not `v1 < v2`: gene_1_id < gene_2_id already fixes the
                -- orientation, so each (variant of gene 1, variant of
                -- gene 2) combination arises exactly once here. Ordering
                -- the keys as well would silently drop about half the
                -- pairs, by nothing more than how the strings sort.
                -- `<>` is only for two overlapping genes sharing a variant.
                WHERE v1.variant_key <> v2.variant_key
                  {"AND v1.from_input AND v2.from_input" if membership == "both"
                    else "AND (v1.from_input OR v2.from_input)"}
            )
            SELECT
                input_1, variant_1_key, r1.rsid AS variant_1_rsid,
                variant_1_chromosome, variant_1_position,
                gene_1_id, gene_1_symbol, variant_1_from_input,
                input_2, variant_2_key, r2.rsid AS variant_2_rsid,
                variant_2_chromosome, variant_2_position,
                gene_2_id, gene_2_symbol, variant_2_from_input,
                group_support_count, group_support_types, group_support_names,
                '{membership}' AS membership
            FROM pairs p
            LEFT JOIN rsids r1 ON r1.chromosome = p.variant_1_chromosome
                              AND r1.position = p.variant_1_position
            LEFT JOIN rsids r2 ON r2.chromosome = p.variant_2_chromosome
                              AND r2.position = p.variant_2_position
            -- The same unordered pair can arise from two different gene
            -- pairs, in either orientation. Keep the best-supported one.
            QUALIFY row_number() OVER (
                PARTITION BY least(variant_1_key, variant_2_key),
                             greatest(variant_1_key, variant_2_key)
                ORDER BY group_support_count DESC, variant_1_key
            ) = 1
            ORDER BY group_support_count DESC, variant_1_key, variant_2_key
            LIMIT {max_pairs if max_pairs > 0 else 9223372036854775807}
            """

    # ------------------------------------------------------------------
    def _note_group_filter(self, query: str, max_group_size: int) -> None:
        """
        Say what `max_group_size` removed.

        Without this an empty result reads as "these genes share no
        biology", when the truth can be "the only things linking them are
        pathways of 1,200 genes, which you asked to exclude". The three
        pathways shared by SMARCB1 and CHEK2 in the current bundle have
        1,231, 1,321 and 1,543 genes — so the default cap returns nothing
        for that pair, correctly and unhelpfully.
        """
        row = self.sql(query).to_pylist()
        stats = row[0] if row else {}
        touching = stats.get("touching_input") or 0
        kept = stats.get("kept") or 0
        excluded = touching - kept

        self.note_provenance(
            "group_filter",
            {
                "max_group_size": max_group_size or None,
                "groups_touching_input": touching,
                "groups_kept": kept,
                "groups_excluded_by_size": excluded,
                "smallest_excluded": stats.get("smallest_excluded"),
                "largest_seen": stats.get("largest_seen"),
                "means": (
                    f"{excluded} of the {touching} groups that reach these genes "
                    f"name more than {max_group_size} genes each and were "
                    f"excluded. The smallest one excluded has "
                    f"{stats.get('smallest_excluded')} genes. If the result is "
                    f"empty or thin, raising max_group_size is what changes it."
                    if excluded
                    else "No group was excluded for its size."
                ),
            },
        )

    # ------------------------------------------------------------------
    def _note_truncation(self, table: pa.Table, max_pairs: int) -> None:
        """
        Say so when the cap hid pairs.

        A capped result is a round number of rows that looks like an
        answer. Here it matters more than usual: pair counts grow with
        the square of the input, so hitting the cap is the normal case
        rather than the exception.
        """
        hit = max_pairs > 0 and table.num_rows >= max_pairs
        self.note_provenance(
            "truncation",
            {
                "max_pairs": max_pairs or None,
                "applied": hit,
                "returned": table.num_rows,
                "means": (
                    "The cap was reached, so this is not every pair. The ones "
                    "kept are those with the most group support. Raise "
                    "max_pairs, raise min_group_support, or lower "
                    "max_group_size to see a different slice."
                    if hit
                    else "Every pair that met the criteria is here."
                ),
            },
        )
