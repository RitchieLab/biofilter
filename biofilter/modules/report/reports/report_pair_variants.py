"""
Candidate variant pairs, linked by shared biology.

Takes variants and returns variant pairs. Three stages:

1. **Place.** Each input variant — an rsID, `chr:pos` or
   `chr:pos:ref:alt` — is placed on the genes whose build-38 range
   contains it, optionally widened by `window_bp`.
2. **Connect.** Those genes reach each other through a shared entity: a
   pathway, a disease, a protein. That entity is the *group*, and how
   many distinct groups link a pair is the pair's support.
3. **Pair.** Two input variants pair when their genes are linked.

**Only variants, and only the ones you name.** It used to accept gene
names, expand each gene into the variants inside it, and offer a
`membership` mode where one side of a pair came from the bundle rather
than from the caller. All three are gone (ADR-005 D13).

The gene path was an implicit chain, and it hid a choice: a gene on chr22
carries about 4,000 variants, of which it kept 100 picked by allele
frequency, and the caller never saw which. Running
`expand_gene_to_variant` first costs one step and puts that selection in
front of the person making it, who can look at the list and filter it
before anything is paired.

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
largest disease in the bundle names 232 genes.

Gene Ontology is not offered as a group type. The bundle carries 38,092
GO entities and **zero** GO relationships, so asking for it would return
nothing and look like a finding.
"""

from __future__ import annotations

from typing import Sequence

import pyarrow as pa

from biofilter.modules.report.reports import _pairing
from biofilter.modules.report.reports import _variants as _v
from biofilter.modules.report.reports.base_report import ReportBase

DEFAULT_BUILD = 38
DEFAULT_MAX_PAIRS = 1_000_000

#: What this report reads as a variant. Anything else is a refusal
#: rather than a silent expansion.
VARIANT_KINDS = ("rsid", "chr_pos", "chr_pos_allele")


class PairVariantsReport(ReportBase):
    name = "pair_variants"
    description = (
        "Candidate variant x variant pairs whose genes share biology: places each "
        "input variant on its genes, connects those genes through shared pathways, "
        "diseases or proteins, and returns the pairs with the support behind them."
    )

    requires = (
        "entities",
        "entity_groups",
        "entity_locations",
        "entity_relationships",
        "entity_aliases",
        "gene_masters",
        "variant_masters",
        "etl_data_sources",
    )

    optional = ("variant_rsid",)

    COLUMNS = (
        "input_1",
        "variant_1_key",
        "variant_1_rsid",
        "variant_1_chromosome",
        "variant_1_position",
        "gene_1_id",
        "gene_1_symbol",
        "input_2",
        "variant_2_key",
        "variant_2_rsid",
        "variant_2_chromosome",
        "variant_2_position",
        "gene_2_id",
        "gene_2_symbol",
        "group_support_count",
        "group_support_source_count",
        "group_support_types",
        "group_support_names",
        "group_support_sources",
    )

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.COLUMNS

    @classmethod
    def example_input(cls):
        return {
            "input_data": ["rs429358", "rs7412", "19:44908684:T:C"],
            "group_types": ["Pathways"],
            "max_group_size": 300,
        }

    # ------------------------------------------------------------------
    def _int_param(self, name: str, default: int) -> int:
        """An explicit zero is an answer, not an absence."""
        value = self.param(name, default)
        if value is None or (isinstance(value, str) and not value.strip()):
            return default
        return int(value)

    # ------------------------------------------------------------------
    def run(self) -> pa.Table:
        inputs = self.resolve_input_list(
            self.param("input_data", required=True), param_name="input_data"
        )
        if not inputs:
            raise ValueError("input_data must contain at least one variant.")

        self._refuse_non_variants(inputs)
        self._refuse_removed_params()

        group_types = _pairing.resolve_group_types(
            self.con, self.param("group_types")
        )
        build = self._int_param("build", DEFAULT_BUILD)
        window = self._int_param("window_bp", 0)
        max_group_size = self._int_param(
            "max_group_size", _pairing.DEFAULT_MAX_GROUP_SIZE
        )
        min_support = self._int_param("min_group_support", 1)
        min_sources = self._int_param("min_group_sources", 1)
        max_pairs = self._int_param("max_pairs", DEFAULT_MAX_PAIRS)

        for name, value in (
            ("window_bp", window),
            ("max_group_size", max_group_size),
            ("max_pairs", max_pairs),
        ):
            if value < 0:
                raise ValueError(f"{name} must be 0 or positive. Got: {value}.")
        if min_support < 1:
            raise ValueError(
                f"min_group_support must be at least 1. Got: {min_support}."
            )
        if min_sources < 1:
            raise ValueError(
                f"min_group_sources must be at least 1. Got: {min_sources}."
            )

        self.con.register("pv_input", _v.input_table(inputs))

        self.note_provenance(
            "pairing",
            {
                "group_types": group_types,
                "max_group_size": max_group_size or None,
                "min_group_support": min_support,
                "min_group_sources": min_sources,
                "window_bp": window,
                "build": build,
                "means": (
                    "Both members of every pair are variants you named. This "
                    "report expands nothing: to start from genes, run "
                    "expand_gene_to_variant first and pair what it returns."
                ),
            },
        )

        common = self._common_cte(
            group_types, build, window, max_group_size, min_support, min_sources
        )
        self._note_group_filter(common, max_group_size)

        table = self.sql(common + self._pairs_select(max_pairs))
        self._note_truncation(table, max_pairs)
        return table

    #: Parameters this report used to take. Silently ignoring one would
    #: hand back a different answer to a caller who thinks they asked for
    #: the old behaviour, which is worse than failing.
    REMOVED_PARAMS = {
        "membership": (
            "every pair is now between two variants you named. For a partner "
            "the bundle chose, run pair_genes(membership='either'), then "
            "expand_gene_to_variant on the partner genes, then pair that."
        ),
        "max_variants_per_gene": (
            "nothing is expanded any more, so there is nothing to cap. "
            "expand_gene_to_variant has the same parameter, where the "
            "expansion now happens."
        ),
        "output_grain": (
            "gene pairs are their own report: pair_genes."
        ),
    }

    def _refuse_removed_params(self) -> None:
        present = [name for name in self.REMOVED_PARAMS if name in self.params]
        if not present:
            return
        lines = "\n".join(
            f"  {name}: {self.REMOVED_PARAMS[name]}" for name in present
        )
        raise ValueError(
            f"pair_variants no longer takes {', '.join(present)} (ADR-005 "
            f"D13).\n\n{lines}"
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _refuse_non_variants(inputs: list[str]) -> None:
        """
        A gene name is not a variant, and used to be silently expanded.

        Refusing names the offending values and the report that turns
        genes into variants, because that is the step being asked for.
        """
        offenders = [
            value
            for value in inputs
            if _v.classify(value)["input_kind"] not in VARIANT_KINDS
        ]
        if not offenders:
            return
        shown = ", ".join(repr(v) for v in offenders[:8])
        more = f" (and {len(offenders) - 8} more)" if len(offenders) > 8 else ""
        raise ValueError(
            f"pair_variants takes variants: an rsID, chr:pos, or "
            f"chr:pos:ref:alt. These are none of those: {shown}{more}.\n\n"
            f"To start from genes, run `expand_gene_to_variant` and pair what "
            f"it returns. That step used to happen here, and it hid a choice: "
            f"a gene carries thousands of variants, and which of them reach "
            f"the pairing is yours to decide rather than this report's."
        )

    # ------------------------------------------------------------------
    def _common_cte(
        self,
        group_types: list[str],
        build: int,
        window: int,
        max_group_size: int,
        min_support: int,
        min_sources: int,
    ) -> str:
        has_rsid = self.bundle.has("variant_rsid")
        narrow = self._narrow_to_input_chromosomes(has_rsid)
        rsids = (
            "SELECT chromosome, position, reference_allele, alternate_allele, rsid "
            f"FROM variant_rsid {narrow}"
            if has_rsid
            else """
            SELECT CAST(NULL AS INTEGER) AS chromosome, CAST(NULL AS BIGINT) AS position,
                   CAST(NULL AS VARCHAR) AS reference_allele,
                   CAST(NULL AS VARCHAR) AS alternate_allele, CAST(NULL AS VARCHAR) AS rsid
            WHERE false
            """
        )
        # Built only when an rsID was actually passed: otherwise it is a
        # full scan resolving nothing, which is what a list of
        # coordinates would pay for. The chromosomes are already known by
        # now, so even this leg is narrowed.
        wants_rsid = bool(
            self.con.execute(
                "SELECT count(*) FROM pv_input WHERE input_kind = 'rsid'"
            ).fetchone()[0]
        )
        rsids_all = (
            "SELECT chromosome, position, reference_allele, alternate_allele, rsid "
            f"FROM variant_rsid {narrow}"
            if has_rsid and wants_rsid
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
        af_filter = ("AND " + " AND ".join(af_clauses)) if af_clauses else ""

        return f"""
            WITH rsids AS ({rsids}),
            rsids_all AS ({rsids_all}),
            masters AS (SELECT * FROM variant_masters {narrow}),
            -- Stage 1: the variants named, placed on the genome.
            variant_inputs AS (
                SELECT DISTINCT
                    i.input_value, v.variant_key, v.chromosome, v.position
                FROM pv_input i
                JOIN rsids_all r ON i.input_kind = 'rsid' AND lower(r.rsid) = i.rsid
                JOIN masters v
                  ON v.chromosome = r.chromosome AND v.position = r.position
                 AND v.reference_allele = r.reference_allele
                 AND v.alternate_allele = r.alternate_allele
                WHERE true {af_filter}
                UNION
                SELECT DISTINCT
                    i.input_value, v.variant_key, v.chromosome, v.position
                FROM pv_input i
                JOIN masters v
                  ON v.chromosome = i.chromosome AND v.position = i.position
                WHERE i.input_kind IN ('chr_pos', 'chr_pos_allele')
                  AND (i.input_kind = 'chr_pos'
                       OR (v.reference_allele = i.reference_allele
                           AND v.alternate_allele = i.alternate_allele))
                  {af_filter}
            ),
            -- A variant belongs to the genes whose range holds it. That
            -- is this report's one assumption, and the reason
            -- `pair_genes` exists for callers whose evidence says
            -- otherwise: a regulatory variant acts on a gene it does not
            -- sit inside.
            variant_genes AS (
                SELECT DISTINCT
                    vi.input_value, vi.variant_key, vi.chromosome, vi.position,
                    l.entity_id AS gene_id
                FROM variant_inputs vi
                JOIN entity_locations l
                  ON l.build = {build} AND l.chromosome = vi.chromosome
                 AND vi.position BETWEEN l.start_pos - {window} AND l.end_pos + {window}
            ),
            seed_genes AS (SELECT DISTINCT gene_id FROM variant_genes),
            {_pairing.links_cte(group_types, max_group_size)},
            {_pairing.gene_pairs_cte(
                side_2_source="seed_genes",
                min_support=min_support,
                min_sources=min_sources,
            )}
        """

    def _narrow_to_input_chromosomes(self, has_rsid: bool) -> str:
        """
        The chromosomes this run can touch, as a WHERE clause.

        A position says its own chromosome. An rsID does not — but
        finding out is a hash join against `variant_rsid` costing about a
        second, and paying it once turns every scan afterwards into one
        partition file instead of the whole bundle. Without it an rsID
        input reads 264 million rsIDs *and* 65 million variants
        unfiltered, which took six minutes.
        """
        rsid_leg = (
            """
            UNION
            SELECT DISTINCT r.chromosome
              FROM pv_input i
              JOIN variant_rsid r ON r.rsid = i.rsid
             WHERE i.input_kind = 'rsid'
            """
            if has_rsid
            else ""
        )
        rows = self.con.execute(
            f"""
            SELECT DISTINCT chromosome FROM pv_input
             WHERE chromosome IS NOT NULL
            {rsid_leg}
            """
        ).fetchall()
        present = sorted({int(r[0]) for r in rows if r[0] is not None})
        if not present:
            return ""
        return f"WHERE chromosome IN ({', '.join(str(c) for c in present)})"

    def _pairs_select(self, max_pairs: int) -> str:
        return f"""
            , pairs AS (
                SELECT
                    v1.input_value AS input_1, v1.variant_key AS variant_1_key,
                    v1.chromosome AS variant_1_chromosome,
                    v1.position AS variant_1_position,
                    p.gene_1_id, p.gene_1_symbol,
                    v2.input_value AS input_2, v2.variant_key AS variant_2_key,
                    v2.chromosome AS variant_2_chromosome,
                    v2.position AS variant_2_position,
                    p.gene_2_id, p.gene_2_symbol,
                    p.group_support_count, p.group_support_source_count,
                    p.group_support_types, p.group_support_names,
                    p.group_support_sources
                FROM named_pairs p
                JOIN variant_genes v1 ON v1.gene_id = p.gene_1_id
                JOIN variant_genes v2 ON v2.gene_id = p.gene_2_id
                -- Not `v1 < v2`: gene_1_id < gene_2_id already fixes the
                -- orientation, so each (variant of gene 1, variant of
                -- gene 2) combination arises exactly once. Ordering the
                -- keys as well would silently drop about half the pairs,
                -- by nothing more than how the strings sort. `<>` is for
                -- two overlapping genes sharing a variant.
                WHERE v1.variant_key <> v2.variant_key
            )
            SELECT
                input_1, variant_1_key, r1.rsid AS variant_1_rsid,
                variant_1_chromosome, variant_1_position,
                gene_1_id, gene_1_symbol,
                input_2, variant_2_key, r2.rsid AS variant_2_rsid,
                variant_2_chromosome, variant_2_position,
                gene_2_id, gene_2_symbol,
                group_support_count, group_support_source_count,
                group_support_types, group_support_names, group_support_sources
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
    def _note_group_filter(self, common: str, max_group_size: int) -> None:
        rows = self.sql(
            common + _pairing.group_filter_select(max_group_size)
        ).to_pylist()
        self.note_provenance(
            "group_filter",
            _pairing.group_filter_block(rows[0] if rows else {}, max_group_size),
        )

    def _note_truncation(self, table: pa.Table, max_pairs: int) -> None:
        """
        Say so when the cap hid pairs.

        Pair counts grow with the square of the input, so hitting the cap
        is the normal case rather than the exception.
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
