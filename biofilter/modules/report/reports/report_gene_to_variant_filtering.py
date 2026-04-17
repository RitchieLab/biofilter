from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import func, or_, text
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import (
    EntityAlias,
    EntityGroup,
    EntityLocation,
    GeneMaster,
    VariantConsequence,
    VariantConsequenceCategory,
    VariantConsequenceGroup,
    VariantImpact,
)
from biofilter.modules.report.reports.base_report import ReportBase

# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

_TEMP_TABLE = "_bf_gene_ranges"


def _norm(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _to_list(v: Any) -> list[str]:
    """Coerce a scalar, list, or comma-string into a clean list of strings."""
    if v is None:
        return []
    if isinstance(v, (list, tuple, set)):
        return [_norm(x) for x in v if _norm(x)]
    s = _norm(v)
    return [p.strip() for p in s.split(",") if p.strip()] if s else []


def _empty_df(gene_input: Any, status: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "resolution_status": status,
                "gene_input": str(gene_input),
                "gene_entity_id": None,
                "gene_symbol": None,
                "gene_chromosome": None,
                "gene_start": None,
                "gene_end": None,
                "variant_id": None,
                "chromosome": None,
                "position_start": None,
                "position_end": None,
                "rsid": None,
                "reference_allele": None,
                "alternate_allele": None,
                "af": None,
                "transcript_id": None,
                "consequence_id": None,
                "consequence_name": None,
                "consequence_group": None,
                "consequence_category": None,
                "impact_id": None,
                "impact_name": None,
                "is_most_severe_for_variant": None,
                "hgvsc": None,
                "hgvsp": None,
                "lof_flag": None,
                "lof_confidence": None,
                "lof_filter": None,
                "lof_flags": None,
                "canonical": None,
                "mane_select": None,
                "cadd_phred": None,
                "sift_max": None,
                "polyphen_max": None,
                "alphamissense_score": None,
                "alphamissense_classification": None,
            }
        ]
    )


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


class GeneToVariantFilteringReport(ReportBase):
    """
    Phase 2 of the single-variant SNP×SNP interaction pipeline.

    Given a list of gene symbols, this report:
      1. Resolves symbols → entity_ids (via entity_aliases).
      2. Resolves entity_ids → genomic loci (entity_locations, filtered by build).
      3. Pre-resolves consequence/impact filter names → IDs (SQL-level filtering).
      4. Queries variant_masters + variant_molecular_effects per chromosome using
         a temporary gene-range table — one query per chromosome partition.
      5. LEFT JOINs variant_effect_predictions to bring AlphaMissense scores.
      6. Returns one row per (gene × variant) when most_severe_only=True, or
         one row per (gene × variant × transcript) when most_severe_only=False.

    All heavy filters (impact, consequence, LoF, AF, CADD, SIFT, PolyPhen) are
    pushed to SQL before data reaches Python. AlphaMissense filters are applied
    post-query because they come from a LEFT JOIN result.

    Resolution failures return a single-row DataFrame with a non-null
    `resolution_status` field.
    """

    name = "gene_to_variant_filtering"
    description = (
        "Phase 2 of the single-variant interaction pipeline. "
        "Given a list of gene symbols, collects variants overlapping each gene "
        "locus with configurable SQL-level filters (impact, consequence type, "
        "LoF confidence, AF, CADD, SIFT, PolyPhen) and AlphaMissense annotations."
    )

    _BATCH = 500

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @classmethod
    def example_input(cls) -> dict:
        return {
            "gene_symbols": ["APOE"],
            "build": 38,
            "gene_window_bp": 0,
            "most_severe_only": True,
            "impact_filter": None,
            "consequence_type_filter": None,
            "lof_confidence_filter": None,
            "af_max": None,
            "af_min": None,
            "cadd_phred_min": None,
            "sift_score_max": None,
            "polyphen_score_min": None,
            "alphamissense_score_min": None,
            "alphamissense_classification": None,
            "max_variants_per_gene": 5000,
        }

    @classmethod
    def explain(cls) -> str:
        return (
            "Parameters\n"
            "----------\n"
            "  gene_symbols (required)  list[str] or comma-separated string of gene\n"
            "    symbols. Also accepted as 'input_data' (alias used by run_example).\n"
            "\n"
            "  build (default 38)  genome assembly build for locus lookup.\n"
            "\n"
            "  gene_window_bp (default 0)  extend each gene locus by this many bp\n"
            "    on each side before querying variants.\n"
            "\n"
            "  most_severe_only (default True)  keep only the row flagged\n"
            "    is_most_severe_for_variant=TRUE in variant_molecular_effects.\n"
            "    True → 1 row per variant (variant-level unit of analysis).\n"
            "    False → 1 row per variant × transcript.\n"
            "\n"
            "  impact_filter (default None)  list of impact names to keep, e.g.\n"
            "    ['HIGH', 'MODERATE'].\n"
            "\n"
            "  consequence_type_filter (default None)  list of consequence group,\n"
            "    category, or individual consequence names. Resolved to\n"
            "    consequence_ids before the main query.\n"
            "\n"
            "  lof_confidence_filter (default None)  list of LoF confidence tiers,\n"
            "    e.g. ['HC'] or ['HC', 'LC'].\n"
            "\n"
            "  af_max (default None)  maximum allele frequency (e.g. 0.01 for rare).\n"
            "\n"
            "  af_min (default None)  minimum allele frequency (e.g. 0.05 for common).\n"
            "\n"
            "  cadd_phred_min (default None)  minimum CADD Phred score.\n"
            "\n"
            "  sift_score_max (default None)  maximum SIFT score (lower = more\n"
            "    deleterious; e.g. 0.05).\n"
            "\n"
            "  polyphen_score_min (default None)  minimum PolyPhen score (higher =\n"
            "    more damaging; e.g. 0.85).\n"
            "\n"
            "  alphamissense_score_min (default None)  minimum AlphaMissense score.\n"
            "\n"
            "  alphamissense_classification (default None)  list of AlphaMissense\n"
            "    classifications to keep, e.g. ['likely_pathogenic', 'ambiguous'].\n"
            "\n"
            "  max_variants_per_gene (default 5000)  safety cap; emits a WARNING\n"
            "    if a gene exceeds this limit after all filters.\n"
        )

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self) -> pd.DataFrame:
        # ── 1. Read parameters ─────────────────────────────────────────────
        # run_example() passes example_input() as input_data=<dict>; unpack it
        # so the individual keys are available via self.param().
        _id = self.param("input_data", default=None)
        if isinstance(_id, dict):
            for k, v in _id.items():
                self.params.setdefault(k, v)

        raw_genes = self.param("gene_symbols", default=None)
        if not raw_genes:
            raise ValueError("Missing required parameter: 'gene_symbols'")

        # Accept list, comma-string, or path to a .txt file (one gene per line)
        if isinstance(raw_genes, (list, tuple, set)):
            gene_symbols: list[str] = _to_list(raw_genes)
        else:
            gene_symbols = self.resolve_input_list(raw_genes, param_name="gene_symbols")

        if not gene_symbols:
            return _empty_df(raw_genes, "empty_gene_list")

        build = int(self.param("build", default=38) or 38)
        gene_window_bp = int(self.param("gene_window_bp", default=0) or 0)
        most_severe_only = bool(self.param("most_severe_only", default=True))

        impact_filter = _to_list(self.param("impact_filter", default=None))
        consequence_type_filter = _to_list(
            self.param("consequence_type_filter", default=None)
        )
        lof_confidence_filter = _to_list(
            self.param("lof_confidence_filter", default=None)
        )

        af_max = self.param("af_max", default=None)
        af_min = self.param("af_min", default=None)
        cadd_phred_min = self.param("cadd_phred_min", default=None)
        sift_score_max = self.param("sift_score_max", default=None)
        polyphen_score_min = self.param("polyphen_score_min", default=None)

        am_score_min = self.param("alphamissense_score_min", default=None)
        am_class_filter = _to_list(
            self.param("alphamissense_classification", default=None)
        )

        max_per_gene = int(self.param("max_variants_per_gene", default=5000) or 5000)

        self.logger.log(
            f"gene_symbols={len(gene_symbols)} genes  build={build}  "
            f"window={gene_window_bp}  most_severe_only={most_severe_only}  "
            f"impact={impact_filter or 'all'}  consequence={consequence_type_filter or 'all'}  "
            f"lof_conf={lof_confidence_filter or 'all'}  "
            f"af_max={af_max}  af_min={af_min}  cadd_phred_min={cadd_phred_min}"
        )

        # ── 2. Resolve gene symbols → entity_ids ───────────────────────────
        gene_entity_map = self._resolve_gene_symbols(gene_symbols)
        if not gene_entity_map:
            self.logger.log("No genes resolved from input symbols", "WARNING")
            return _empty_df(gene_symbols, "no_genes_resolved")

        self.logger.log(
            f"Resolved {len(gene_entity_map)}/{len(gene_symbols)} gene symbols"
        )

        # ── 3. Resolve entity_ids → genomic loci ───────────────────────────
        assembly_map = self.resolve_assembly_map(str(build))
        assembly_ids = list(assembly_map.values())

        gene_loci = self._resolve_gene_loci(
            list(gene_entity_map.keys()), assembly_ids, gene_window_bp
        )
        if not gene_loci:
            self.logger.log("No genomic loci found for resolved genes", "WARNING")
            return _empty_df(gene_symbols, "no_loci_found")

        self.logger.log(f"Gene loci resolved: {len(gene_loci)} genes with locations")

        # ── 4. Pre-resolve filter ID sets ──────────────────────────────────
        consequence_ids = (
            self._resolve_consequence_ids(consequence_type_filter)
            if consequence_type_filter
            else set()
        )
        impact_ids = self._resolve_impact_ids(impact_filter) if impact_filter else set()

        # ── 5. Build label lookup dicts ────────────────────────────────────
        consequence_labels = self._build_consequence_labels()
        impact_labels = self._build_impact_labels()

        # ── 6. Create temp table and populate ──────────────────────────────
        self._create_temp_table()
        self._populate_temp_table(gene_entity_map, gene_loci)

        # ── 7. Query variants per chromosome ───────────────────────────────
        chromosomes = sorted({loc["chromosome"] for loc in gene_loci.values()})
        self.logger.log(f"Querying {len(chromosomes)} chromosome partition(s)")

        frames: list[pd.DataFrame] = []
        try:
            for chrom in chromosomes:
                df_chrom = self._query_chromosome(
                    chromosome=chrom,
                    most_severe_only=most_severe_only,
                    impact_ids=impact_ids,
                    consequence_ids=consequence_ids,
                    lof_confidence=lof_confidence_filter,
                    af_max=af_max,
                    af_min=af_min,
                    cadd_phred_min=cadd_phred_min,
                    sift_score_max=sift_score_max,
                    polyphen_score_min=polyphen_score_min,
                )
                if df_chrom is not None and not df_chrom.empty:
                    frames.append(df_chrom)
                    self.logger.log(
                        f"  chr{chrom}: {len(df_chrom)} rows, "
                        f"{df_chrom['variant_id'].nunique()} unique variants"
                    )
        finally:
            self._drop_temp_table()

        if not frames:
            return _empty_df(gene_symbols, "no_variants_found")

        df = pd.concat(frames, ignore_index=True)

        # ── 8. Enrich with label columns ───────────────────────────────────
        df["consequence_name"] = df["consequence_id"].map(
            lambda x: consequence_labels.get(x, {}).get("name")
        )
        df["consequence_group"] = df["consequence_id"].map(
            lambda x: consequence_labels.get(x, {}).get("group")
        )
        df["consequence_category"] = df["consequence_id"].map(
            lambda x: consequence_labels.get(x, {}).get("category")
        )
        df["impact_name"] = df["impact_id"].map(lambda x: impact_labels.get(x))

        # ── 9. Apply AlphaMissense Python-side filters ─────────────────────
        if am_score_min is not None:
            df = df[
                df["alphamissense_score"].isna()
                | (df["alphamissense_score"] >= float(am_score_min))
            ]
        if am_class_filter:
            classes_lower = {c.lower() for c in am_class_filter}
            df = df[
                df["alphamissense_classification"].isna()
                | df["alphamissense_classification"].str.lower().isin(classes_lower)
            ]

        # ── 10. Deduplicate when most_severe_only=True ────────────────────
        # is_most_severe_for_variant = TRUE can still match multiple transcripts
        # (same worst consequence on different transcripts). Dedup collapses them
        # to 1 row per (gene × variant), keeping the first occurrence which
        # represents the transcript that carries the most severe consequence.
        if most_severe_only:
            before = len(df)
            df = df.drop_duplicates(subset=["gene_entity_id", "variant_id"])
            after = len(df)
            if before != after:
                self.logger.log(
                    f"most_severe_only dedup: {before} → {after} rows "
                    f"({before - after} transcript duplicates removed)"
                )

        # ── 11. Apply max_variants_per_gene cap ────────────────────────────
        capped_frames: list[pd.DataFrame] = []
        for gene_eid, gdf in df.groupby("gene_entity_id"):
            unique_v = gdf["variant_id"].nunique()
            if unique_v > max_per_gene:
                sym = gdf["gene_symbol"].iloc[0]
                self.logger.log(
                    f"Gene {sym} (entity_id={gene_eid}) has {unique_v} variants "
                    f"after filtering — capped at {max_per_gene}",
                    "WARNING",
                )
                keep_ids = gdf["variant_id"].drop_duplicates().iloc[:max_per_gene]
                gdf = gdf[gdf["variant_id"].isin(keep_ids)]
            capped_frames.append(gdf)

        df = pd.concat(capped_frames, ignore_index=True)
        df["resolution_status"] = None
        df["gene_input"] = df["gene_symbol"]

        total_variants = df["variant_id"].nunique()  # noqa: E501
        self.logger.log(
            f"Final output: {len(df)} rows, "
            f"{df['gene_entity_id'].nunique()} genes, "
            f"{total_variants} unique variants"
        )

        # ── 11. Reorder columns (and suppress transcript columns in variant mode) ──
        # When most_severe_only=True the unit is the variant, not the transcript.
        # transcript_id, canonical and mane_select are implementation details of
        # the consequence selection — exposing them would confuse users who did
        # not ask for transcript-level analysis.
        # hgvsc / hgvsp are kept: they describe the molecular effect of the
        # variant and are useful regardless of the analysis mode.
        transcript_cols = {"transcript_id", "canonical", "mane_select"}

        col_order = [
            "resolution_status",
            "gene_input",
            "gene_entity_id",
            "gene_symbol",
            "gene_chromosome",
            "gene_start",
            "gene_end",
            "variant_id",
            "chromosome",
            "position_start",
            "position_end",
            "rsid",
            "reference_allele",
            "alternate_allele",
            "af",
            "transcript_id",
            "consequence_id",
            "consequence_name",
            "consequence_group",
            "consequence_category",
            "impact_id",
            "impact_name",
            "is_most_severe_for_variant",
            "hgvsc",
            "hgvsp",
            "lof_flag",
            "lof_confidence",
            "lof_filter",
            "lof_flags",
            "canonical",
            "mane_select",
            "cadd_phred",
            "sift_max",
            "polyphen_max",
            "alphamissense_score",
            "alphamissense_classification",
        ]
        if most_severe_only:
            col_order = [c for c in col_order if c not in transcript_cols]

        present = [c for c in col_order if c in df.columns]
        return df[present]

    # ------------------------------------------------------------------
    # Step 2 — Gene symbol → entity_id resolution
    # ------------------------------------------------------------------

    def _resolve_gene_symbols(self, symbols: list[str]) -> dict[int, str]:
        """
        Returns {entity_id: gene_symbol} for every resolved symbol.

        Searches entity_aliases scoped to the 'Genes' entity_group.
        A symbol may match a preferred alias, synonym, or code. When
        multiple entity_ids match one symbol, all are included.
        """
        gene_group = (
            self.session.query(EntityGroup)
            .filter(func.lower(EntityGroup.name) == "genes")
            .first()
        )
        if gene_group is None:
            self.logger.log("EntityGroup 'Genes' not found", "ERROR")
            return {}

        symbols_lower = [s.lower() for s in symbols]

        rows = (
            self.session.query(
                EntityAlias.entity_id,
                EntityAlias.alias_value,
            )
            .filter(
                EntityAlias.group_id == gene_group.id,
                func.lower(EntityAlias.alias_value).in_(symbols_lower),
            )
            .all()
        )

        # Use the GeneMaster symbol as the canonical label when available
        entity_ids = {int(r.entity_id) for r in rows}
        symbol_map: dict[int, str] = {}
        if entity_ids:
            gm_rows = (
                self.session.query(GeneMaster.entity_id, GeneMaster.symbol)
                .filter(GeneMaster.entity_id.in_(list(entity_ids)))
                .all()
            )
            for gm in gm_rows:
                symbol_map[int(gm.entity_id)] = _norm(gm.symbol)

        result: dict[int, str] = {}
        for r in rows:
            eid = int(r.entity_id)
            result[eid] = symbol_map.get(eid) or _norm(r.alias_value)

        return result

    # ------------------------------------------------------------------
    # Step 3 — entity_id → loci
    # ------------------------------------------------------------------

    def _resolve_gene_loci(
        self,
        entity_ids: list[int],
        assembly_ids: list[int],
        window_bp: int,
    ) -> dict[int, dict]:
        """
        Returns {entity_id: {chromosome, start_pos, end_pos}} with optional
        window expansion applied.
        """
        rows = (
            self.session.query(
                EntityLocation.entity_id,
                EntityLocation.chromosome,
                EntityLocation.start_pos,
                EntityLocation.end_pos,
            )
            .filter(
                EntityLocation.entity_id.in_(entity_ids),
                EntityLocation.assembly_id.in_(assembly_ids),
            )
            .all()
        )

        result: dict[int, dict] = {}
        for r in rows:
            eid = int(r.entity_id)
            if eid not in result:
                result[eid] = {
                    "chromosome": int(r.chromosome),
                    "start_pos": max(0, int(r.start_pos) - window_bp),
                    "end_pos": int(r.end_pos) + window_bp,
                }
        return result

    # ------------------------------------------------------------------
    # Step 4 — Filter ID pre-resolution
    # ------------------------------------------------------------------

    def _resolve_consequence_ids(self, type_filter: list[str]) -> set[int]:
        """
        Resolve consequence group names, category names, or individual
        consequence names → set of consequence_id integers.
        """
        names_lower = {s.lower() for s in type_filter}

        group_ids = {
            int(r.id)
            for r in self.session.query(VariantConsequenceGroup.id)
            .filter(func.lower(VariantConsequenceGroup.name).in_(names_lower))
            .all()
        }
        cat_ids = {
            int(r.id)
            for r in self.session.query(VariantConsequenceCategory.id)
            .filter(func.lower(VariantConsequenceCategory.name).in_(names_lower))
            .all()
        }
        direct_ids = {
            int(r.id)
            for r in self.session.query(VariantConsequence.id)
            .filter(func.lower(VariantConsequence.name).in_(names_lower))
            .all()
        }

        inherited: set[int] = set()
        if group_ids or cat_ids:
            filters = []
            if group_ids:
                filters.append(VariantConsequence.consequence_group_id.in_(group_ids))
            if cat_ids:
                filters.append(VariantConsequence.consequence_category_id.in_(cat_ids))
            rows = self.session.query(VariantConsequence.id).filter(or_(*filters)).all()
            inherited = {int(r.id) for r in rows}

        return inherited | direct_ids

    def _resolve_impact_ids(self, impact_filter: list[str]) -> set[int]:
        names_lower = {s.lower() for s in impact_filter}
        rows = (
            self.session.query(VariantImpact.id)
            .filter(func.lower(VariantImpact.name).in_(names_lower))
            .all()
        )
        return {int(r.id) for r in rows}

    # ------------------------------------------------------------------
    # Step 5 — Label dicts
    # ------------------------------------------------------------------

    def _build_consequence_labels(self) -> dict[int, dict]:
        """Returns {consequence_id: {name, group, category}}."""
        cons_alias = aliased(VariantConsequence)
        grp_alias = aliased(VariantConsequenceGroup)
        cat_alias = aliased(VariantConsequenceCategory)

        rows = (
            self.session.query(
                cons_alias.id,
                cons_alias.name,
                grp_alias.name.label("group_name"),
                cat_alias.name.label("cat_name"),
            )
            .outerjoin(grp_alias, cons_alias.consequence_group_id == grp_alias.id)
            .outerjoin(cat_alias, cons_alias.consequence_category_id == cat_alias.id)
            .all()
        )
        return {
            int(r.id): {
                "name": _norm(r.name),
                "group": _norm(r.group_name) or None,
                "category": _norm(r.cat_name) or None,
            }
            for r in rows
        }

    def _build_impact_labels(self) -> dict[int, str]:
        """Returns {impact_id: impact_name}."""
        rows = self.session.query(VariantImpact.id, VariantImpact.name).all()
        return {int(r.id): _norm(r.name) for r in rows}

    # ------------------------------------------------------------------
    # Step 6 — Temp table lifecycle
    # ------------------------------------------------------------------

    def _create_temp_table(self) -> None:
        self.session.execute(text(f"DROP TABLE IF EXISTS {_TEMP_TABLE}"))
        self.session.execute(
            text(
                f"""
            CREATE TEMP TABLE {_TEMP_TABLE} (
                gene_entity_id BIGINT,
                gene_symbol    TEXT,
                chromosome     INTEGER,
                range_start    BIGINT,
                range_end      BIGINT
            )
        """
            )
        )
        self.session.flush()

    def _populate_temp_table(
        self,
        gene_entity_map: dict[int, str],
        gene_loci: dict[int, dict],
    ) -> None:
        rows = []
        for eid, loc in gene_loci.items():
            rows.append(
                {
                    "eid": eid,
                    "sym": gene_entity_map.get(eid, f"entity_{eid}"),
                    "chr": loc["chromosome"],
                    "start": loc["start_pos"],
                    "end": loc["end_pos"],
                }
            )

        for i in range(0, len(rows), self._BATCH):
            batch = rows[i : i + self._BATCH]
            self.session.execute(
                text(
                    f"""
                    INSERT INTO {_TEMP_TABLE}
                        (gene_entity_id, gene_symbol, chromosome, range_start, range_end)
                    VALUES (:eid, :sym, :chr, :start, :end)
                """
                ),
                batch,
            )
        self.session.flush()

    def _drop_temp_table(self) -> None:
        try:
            self.session.execute(text(f"DROP TABLE IF EXISTS {_TEMP_TABLE}"))
            self.session.flush()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Step 7 — Per-chromosome variant query
    # ------------------------------------------------------------------

    def _query_chromosome(
        self,
        chromosome: int,
        most_severe_only: bool,
        impact_ids: set[int],
        consequence_ids: set[int],
        lof_confidence: list[str],
        af_max,
        af_min,
        cadd_phred_min,
        sift_score_max,
        polyphen_score_min,
    ) -> pd.DataFrame | None:
        """
        Build and execute the main variant query for one chromosome.

        Strategy:
          1. JOIN variant_masters to the temp gene-range table
             (partition-aware: vm.chromosome = :chromosome).
          2. JOIN variant_molecular_effects with optional most_severe_only filter.
          3. LEFT JOIN a pre-aggregated AlphaMissense subquery.
          4. Apply all SQL-level filters.

        The join condition uses BETWEEN which allows the Postgres query planner
        to use indexes on (chromosome, position_start).
        """
        # ── VME filters ────────────────────────────────────────────────────
        vme_clauses: list[str] = []
        if most_severe_only:
            vme_clauses.append("AND vme.is_most_severe_for_variant = true")
        if impact_ids:
            ids_str = ",".join(str(i) for i in impact_ids)
            vme_clauses.append(f"AND vme.impact_id IN ({ids_str})")
        if consequence_ids:
            ids_str = ",".join(str(i) for i in consequence_ids)
            vme_clauses.append(f"AND vme.consequence_id IN ({ids_str})")
        if lof_confidence:
            quoted = ",".join(f"'{c}'" for c in lof_confidence)
            vme_clauses.append(f"AND vme.lof_confidence IN ({quoted})")

        # ── VM (variant_masters) filters ───────────────────────────────────
        vm_clauses: list[str] = []
        bind_params: dict[str, Any] = {"chromosome": chromosome}

        if af_max is not None:
            vm_clauses.append("AND vm.af <= :af_max")
            bind_params["af_max"] = float(af_max)
        if af_min is not None:
            vm_clauses.append("AND vm.af >= :af_min")
            bind_params["af_min"] = float(af_min)
        if cadd_phred_min is not None:
            vm_clauses.append("AND vm.cadd_phred >= :cadd_phred_min")
            bind_params["cadd_phred_min"] = float(cadd_phred_min)
        if sift_score_max is not None:
            vm_clauses.append("AND vm.sift_max <= :sift_score_max")
            bind_params["sift_score_max"] = float(sift_score_max)
        if polyphen_score_min is not None:
            vm_clauses.append("AND vm.polyphen_max >= :polyphen_score_min")
            bind_params["polyphen_score_min"] = float(polyphen_score_min)

        sql = f"""
            SELECT
                gr.gene_entity_id                    AS gene_entity_id,
                gr.gene_symbol                       AS gene_symbol,
                gr.range_start                       AS gene_start,
                gr.range_end                         AS gene_end,
                vm.variant_id                        AS variant_id,
                vm.chromosome                        AS chromosome,
                vm.position_start                    AS position_start,
                vm.position_end                      AS position_end,
                vm.rsid                              AS rsid,
                vm.reference_allele                  AS reference_allele,
                vm.alternate_allele                  AS alternate_allele,
                vm.af                                AS af,
                vm.cadd_phred                        AS cadd_phred,
                vm.sift_max                          AS sift_max,
                vm.polyphen_max                      AS polyphen_max,
                vme.transcript_id                    AS transcript_id,
                vme.consequence_id                   AS consequence_id,
                vme.impact_id                        AS impact_id,
                vme.is_most_severe_for_variant       AS is_most_severe_for_variant,
                vme.hgvsc                            AS hgvsc,
                vme.hgvsp                            AS hgvsp,
                vme.lof_flag                         AS lof_flag,
                vme.lof_confidence                   AS lof_confidence,
                vme.lof_filter                       AS lof_filter,
                vme.lof_flags                        AS lof_flags,
                vme.canonical                        AS canonical,
                vme.mane_select                      AS mane_select,
                vep_am.am_score                      AS alphamissense_score,
                vep_am.am_class                      AS alphamissense_classification
            FROM {_TEMP_TABLE} gr
            JOIN variant_masters vm
                ON  vm.chromosome      = gr.chromosome
                AND vm.position_start >= gr.range_start
                AND vm.position_start <= gr.range_end
            JOIN variant_molecular_effects vme
                ON  vme.variant_id  = vm.variant_id
                AND vme.chromosome  = vm.chromosome
                {" ".join(vme_clauses)}
            LEFT JOIN (
                SELECT
                    chromosome,
                    variant_id,
                    MAX(score)          AS am_score,
                    MAX(classification) AS am_class
                FROM variant_effect_predictions
                WHERE chromosome   = :chromosome
                  AND predictor_key = 'alphamissense'
                GROUP BY chromosome, variant_id
            ) vep_am
                ON  vep_am.variant_id  = vm.variant_id
                AND vep_am.chromosome  = vm.chromosome
            WHERE vm.chromosome = :chromosome
              AND gr.chromosome  = :chromosome
              {" ".join(vm_clauses)}
        """  # noqa: S608 — bind params used for all user values; IDs are pre-resolved ints

        try:
            result = self.session.execute(text(sql), bind_params)
            rows = result.mappings().all()
        except Exception as exc:
            self.logger.log(f"Query failed for chr{chromosome}: {exc}", "ERROR")
            return None

        if not rows:
            return None

        df = pd.DataFrame([dict(r) for r in rows])
        df["gene_chromosome"] = chromosome
        return df
