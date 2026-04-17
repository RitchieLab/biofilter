from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from biofilter.modules.report.reports.base_report import ReportBase

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_MAX_PAIRS = 1_000_000

# Annotation columns to carry from Lista A to both sides of every pair.
# All columns present in the annotation_source are mirrored with _a / _b suffix.
_PREFERRED_COLS = [
    "variant_id",
    "rsid",
    "chromosome",
    "position_start",
    "gene_entity_id",
    "gene_symbol",
    "consequence_name",
    "consequence_group",
    "consequence_category",
    "impact_name",
    "af",
    "cadd_phred",
    "sift_max",
    "polyphen_max",
    "alphamissense_score",
    "alphamissense_classification",
    "lof_confidence",
    "hgvsp",
]

# ---------------------------------------------------------------------------
# ID parsing helpers (shared logic with variant_list_intersect)
# ---------------------------------------------------------------------------

_RSID_RE = re.compile(r"^rs\d+$", re.IGNORECASE)


def _looks_like_rsid(s: str) -> bool:
    return bool(_RSID_RE.match(s.strip()))


def _parse_chr_pos(s: str) -> tuple[int, int] | None:
    m = re.match(
        r"^(?:chr(?:omosome)?)?([0-9xymXYM]+)[:\-_ ,\t](\d+)$", s, re.IGNORECASE
    )
    if not m:
        return None
    raw = m.group(1).lower()
    mapping = {"x": 23, "y": 24, "m": 25, "mt": 25}
    try:
        chr_int = int(raw)
    except ValueError:
        chr_int = mapping.get(raw)
    if chr_int is None:
        return None
    return (chr_int, int(m.group(2)))


# ---------------------------------------------------------------------------
# Lista D loader  (PLINK .prune.in / .txt / .csv)
# ---------------------------------------------------------------------------


def _load_variant_list(path: Path) -> list[str]:
    """Load a plain variant list — one ID per line (rsID or chr:pos)."""
    ids: list[str] = []
    opener = open
    with opener(path) as fh:
        for line in fh:
            raw = line.strip()
            if raw and not raw.startswith("#"):
                ids.append(raw)
    return ids


# ---------------------------------------------------------------------------
# Annotation enrichment
# ---------------------------------------------------------------------------


def _enrich(variant_ids: list[str], df_annot: pd.DataFrame) -> pd.DataFrame:
    """
    Join a flat list of variant IDs (from Lista D) to the annotation DataFrame
    (Lista A). Matches by rsid first, then chr:pos.

    Returns a DataFrame with one row per (variant_id_from_D × gene) with all
    annotation columns. Variants not found in Lista A are dropped (logged later).
    """
    if df_annot.empty:
        return pd.DataFrame()

    # ── Build lookup indexes from Lista A ──────────────────────────────────
    rsid_col = next(
        (c for c in ["rsid", "rs_id", "snp_id"] if c in df_annot.columns), None
    )
    chr_col = next(
        (c for c in ["chromosome", "chr", "chrom"] if c in df_annot.columns), None
    )
    pos_col = next(
        (c for c in ["position_start", "position", "pos", "bp"] if c in df_annot.columns),
        None,
    )

    # rsid index: rsid_lower → row indices
    rsid_index: dict[str, list[int]] = {}
    if rsid_col:
        for i, v in enumerate(df_annot[rsid_col].fillna("").astype(str)):
            k = v.strip().lower()
            if k and k != "nan":
                rsid_index.setdefault(k, []).append(i)

    # chr:pos index: (chr_int, pos) → row indices
    chrpos_index: dict[tuple[int, int], list[int]] = {}
    if chr_col and pos_col:
        for i, (c, p) in enumerate(
            zip(df_annot[chr_col].fillna(-1), df_annot[pos_col].fillna(-1))
        ):
            try:
                key = (int(c), int(p))
                if key[0] > 0:
                    chrpos_index.setdefault(key, []).append(i)
            except (ValueError, TypeError):
                pass

    # ── Match variant IDs ──────────────────────────────────────────────────
    matched_rows: list[pd.DataFrame] = []
    unmatched: list[str] = []

    for raw_id in variant_ids:
        rows = None
        # Try rsID
        if _looks_like_rsid(raw_id):
            idxs = rsid_index.get(raw_id.lower())
            if idxs:
                rows = df_annot.iloc[idxs].copy()
                rows["_list_d_id"] = raw_id
        # Try chr:pos
        if rows is None:
            parsed = _parse_chr_pos(raw_id)
            if parsed:
                idxs = chrpos_index.get(parsed)
                if idxs:
                    rows = df_annot.iloc[idxs].copy()
                    rows["_list_d_id"] = raw_id
        if rows is not None:
            matched_rows.append(rows)
        else:
            unmatched.append(raw_id)

    if not matched_rows:
        return pd.DataFrame()

    df_enriched = pd.concat(matched_rows, ignore_index=True)
    return df_enriched, unmatched  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Pair generation helpers
# ---------------------------------------------------------------------------


def _estimate_pairs(
    strategy: str,
    df: pd.DataFrame,
    seed_ids: set[str],
    exclude_same_gene: bool,
) -> int:
    """Fast upper-bound estimate — does not materialise pairs."""
    n = len(df)
    if strategy == "all_vs_all":
        return n * (n - 1) // 2
    if strategy == "seed_vs_all":
        n_seed = len(seed_ids)
        n_other = n - n_seed
        return n_seed * n_other
    if strategy == "cross_gene":
        # Upper bound = all_vs_all; actual will be lower after same-gene exclusion
        return n * (n - 1) // 2
    return n * (n - 1) // 2  # fallback


def _generate_pairs(
    strategy: str,
    df: pd.DataFrame,
    seed_ids: set[str],
    exclude_same_gene: bool,
    annotation_cols: list[str],
) -> pd.DataFrame:
    """
    Materialise pairs as a DataFrame with columns suffixed _a and _b.
    Uses numpy triu_indices for all_vs_all / cross_gene (no Python loop).
    Uses cartesian merge for seed_vs_all.
    """

    def _side(df_sub: pd.DataFrame, suffix: str) -> pd.DataFrame:
        return df_sub[annotation_cols].rename(
            columns={c: f"{c}{suffix}" for c in annotation_cols}
        )

    if strategy == "seed_vs_all":
        gene_col = next((c for c in ["gene_symbol", "gene"] if c in df.columns), None)
        df_seed = df[df["_list_d_id"].isin(seed_ids)].reset_index(drop=True)
        df_other = df[~df["_list_d_id"].isin(seed_ids)].reset_index(drop=True)

        df_seed = df_seed.assign(_key=1)
        df_other = df_other.assign(_key=1)
        pairs = df_seed.merge(df_other, on="_key", suffixes=("_a", "_b")).drop(
            columns="_key"
        )

        # Rename annotation columns that were automatically suffixed by merge
        # The merge already appended _a/_b because of suffixes parameter
        # No further renaming needed

        if exclude_same_gene and gene_col:
            before = len(pairs)
            pairs = pairs[
                pairs[f"{gene_col}_a"] != pairs[f"{gene_col}_b"]
            ]
            after = len(pairs)
            if before != after:
                pairs = pairs.reset_index(drop=True)

        return pairs

    # all_vs_all / cross_gene — use numpy upper triangular indices
    df = df.reset_index(drop=True)
    n = len(df)
    idx_a, idx_b = np.triu_indices(n, k=1)

    side_a = df.iloc[idx_a][annotation_cols].reset_index(drop=True)
    side_b = df.iloc[idx_b][annotation_cols].reset_index(drop=True)

    side_a.columns = [f"{c}_a" for c in annotation_cols]
    side_b.columns = [f"{c}_b" for c in annotation_cols]

    pairs = pd.concat([side_a, side_b], axis=1)

    gene_col = next((c for c in ["gene_symbol", "gene"] if c in df.columns), None)

    if strategy == "cross_gene" and gene_col:
        pairs = pairs[pairs[f"{gene_col}_a"] != pairs[f"{gene_col}_b"]].reset_index(
            drop=True
        )
    elif exclude_same_gene and gene_col:
        pairs = pairs[pairs[f"{gene_col}_a"] != pairs[f"{gene_col}_b"]].reset_index(
            drop=True
        )

    return pairs


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


class SNPSNPPairGeneratorReport(ReportBase):
    name = "snp_snp_pair_generator"
    description = (
        "Phase 3 of the SNP×SNP interaction pipeline. "
        "Takes a LD-pruned variant list (Lista D) and an annotated variant source "
        "(Lista A from gene_to_variant_filtering), generates all variant pairs "
        "according to the chosen strategy, and returns a fully annotated pair "
        "DataFrame ready for statistical interaction testing."
    )

    @classmethod
    def example_input(cls) -> dict:
        return {
            "variant_list":       "pipeline_output/lista_D.prune.in",
            "annotation_source":  "pipeline_output/lista_A.csv",
            "pairing_strategy":   "seed_vs_all",
            "seed_gene":          "APOE",
            "seed_variants":      None,
            "max_pairs":          _DEFAULT_MAX_PAIRS,
            "exclude_same_gene":  True,
        }

    @classmethod
    def available_columns(cls) -> list[str]:
        suffixes = ["_a", "_b"]
        cols = []
        for c in _PREFERRED_COLS:
            for s in suffixes:
                cols.append(f"{c}{s}")
        cols += ["same_gene", "pairing_strategy"]
        return cols

    def run(self) -> pd.DataFrame:
        # ------------------------------------------------------------------ #
        # 1. Parameters
        # ------------------------------------------------------------------ #
        _id = self.param("input_data", default=None)
        if isinstance(_id, dict):
            for k, v in _id.items():
                self.params.setdefault(k, v)

        variant_list      = Path(self.param("variant_list",      required=True))
        annotation_source = Path(self.param("annotation_source", required=True))
        pairing_strategy  = self.param("pairing_strategy",  default="seed_vs_all")
        seed_gene         = self.param("seed_gene",          default=None)
        seed_variants     = self.param("seed_variants",      default=None)
        max_pairs         = int(self.param("max_pairs",      default=_DEFAULT_MAX_PAIRS))
        exclude_same_gene = bool(self.param("exclude_same_gene", default=True))

        valid_strategies = {"seed_vs_all", "cross_gene", "all_vs_all"}
        if pairing_strategy not in valid_strategies:
            raise ValueError(
                f"pairing_strategy must be one of {sorted(valid_strategies)}, "
                f"got {pairing_strategy!r}"
            )

        if not variant_list.exists():
            raise FileNotFoundError(f"variant_list not found: {variant_list}")
        if not annotation_source.exists():
            raise FileNotFoundError(f"annotation_source not found: {annotation_source}")

        # ------------------------------------------------------------------ #
        # 2. Load inputs
        # ------------------------------------------------------------------ #
        self.logger.log(f"Loading Lista D: {variant_list}")
        lista_d_ids = _load_variant_list(variant_list)
        self.logger.log(f"  → {len(lista_d_ids):,} variant IDs")

        self.logger.log(f"Loading annotation source: {annotation_source}")
        df_annot = pd.read_csv(annotation_source, low_memory=False)
        self.logger.log(f"  → {len(df_annot):,} rows, {df_annot.columns.tolist()[:6]}…")

        # ------------------------------------------------------------------ #
        # 3. Enrich Lista D with annotations from Lista A
        # ------------------------------------------------------------------ #
        result = _enrich(lista_d_ids, df_annot)
        if isinstance(result, tuple):
            df_enriched, unmatched = result
        else:
            df_enriched, unmatched = result, []

        if df_enriched.empty:
            self.logger.log("No variants from Lista D matched annotation_source.")
            return pd.DataFrame(
                [{"resolution_status": "no_variants_matched", "variant_list": str(variant_list)}]
            )

        n_enriched = df_enriched["_list_d_id"].nunique()
        self.logger.log(
            f"Enriched: {n_enriched:,}/{len(lista_d_ids):,} variants matched "
            f"({len(unmatched):,} not found in annotation_source)"
        )

        # ------------------------------------------------------------------ #
        # 4. Resolve seed IDs
        # ------------------------------------------------------------------ #
        seed_ids: set[str] = set()

        if pairing_strategy == "seed_vs_all":
            # Option A: explicit variant list
            if seed_variants:
                seed_ids = {
                    str(v).strip()
                    for v in (
                        seed_variants
                        if isinstance(seed_variants, (list, tuple, set))
                        else [seed_variants]
                    )
                }
            # Option B: resolve by gene symbol
            elif seed_gene:
                gene_col = next(
                    (c for c in ["gene_symbol", "gene"] if c in df_enriched.columns),
                    None,
                )
                if gene_col:
                    seed_ids = set(
                        df_enriched[df_enriched[gene_col] == seed_gene][
                            "_list_d_id"
                        ].unique()
                    )

            if not seed_ids:
                return pd.DataFrame(
                    [
                        {
                            "resolution_status": "seed_not_found",
                            "seed_gene": seed_gene,
                            "seed_variants": str(seed_variants),
                            "note": "No seed variants found in the enriched list. "
                            "Verify seed_gene or seed_variants match the annotation_source.",
                        }
                    ]
                )

            self.logger.log(
                f"Seed resolved: {len(seed_ids):,} seed variants "
                f"(gene={seed_gene!r})"
            )

        # ------------------------------------------------------------------ #
        # 5. Estimate pair count — safety check
        # ------------------------------------------------------------------ #
        # Deduplicate enriched list to one row per variant ID for estimation.
        df_dedup = df_enriched.drop_duplicates(subset=["_list_d_id"])
        n_estimated = _estimate_pairs(
            pairing_strategy, df_dedup, seed_ids, exclude_same_gene
        )

        self.logger.log(
            f"Estimated pairs ({pairing_strategy}): {n_estimated:,} "
            f"(limit={max_pairs:,})"
        )

        if n_estimated > max_pairs:
            suggestion = _suggest_reduction(
                pairing_strategy, n_estimated, max_pairs, len(seed_ids)
            )
            self.logger.log(f"ABORT: pair limit exceeded. {suggestion}")
            return pd.DataFrame(
                [
                    {
                        "resolution_status": "pair_limit_exceeded",
                        "estimated_pairs":   n_estimated,
                        "max_pairs":         max_pairs,
                        "pairing_strategy":  pairing_strategy,
                        "suggestion":        suggestion,
                    }
                ]
            )

        # ------------------------------------------------------------------ #
        # 6. Determine annotation columns to carry
        # ------------------------------------------------------------------ #
        preferred = [c for c in _PREFERRED_COLS if c in df_enriched.columns]
        extra = [
            c
            for c in df_enriched.columns
            if c not in preferred and not c.startswith("_")
        ]
        annotation_cols = preferred + extra

        # ------------------------------------------------------------------ #
        # 7. Generate pairs
        # ------------------------------------------------------------------ #
        self.logger.log(f"Generating pairs (strategy={pairing_strategy!r}) …")

        df_pairs = _generate_pairs(
            pairing_strategy,
            df_enriched,
            seed_ids,
            exclude_same_gene,
            annotation_cols,
        )

        # ------------------------------------------------------------------ #
        # 8. Add metadata columns
        # ------------------------------------------------------------------ #
        gene_a_col = "gene_symbol_a" if "gene_symbol_a" in df_pairs.columns else None
        gene_b_col = "gene_symbol_b" if "gene_symbol_b" in df_pairs.columns else None

        if gene_a_col and gene_b_col:
            df_pairs["same_gene"] = df_pairs[gene_a_col] == df_pairs[gene_b_col]

        df_pairs["pairing_strategy"] = pairing_strategy

        self.logger.log(
            f"Pairs generated: {len(df_pairs):,} | "
            f"unique variants A: {df_pairs.get('rsid_a', df_pairs.get('variant_id_a', pd.Series())).nunique()} | "
            f"unique variants B: {df_pairs.get('rsid_b', df_pairs.get('variant_id_b', pd.Series())).nunique()}"
        )

        return df_pairs.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Helper: reduction suggestion for pair_limit_exceeded
# ---------------------------------------------------------------------------


def _suggest_reduction(
    strategy: str,
    n_estimated: int,
    max_pairs: int,
    n_seed: int,
) -> str:
    ratio = n_estimated / max_pairs
    tips = [
        f"Estimated {n_estimated:,} pairs exceeds max_pairs={max_pairs:,} "
        f"({ratio:.1f}× over limit)."
    ]
    if strategy == "all_vs_all":
        tips.append(
            "Switch to pairing_strategy='seed_vs_all' with a seed_gene to dramatically "
            "reduce the number of pairs."
        )
    if strategy in ("all_vs_all", "cross_gene"):
        tips.append(
            "Add exclude_same_gene=True or apply stricter filters in Phase 2 "
            "(e.g., af_max, impact_filter) to reduce Lista A before this step."
        )
    tips.append(
        f"Alternatively, increase max_pairs to {n_estimated:,} if your environment "
        "can handle it."
    )
    return " ".join(tips)
