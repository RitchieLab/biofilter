#!/usr/bin/env python3
"""
Step 2 — narrow step 1's variants by functional consequence.

Specified in the 2026-09-03 message, and confirmed as a **partition** on
2026-09-04:

  "Okay, so the new plan is to prioritize functional consequence, with differing
   definitions of 'functional' criteria for noncoding and coding variants."
                                                                   — Diane Xue

  "Yes, it should be a partition I think. There are different inclusion criteria
   for coding and non-coding variants."                            — Diane Xue

So each variant goes down exactly one branch, decided by whether it has a coding
consequence at all:

  branch A  coding variants      must alter the protein
  branch B  non-coding variants  must be a brain eQTL *and* a pQTL for the
                                 same gene

A variant with a coding consequence is never tested against the regulatory
criterion, even when it has strong regulatory evidence. That is what makes it a
partition rather than two filters, and it is the part that differs from the
first pass at this step.

The gene a branch B variant pairs on is the **QTL target**, not the gene it sits
inside:

  "We want to keep all noncoding variants where the eQTL and pQTL point to the
   same gene, regardless of what gene the variant actually sits in."
                                                                   — Diane Xue

Where the data comes from
-------------------------
eQTL   `expand_variant_regulatory`, from GTEx in the bundle. The current build
       carries 13 brain tissues and eQTL only — no sQTL.
pQTL   read straight from the FunGen xQTL file. It is **not** in BF4, so this
       script is the only place that knows where that data came from. If the
       requirement outlives this analysis it should become a DTP, the way GTEx
       eQTL is.

Usage
-----
    python step_02_adsp_functional_filter.py \
        --input   outputs/step1_per_variant_coding.csv \
        --pqtl    data/brain_pQTL_hmt_variant_gene_lookup.tsv.gz \
        --out-dir outputs
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from biofilter import Biofilter

# Diane's ten consequences are exactly VEP severity rank <= 14, so the rank is
# what the code uses — one number instead of a list to keep in sync.
PROTEIN_ALTERING_MAX_RANK = 14


def load_step1(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    missing = {"variant", "coding_gene", "gene_entity_id", "gene_ensembl_id",
               "consequence_category", "severity_rank"} - set(df.columns)
    if missing:
        sys.exit(
            f"{path} is missing {sorted(missing)}. It should be step 1's "
            f"--out-coding file; re-run step 1 if it predates gene_ensembl_id."
        )
    return df


def check_same_bundle(step1_path: Path, bundle_id: str | None) -> str | None:
    """
    Step 1's ids only mean something next to the bundle that made them.

    Reading a product built from another bundle would join `gene_entity_id`
    against a different id space and silently produce rows that look fine.
    """
    sidecar = Path(f"{step1_path}.provenance.json")
    if not sidecar.exists():
        return None
    recorded = json.loads(sidecar.read_text()).get("bundle_id")
    if recorded and bundle_id and recorded != bundle_id:
        sys.exit(
            f"Step 1 was built from bundle {recorded}, this one is {bundle_id}. "
            f"Entity ids are scoped to a bundle — re-run step 1 against this one."
        )
    return recorded


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="step 1's --out-coding CSV")
    ap.add_argument("--pqtl", required=True, help="FunGen xQTL brain pQTL lookup .tsv.gz")
    ap.add_argument("--bundle", default=None, help="bundle directory")
    ap.add_argument("--out-dir", default="outputs")
    ap.add_argument(
        "--eqtl-p-max",
        type=float,
        default=None,
        help="keep only eQTL evidence at least this significant. Off by default: "
        "the pQTL requirement already does the filtering, and a cutoff at 1e-4 "
        "removes 2.4%% of the branch while one at 1e-8 removes half of it.",
    )
    args = ap.parse_args()

    step1_path = Path(args.input).expanduser().resolve()
    pqtl_path = Path(args.pqtl).expanduser().resolve()
    for path in (step1_path, pqtl_path):
        if not path.exists():
            sys.exit(f"File not found: {path}")
    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    bf = Biofilter(bundle=args.bundle) if args.bundle else Biofilter()

    step1 = load_step1(step1_path)

    # ------------------------------------------------------------------
    # 1. The partition
    # ------------------------------------------------------------------
    coding_rows = step1[step1.consequence_category.eq("coding")]
    coding_variants = set(coding_rows.variant)
    non_coding = sorted(set(step1.variant) - coding_variants)

    # ------------------------------------------------------------------
    # 2. Branch A — coding variants that alter the protein
    # ------------------------------------------------------------------
    branch_a = (
        coding_rows[coding_rows.severity_rank.le(PROTEIN_ALTERING_MAX_RANK)]
        .drop_duplicates(["variant", "gene_entity_id"])
        .copy()
    )
    branch_a["branch"] = "A_protein_altering"
    branch_a["pair_gene_entity_id"] = branch_a.gene_entity_id
    branch_a["pair_gene_id"] = branch_a.gene_ensembl_id
    branch_a["pair_gene_symbol"] = branch_a.coding_gene

    # ------------------------------------------------------------------
    # 3. Branch B — non-coding variants with eQTL and pQTL on the same gene
    # ------------------------------------------------------------------
    result = bf.report.run("expand_variant_regulatory", input_data=non_coding)
    bundle_id = result.provenance.get("bundle_id")
    check_same_bundle(step1_path, bundle_id)

    eqtl = result.to_pandas()
    eqtl = eqtl[eqtl.status.eq("ok") & eqtl.regulated_gene_id.notna()]
    if args.eqtl_p_max is not None:
        eqtl = eqtl[eqtl.p_value.le(args.eqtl_p_max)]

    pqtl = pd.read_csv(pqtl_path, sep="\t")
    # The pQTL file carries a `chr` prefix on the variant id; the bundle does
    # not. The gene join is on Ensembl id, which both sides use.
    pqtl["variant"] = pqtl.variant_id.str.replace(r"^chr", "", regex=True)

    both = eqtl.merge(
        pqtl,
        left_on=["input_value", "regulated_gene_id"],
        right_on=["variant", "target_ensembl_id"],
        how="inner",
    )

    # One row per (variant, target gene): the tissue detail collapses into counts
    # so the product has the same grain as branch A.
    branch_b = (
        both.groupby(["input_value", "regulated_gene_id"], as_index=False)
        .agg(
            pair_gene_symbol=("regulated_gene_symbol", "first"),
            chromosome=("chromosome", "first"),
            position=("position", "first"),
            n_eqtl_tissues=("bio_context", "nunique"),
            eqtl_p_min=("p_value", "min"),
            n_pqtl_cohorts=("n_pqtl_cohorts", "max"),
            pqtl_fdr_min=("min_FDR", "min"),
        )
        .rename(columns={"input_value": "variant", "regulated_gene_id": "pair_gene_id"})
    )
    branch_b["branch"] = "B_regulatory"

    # The QTL target arrives as an Ensembl id. Resolving it to a BF4 entity id
    # belongs here, where the gene is chosen, rather than three steps later:
    # both branches then leave step 2 carrying the same two identifiers, and
    # nothing downstream has to know they came from different places.
    resolved = bf.report.run(
        "annotate_gene",
        input_data=branch_b.pair_gene_id.dropna().unique().tolist(),
        include_variant_summary=False,
    ).to_pandas()
    lookup = resolved.loc[resolved.entity_id.notna(), ["input_value", "entity_id"]].rename(
        columns={"input_value": "pair_gene_id", "entity_id": "pair_gene_entity_id"}
    )
    branch_b = branch_b.merge(lookup, on="pair_gene_id", how="left")

    # ------------------------------------------------------------------
    # 4. The product
    # ------------------------------------------------------------------
    a_out = branch_a[[
        "variant", "chromosome", "position", "branch", "pair_gene_symbol",
        "pair_gene_id", "pair_gene_entity_id", "consequence", "severity_rank",
    ]]
    b_out = branch_b[[
        "variant", "chromosome", "position", "branch", "pair_gene_symbol",
        "pair_gene_id", "pair_gene_entity_id",
        "n_eqtl_tissues", "eqtl_p_min", "n_pqtl_cohorts", "pqtl_fdr_min",
    ]]
    product = pd.concat([a_out, b_out], ignore_index=True).sort_values(
        ["chromosome", "position", "branch"]
    )

    overlap = set(a_out.variant) & set(b_out.variant)

    summary = [
        ("bundle_id", bundle_id),
        ("step1_variants", step1.variant.nunique()),
        ("coding_variants", len(coding_variants)),
        ("non_coding_variants", len(non_coding)),
        ("branch_a_variants", a_out.variant.nunique()),
        ("branch_a_dropped_not_protein_altering", len(coding_variants) - a_out.variant.nunique()),
        ("eqtl_variants", eqtl.input_value.nunique()),
        ("eqtl_target_genes", eqtl.regulated_gene_id.nunique()),
        ("branch_b_variants", b_out.variant.nunique()),
        ("branch_b_target_genes", b_out.pair_gene_id.nunique()),
        ("genes_without_an_entity", int(product.pair_gene_entity_id.isna().sum())),
        ("total_variants", product.variant.nunique()),
        ("branch_overlap", len(overlap)),
    ]
    width = max(len(k) for k, _ in summary)
    print()
    for key, value in summary:
        print(f"{key:<{width}} : {value}")

    if overlap:
        sys.exit(f"\nBranches overlap on {len(overlap)} variants — not a partition.")

    product_path = out_dir / "step2_selected_variants.csv"
    product.to_csv(product_path, index=False)
    pd.DataFrame(summary, columns=["metric", "value"]).to_csv(
        out_dir / "step2_summary.csv", index=False
    )
    Path(f"{product_path}.provenance.json").write_text(
        json.dumps(
            {
                "step": "adsp_step_02_functional_filter",
                "bundle_id": bundle_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "step1_input": str(step1_path),
                "pqtl_file": str(pqtl_path),
                "pqtl_in_bundle": False,
                "reports": ["expand_variant_regulatory"],
                "partition": {
                    "A_protein_altering": f"consequence_category=coding and severity_rank<={PROTEIN_ALTERING_MAX_RANK}",
                    "B_regulatory": "consequence_category!=coding, brain eQTL and pQTL on the same gene",
                },
                "eqtl_p_max": args.eqtl_p_max,
                "variants": int(product.variant.nunique()),
                "rows": len(product),
            },
            indent=2,
        )
    )
    print(f"\nproduct    -> {product_path}  ({len(product):,} rows)")
    print(f"provenance -> {product_path}.provenance.json")


if __name__ == "__main__":
    main()
