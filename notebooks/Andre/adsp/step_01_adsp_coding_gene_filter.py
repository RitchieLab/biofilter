#!/usr/bin/env python3
"""
Step 1 — keep variants that VEP maps to a protein-coding gene.

Specified in the 2026-08-28 meeting:

  "You want to filter only variants that map to protein coding genes...
   using VEP, because VEP will differentiate between them."         — Diane Xue

  "I think what you should do is drop the genes first, rather than dropping
   the variants. Restrict only to genes that are protein coding."  — Diane Xue

So the order is: restrict the gene list first, then keep any variant VEP links
to a surviving gene. The variant's consequence is irrelevant here — an intron,
synonymous or regulatory variant of a protein-coding gene is kept. Only
variants whose sole links are to lncRNAs, pseudogenes and similar are dropped.

Two report calls and a join, nothing else:

  annotate_variant  — which genes VEP links each variant to, and how
  annotate_gene     — what kind of gene each of those is, from HGNC

Neither call is per-variant. What `annotate_variant` costs follows the
chromosomes the input touches rather than how many variants it holds: 12,643
chr22 variants take 0.8 s, while 20 variants split across chr1 and chr22 take
2.0 s. Splitting the input into one call per chromosome returns identical rows
in 17.8 s against 40.3 s for one call, which is worth measuring before a long
run.

Usage
-----
    python step_01_adsp_coding_gene_filter.py \
        --input data/adsp_variants.csv \
        --out-summary outputs/step1_summary.csv \
        --out-detail outputs/step1_per_variant.csv \
        --out-coding outputs/step1_per_variant_coding.csv

The bundle comes from .biofilter.toml. Pass --bundle to override, which is
what the LPC job does:

    --bundle /project/hall_shared/datasets/biofilter/<YYYYMMDD>

A bundle is a directory — the one holding manifest.json, not its tables/.

Output
------
`--out-coding` is the product: one row per (variant, protein-coding gene).
Join downstream on `gene_entity_id`, not on `coding_gene` — the displayed name
is the one VEP used, which may be an alias of the entity it resolved to, and
those ids are scoped to the bundle that produced them.
`--out-detail` carries every input, kept or not, so each decision is auditable —
including `unresolved_vep_genes`, which records VEP gene references that have no
matching entity in BF4 rather than letting them vanish.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from biofilter import Biofilter

CODING_LOCUS_GROUP = "protein-coding gene"

# HGNC's placeholder for a missing value, not a locus type.
UNLABELLED = "unknown"

CHROM_CODE = {"X": 23, "Y": 24, "M": 25, "MT": 25}


def read_input(path: Path) -> pd.DataFrame:
    """One variant per line, first column used. `1:633963:C:T`, `chr1:633963:C:T`."""
    raw = pd.read_csv(path, header=None, usecols=[0], names=["variant"], dtype=str)
    raw["variant"] = raw.variant.str.strip()
    raw = raw[raw.variant.notna() & raw.variant.ne("")].drop_duplicates()

    # Parsed here only so the audit file can place an input the bundle never
    # matched: the report returns chromosome and position as NULL for those.
    parts = raw.variant.str.extract(
        r"^(?:chr)?([0-9]{1,2}|[XYxyMm][Tt]?)[:_\-](\d+)", expand=True
    )
    chrom = parts[0].str.upper()
    raw["chromosome"] = pd.to_numeric(chrom.replace(CHROM_CODE), errors="coerce")
    raw["position"] = pd.to_numeric(parts[1], errors="coerce")
    return raw.reset_index(drop=True)


def vep_links(bf: Biofilter, variants: list[str]) -> pd.DataFrame:
    """Every (variant, transcript) VEP annotated, with the gene it names."""
    result = bf.report.run("annotate_variant", input_data=variants)
    df = result.to_pandas()
    # gene_symbol first, Ensembl gene_id as the fallback — the same name the
    # gene list is looked up under below.
    df["vep_gene"] = df.gene_symbol.fillna(df.gene_id)
    return df, result.provenance


def gene_kinds(bf: Biofilter, names: list[str]) -> pd.DataFrame:
    """HGNC locus group and type for each VEP gene name, resolved through aliases."""
    # include_variant_summary=False: this step never reads
    # variant_count_in_gene_range, and computing it is a range join against the
    # variant tables — 30.4 s of the 30.6 s these 20,347 genes take with it on.
    df = bf.report.run(
        "annotate_gene", input_data=names, include_variant_summary=False
    ).to_pandas()
    return df[
        [
            "input_value",
            "entity_id",
            "ensembl_id",
            "gene_symbol",
            "gene_locus_group",
            "gene_locus_type",
        ]
    ].rename(
        columns={
            "input_value": "vep_gene",
            "gene_symbol": "resolved_gene_symbol",
            "ensembl_id": "gene_ensembl_id",
        }
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="one variant per line")
    ap.add_argument("--bundle", default=None, help="bundle directory")
    ap.add_argument("--out-summary", default="step1_summary.csv")
    ap.add_argument("--out-detail", default=None, help="per-variant CSV, all inputs")
    ap.add_argument(
        "--out-coding",
        default=None,
        help="per-variant CSV restricted to the variants that passed — this is "
        "what step 2 consumes",
    )
    ap.add_argument(
        "--allow-unlabelled-genes",
        action="store_true",
        help="keep protein-coding genes that have no HGNC locus type. These come "
        "from the NCBI DTP (LOC*/ENSG* identifiers with no HGNC name); their "
        "protein-coding status is our ETL's reading of NCBI, not an HGNC "
        "assertion. Excluded by default so the gene list has one provenance.",
    )
    args = ap.parse_args()

    src = Path(args.input).expanduser().resolve()
    if not src.exists():
        sys.exit(f"Input file not found: {src}")

    bf = Biofilter(bundle=args.bundle) if args.bundle else Biofilter()

    # ------------------------------------------------------------------
    # 1. The input
    # ------------------------------------------------------------------
    inputs = read_input(src)
    total = len(inputs)

    # ------------------------------------------------------------------
    # 2. What VEP links each variant to
    # ------------------------------------------------------------------
    links, provenance = vep_links(bf, inputs.variant.tolist())
    found = links[links.status != "not_found"]
    matched = found.input_value.nunique()

    # ------------------------------------------------------------------
    # 3. "Drop the genes first" — what kind of gene each of those is
    # ------------------------------------------------------------------
    names = found.vep_gene.dropna().unique().tolist()
    genes = gene_kinds(bf, names)
    n_coding_genes = int(genes.gene_locus_group.eq(CODING_LOCUS_GROUP).sum())

    annotated = found.merge(genes, on="vep_gene", how="left")

    # ------------------------------------------------------------------
    # 4. Keep the variant if any of its genes survived
    # ------------------------------------------------------------------
    is_coding = annotated.gene_locus_group.eq(CODING_LOCUS_GROUP)
    is_labelled = annotated.gene_locus_type.notna() & annotated.gene_locus_type.ne(
        UNLABELLED
    )
    keep = is_coding if args.allow_unlabelled_genes else (is_coding & is_labelled)

    # One row per (variant, coding gene), carrying the most severe consequence
    # VEP reported for that pair. A variant touching two genes gets two rows, so
    # "upstream of GENE" has somewhere to live.
    coding = (
        annotated[keep]
        .sort_values("severity_rank")
        .drop_duplicates(["input_value", "entity_id"])
        .copy()
    )
    coding["relation_to_gene"] = (
        coding.consequence.map(
            {
                "upstream_gene_variant": "upstream of gene",
                "downstream_gene_variant": "downstream of gene",
            }
        ).fillna("within gene")
    )
    coding["n_coding_genes"] = coding.groupby("input_value").input_value.transform("size")

    # ------------------------------------------------------------------
    # 5. The audit: every input, kept or not, with the reason
    # ------------------------------------------------------------------
    unresolved = annotated.vep_gene.notna() & annotated.entity_id.isna()

    def joined(mask: pd.Series) -> pd.Series:
        return (
            annotated[mask]
            .groupby("input_value")
            .vep_gene.agg(lambda s: ";".join(sorted(set(s.dropna()))))
            .replace("", pd.NA)
        )

    audit = inputs.rename(columns={"variant": "input_variant"}).set_index("input_variant")
    audit["found_in_db"] = audit.index.isin(found.input_value)
    audit["has_vep_gene"] = audit.index.isin(annotated.loc[annotated.vep_gene.notna(), "input_value"])
    audit["maps_to_coding_gene"] = audit.index.isin(coding.input_value)
    audit["vep_coding_genes"] = joined(keep)
    audit["vep_all_genes"] = joined(annotated.vep_gene.notna())
    audit["unresolved_vep_genes"] = joined(unresolved)
    # Two labels from two sources: the transcript is protein_coding (gnomAD/VEP)
    # and the gene has no BF4 entity to ask HGNC about. The flag does not assert
    # these genes code for proteins — it records that VEP suggests they do and we
    # could not confirm it.
    hint = annotated[unresolved & annotated.biotype.eq("protein_coding")].input_value
    audit["unresolved_coding_hint"] = audit.index.isin(hint)
    audit = audit.reset_index()

    # ------------------------------------------------------------------
    # 6. Report
    # ------------------------------------------------------------------
    # Only variants the bundle actually holds can be *dropped* by the filter.
    # An input the bundle has never seen was never a candidate, and counting it
    # here would let a partial bundle look like a strict filter.
    dropped = audit[audit.found_in_db & ~audit.maps_to_coding_gene]
    coding_before_label = annotated.loc[is_coding, "input_value"].nunique()
    summary = [
        ("bundle_id", provenance.get("bundle_id")),
        ("input_variants", total),
        ("matched_in_bundle", matched),
        ("not_in_bundle", total - matched),
        ("protein_coding_genes_in_gene_list", n_coding_genes),
        ("variants_mapped_to_coding_gene", int(audit.maps_to_coding_gene.sum())),
        (
            "variants_mapped_to_coding_gene_pct",
            round(100.0 * audit.maps_to_coding_gene.sum() / matched, 2) if matched else 0.0,
        ),
        ("variant_gene_rows", len(coding)),
        (
            "dropped_non_coding_gene_only",
            int((dropped.has_vep_gene & dropped.unresolved_vep_genes.isna()).sum()),
        ),
        (
            "dropped_with_unresolved_vep_gene",
            int((dropped.has_vep_gene & dropped.unresolved_vep_genes.notna()).sum()),
        ),
        ("dropped_no_vep_gene", int((~dropped.has_vep_gene).sum())),
        ("unresolved_but_coding_biotype", int(dropped.unresolved_coding_hint.sum())),
        # The HGNC locus-type restriction, stated as its own cost rather than
        # folded into the counts above.
        (
            "dropped_unlabelled_gene_only",
            coding_before_label - int(audit.maps_to_coding_gene.sum()),
        ),
    ]

    width = max(len(k) for k, _ in summary)
    print()
    print(f"Input  : {src}")
    print("-" * (width + 22))
    for key, value in summary:
        print(f"{key:<{width}} : {value}")
    print("-" * (width + 22))

    pd.DataFrame(summary, columns=["metric", "value"]).to_csv(args.out_summary, index=False)
    print(f"summary -> {args.out_summary}")

    if args.out_detail:
        audit.sort_values(["chromosome", "position"]).to_csv(args.out_detail, index=False)
        print(f"per-variant (all)     -> {args.out_detail}  ({total:,} rows)")

    if args.out_coding:
        # One row per (variant, coding gene). Only protein-coding genes appear,
        # so nothing in this file says "non-coding" — that is the point.
        product = coding.rename(
            columns={
                "input_value": "variant",
                "vep_gene": "coding_gene",
                "entity_id": "gene_entity_id",
            }
        )[
            [
                "variant",
                "chromosome",
                "position",
                "coding_gene",
                # Two identifiers, because they do two different jobs. The
                # entity id is this bundle's join key — it is what makes the
                # provenance meaningful, and it means nothing outside the build
                # that minted it. The Ensembl id is the portable one: what you
                # hand to another tool, or back to a report that resolves names
                # through aliases. Feeding the join key to a resolver is what
                # step 3 got wrong once, silently.
                "gene_entity_id",
                "gene_ensembl_id",
                "relation_to_gene",
                "consequence",
                # Carried because step 2 partitions on it and this is where the
                # value already exists. "coding" here is the category of the
                # most severe consequence for the pair, which is the same set of
                # variants as "any coding consequence at all": coding
                # consequences all rank above non-coding ones.
                "consequence_category",
                "severity_rank",
                "gene_locus_group",
                "gene_locus_type",
                "n_coding_genes",
            ]
        ].copy()
    # The report returns these as float64 — nullable ints that went through
    # pandas. Nothing here is null after the filter, and step 2 joins on them.
    for column in ("chromosome", "position", "gene_entity_id", "severity_rank"):
        product[column] = product[column].astype("int64")
        product.sort_values(["chromosome", "position", "coding_gene"]).to_csv(
            args.out_coding, index=False
        )
        # `gene_entity_id` is scoped to the bundle that produced it, so the file
        # does not travel without saying which one. Same contract as the
        # sidecar a ReportResult.write() leaves beside its own output.
        sidecar = Path(f"{args.out_coding}.provenance.json")
        sidecar.write_text(
            json.dumps(
                {
                    "step": "adsp_step_01_coding_gene_filter",
                    "bundle_id": provenance.get("bundle_id"),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "input_file": str(src),
                    "reports": ["annotate_variant", "annotate_gene"],
                    "filter": {
                        "gene_locus_group": CODING_LOCUS_GROUP,
                        "require_hgnc_locus_type": not args.allow_unlabelled_genes,
                    },
                    "rows": len(product),
                    "variants": int(product.variant.nunique()),
                },
                indent=2,
            )
        )
        print(
            f"variant x gene (kept) -> {args.out_coding}  "
            f"({len(product):,} rows over {product.variant.nunique():,} variants)"
        )
        print(f"provenance            -> {sidecar}")


if __name__ == "__main__":
    main()
