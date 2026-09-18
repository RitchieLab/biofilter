#!/usr/bin/env python3
"""
Step 3 — turn step 2's variants into candidate variant pairs (models).

A model is one variant x variant pair to test for interaction. Two variants are
worth pairing when the genes they were selected for share biology: a pathway, a
disease, a protein complex.

Why `pair_genes` and not `pair_variants`
----------------------------------------
`pair_variants` derives "this variant belongs to this gene" from coordinates.
That is right for a coding variant and wrong for a regulatory one: a branch B
variant is attached to the gene its QTL points at, which it usually does not sit
inside. `pair_variants` would look for it among that gene's positional variants,
not find it, and drop it — with no error.

`pair_genes` takes the attachment from us instead of re-deriving it. We supply
the gene -> variant mapping the first two steps produced, and the report pairs
the genes and expands our list across the pairs it finds.

It also owns the three rules that are easy to get wrong and expensive to miss:
pairs are unordered, deduplication is global across the whole answer, and an
item on both genes of a pair must not pair with itself.

Usage
-----
    python step_03_adsp_variant_pairs.py \
        --input   outputs/step2_selected_variants.csv \
        --out-dir outputs \
        --max-group-size 500 \
        --min-group-sources 1
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from biofilter import Biofilter

#: The space our gene ids live in. Steps 1 and 2 hand every gene an Ensembl id
#: precisely so this can be named rather than guessed.
GENE_IDENTIFIER = "ensembl"


def gene_to_variant_map(step2: pd.DataFrame):
    """
    The mapping `pair_genes` expands over: gene -> the variants we chose for it.

    Step 2 now hands both branches the same two identifiers, so there is nothing
    to reconcile here any more. Which one to use is still a decision:

    **Ensembl, not the BF4 entity id.** `pair_genes` resolves its input through
    aliases, and an entity id is not an alias — it is this bundle's join key. A
    bare entity id either fails to resolve or, worse, matches an Entrez id
    belonging to a different gene. Measured once on 400 of ours: 304 did not
    resolve and all 96 that did came back as the wrong gene, silently.

    **Not the symbol either.** Symbols are ambiguous; the report warns when a
    name in the mapping answers to more than one gene, and with symbols that was
    111 names against 8 with Ensembl ids.
    """
    mapping = step2.loc[
        step2.pair_gene_id.notna(), ["variant", "pair_gene_id", "branch"]
    ].rename(columns={"pair_gene_id": "ensembl_id"})
    mapping["branch"] = mapping.branch.str[0]  # A_protein_altering -> A
    # A gene the bundle has no entity for is still passed to the report: its
    # Ensembl id is a real identifier, and the report is the right place to say
    # it cannot pair it. Counting it here is for the summary, not a filter.
    unpairable = step2[step2.pair_gene_id.isna() | step2.pair_gene_entity_id.isna()]
    return mapping.drop_duplicates(["variant", "ensembl_id"]), unpairable


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="step 2's selected variants CSV")
    ap.add_argument("--bundle", default=None)
    ap.add_argument("--out-dir", default="outputs")
    ap.add_argument("--group-types", default="Pathways",
                    help="comma-separated: Pathways, Diseases, Proteins, Genes")
    ap.add_argument("--max-group-size", type=int, default=500,
                    help="drop groups reaching more genes than this; 0 for no limit")
    ap.add_argument("--label", default="",
                    help="suffix for the output filenames, so two configurations "
                         "can sit side by side (e.g. --label 2src)")
    ap.add_argument("--min-group-sources", type=int, default=1,
                    help="require this many distinct curations behind a gene pair. "
                         "2 is the two-source filter.")
    args = ap.parse_args()

    step2_path = Path(args.input).expanduser().resolve()
    if not step2_path.exists():
        sys.exit(f"File not found: {step2_path}")
    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    bf = Biofilter(bundle=args.bundle) if args.bundle else Biofilter()
    step2 = pd.read_csv(step2_path)

    mapping, unpairable = gene_to_variant_map(step2)
    gene_to_variants = mapping.groupby("ensembl_id").variant.apply(list).to_dict()
    group_types = [g.strip() for g in args.group_types.split(",") if g.strip()]

    result = bf.report.run(
        "pair_genes",
        input_data=sorted(gene_to_variants),
        mapping=gene_to_variants,
        # These are Ensembl ids, so say so: the search narrows to ENSEMBL
        # aliases instead of every alias in the bundle. 174,410 of those
        # are bare numbers and 14,335 collide with some gene's entity id,
        # so naming the column is what keeps a lookup exact.
        gene_identifier=GENE_IDENTIFIER,
        group_types=group_types,
        max_group_size=args.max_group_size,
        min_group_sources=args.min_group_sources,
    )
    models = result.to_pandas()
    gene_pairs = result.extra_tables["gene_pairs"].to_pandas()

    # Which branch each side came from is ours, not the report's: the item is
    # opaque to it, which is what let us hand it a QTL-derived attachment at all.
    branch_of = dict(zip(mapping.variant, mapping.branch))
    models["branch_1"] = models.item_1.map(branch_of)
    models["branch_2"] = models.item_2.map(branch_of)

    summary = [
        ("bundle_id", result.provenance.get("bundle_id")),
        ("step2_variants", step2.variant.nunique()),
        ("variants_mapped", mapping.variant.nunique()),
        ("variants_on_a_gene_the_bundle_lacks", unpairable.variant.nunique()),
        ("genes", mapping.ensembl_id.nunique()),
        ("group_types", ",".join(group_types)),
        ("max_group_size", args.max_group_size),
        ("min_group_sources", args.min_group_sources),
        ("gene_pairs", len(gene_pairs)),
        ("models", len(models)),
        ("variants_in_models", len(set(models.item_1) | set(models.item_2))),
    ]
    width = max(len(k) for k, _ in summary)
    print()
    for key, value in summary:
        print(f"{key:<{width}} : {value}")

    suffix = f"__{args.label}" if args.label else ""
    product_path = out_dir / f"step3_variant_pairs{suffix}.csv"
    models.rename(columns={"item_1": "variant_1", "item_2": "variant_2"}).to_csv(
        product_path, index=False
    )
    gene_pairs.to_csv(out_dir / f"step3_gene_pairs{suffix}.csv", index=False)
    pd.DataFrame(summary, columns=["metric", "value"]).to_csv(
        out_dir / f"step3_summary{suffix}.csv", index=False
    )
    Path(f"{product_path}.provenance.json").write_text(
        json.dumps(
            {
                "step": "adsp_step_03_variant_pairs",
                "label": args.label or None,
                "bundle_id": result.provenance.get("bundle_id"),
                "created_at": datetime.now(timezone.utc).isoformat(),
                "step2_input": str(step2_path),
                "reports": ["pair_genes"],
                "attachment": (
                    "variant->gene came from steps 1-2 (VEP for coding, QTL "
                    "target for regulatory) and was supplied to pair_genes as a "
                    "mapping, not re-derived from coordinates."
                ),
                "gene_identifier": GENE_IDENTIFIER,
                "group_types": group_types,
                "max_group_size": args.max_group_size,
                "min_group_sources": args.min_group_sources,
                "group_filter": result.provenance.get("group_filter"),
                "models": len(models),
            },
            indent=2,
        )
    )
    print(f"\nmodels     -> {product_path}  ({len(models):,} rows)")
    print(f"gene pairs -> {out_dir / f'step3_gene_pairs{suffix}.csv'}  ({len(gene_pairs):,} rows)")
    print(f"provenance -> {product_path}.provenance.json")


if __name__ == "__main__":
    main()
