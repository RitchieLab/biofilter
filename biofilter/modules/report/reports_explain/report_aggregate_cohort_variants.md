# aggregate_cohort_variants

Your cohort's variants, matched against the bundle and aggregated into
biological bins.

```bash
biofilter report run --report-name aggregate_cohort_variants \
    --param cohort_file=./cohort.vcf.gz \
    --param phenotype_file=./phenotype.csv \
    --param output_grain=bins \
    --param group_by=gene \
    --param maf_cutoff=0.01 \
    --output bins.csv
```

> Replaces `variant_list_intersect` and `variant_binning`, which were the
> same pipeline stopped at different points.

## Three stages, two places to stop

1. **Read** the cohort file — a VCF with genotypes, a PLINK `.bim`, or a
   plain list of rsIDs and positions.
2. **Match** each variant against the bundle and place it on the genes
   whose build-38 range contains it.
3. **Aggregate** — keep the rare variants, put each in a bin, and count
   what every sample carries in every bin.

| `output_grain` | one row is | needs genotypes | replaces |
| --- | --- | --- | --- |
| `variants` (default) | one cohort variant | no | `variant_list_intersect` |
| `bins` | one sample in one bin | **yes** | `variant_binning` |

Asking for `bins` with a `.bim` is an error, not an empty result: a
`.bim` has no genotypes, and binning counts what people carry.

## The guard that matters most

A cohort file spans the genome. A bundle need not. Binning a whole-genome
VCF against a bundle carrying one chromosome **does not fail** — it
returns bins covering that chromosome and says nothing about the rest.

Every run reports the overlap:

```json
"chromosome_coverage": {
  "chromosomes_in_cohort": [1, 7, 22],
  "chromosomes_in_bundle": [22],
  "chromosomes_missing_from_bundle": [1, 7],
  "variants_the_bundle_cannot_place": 2,
  "share_unplaceable": 0.005
}
```

Pass `require_full_coverage=true` to turn that into a refusal. Do that
whenever the result is going anywhere but your own screen.

## The other way an empty result happens

With **N samples the smallest observable minor allele frequency is
1/(2N)** — one allele copy in one person. Ten samples cannot see anything
rarer than 0.05. Ask for `maf_cutoff=0.01` on a ten-sample cohort and
every variant passing the filter is one nobody carries, so every bin
comes back empty, correctly, for a reason no column shows.

So the report says it outright:

```json
"rare_variants": {
  "maf_cutoff": 0.01,
  "samples": 20,
  "smallest_observable_maf": 0.025,
  "rare": 368,
  "rare_with_carriers": 0,
  "means": "This cohort has 20 samples, so the smallest frequency it can
            observe is 0.0250 … Raise maf_cutoff above 0.0250, or use a
            larger cohort."
}
```

## Which frequency the rare filter uses

| setting | filters on |
| --- | --- |
| `rare_case_control=true` (default), both arms present | the **larger** of the case and control MAFs |
| `rare_case_control=false`, `overall_major_allele=false` | the control MAF |
| otherwise | the overall MAF |

The first is BioBin's rule: a variant counts as rare only if it is rare in
both arms, so a variant common in cases and absent in controls is not
swept into a rare-variant bin. This is the relational version's behaviour,
kept exactly — changing it silently would reclassify somebody's variants.

A no-call (`./.`) leaves the denominator rather than counting as a
reference call, which would deflate every frequency.

## Bin types, and how much of the catalogue each can reach

| `group_by` | bin is | genes it can reach |
| --- | --- | --- |
| `gene` (default) | the gene | 39,306 — 54.1% |
| `gene_group` | an HGNC gene family | 25,059 — 34.5% |
| `locus_type` | protein-coding, lncRNA, pseudogene… | 39,306 — 54.1% |
| `pathway` | a Reactome or KEGG pathway | 12,521 — 17.2% |

Step 2 is positional, and only 39,306 of the bundle's 72,660 genes carry
build-38 coordinates — so a little over half the catalogue is reachable at
all, whatever the grouping. The `bin_coverage` provenance block reports
the number for the grouping you chose. A variant in a gene the grouping
cannot reach is reported as unbinned, not dropped.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `cohort_file` | required | VCF / `.vcf.gz`, PLINK `.bim`, or a plain list |
| `cohort_format` | by extension | `vcf`, `bim` or `list`, to override |
| `output_grain` | `variants` | or `bins` |
| `group_by` | `gene` | `gene`, `gene_group`, `locus_type`, `pathway` |
| `phenotype_file` | none | CSV with a sample column and a phenotype column |
| `phenotype_sample_column` | `SampleID` | |
| `phenotype_value_column` | `Phenotype` | |
| `phenotype_control_value` | `0` | values meaning control |
| `phenotype_case_values` | none | when absent, anything not a control is a case |
| `maf_cutoff` | `0.01` | a minor allele frequency, so in (0, 0.5] |
| `rare_case_control` | `true` | see above |
| `overall_major_allele` | `true` | see above |
| `window_bp` | `0` | widen a gene's range when placing a variant |
| `build` | `38` | |
| `max_variants` | none | stop after this many, for a quick look |
| `require_full_coverage` | `false` | refuse if the bundle cannot place a chromosome |
| `plink_extract_path` | none | write a `--extract` list |
| `variant_to_bin_path` | none | write the (variant, bin) mapping |

## The PLINK `--extract` file

PLINK matches on the id in your `.bim`, not on coordinates, so a file of
`chr:pos` strings extracts nothing from a dataset whose ids are rsIDs.
The id your own file used is therefore preferred, falling back to
`chr:pos`.

It lists **every cohort variant the bundle carries**, including ones no
gene contains. Gene placement is a separate question from whether the
bundle knows the variant, and `--extract` answers the second one.

## Columns

### `output_grain="variants"`

| column | meaning |
| --- | --- |
| `cohort_variant_id` | the id in your file, or `chr:pos:ref:alt` |
| `chromosome`, `position`, alleles | as your file spelled them |
| `variant_key` | the bundle's key, when it has the variant |
| `rsid`, `plink_id` | for joining and for `--extract` |
| `match_status`, `note` | see below |
| `gene_entity_ids`, `gene_symbols` | the genes containing it |
| `maf_overall`, `maf_case`, `maf_control` | from your genotypes |
| `ac_overall`, `an_overall` | allele count and called alleles |
| `is_rare` | whether it passes `maf_cutoff` |

| `match_status` | means |
| --- | --- |
| `matched` | the bundle has it and a gene contains it |
| `in_bundle_no_gene` | the bundle has it; no gene with coordinates contains it |
| `not_in_bundle` | the bundle covers the chromosome and lacks this variant |
| `chromosome_not_in_bundle` | the bundle carries nothing for that chromosome |
| `unresolved` | an rsID the bundle does not carry, so it has no coordinates |

### `output_grain="bins"`

| column | meaning |
| --- | --- |
| `bin_name`, `bin_type` | the bin, and what kind it is |
| `sample`, `sample_class` | the person, and `case` / `control` / `unknown` |
| `variant_count` | rare variants in this bin the sample carries |
| `alt_count` | alternate allele copies — the burden |
| `bin_variant_count`, `bin_gene_count` | the bin's totals across the cohort |

Only carriers appear. A sample carrying nothing in a bin is absent rather
than a row of zeros, which matters when a cohort has thousands of samples
and a bin has three carriers.

## Reading the result

**`alt_count` is what a burden test consumes**, not evidence of anything
by itself. Compare it across `sample_class`, with a test that belongs
outside this report.

**Rebinning changes the bins.** Two runs differing only in `maf_cutoff`
produce different bins and the result table does not say which variants
moved. `variant_to_bin_path` writes that mapping; keep it next to the
result whenever the bins matter.
