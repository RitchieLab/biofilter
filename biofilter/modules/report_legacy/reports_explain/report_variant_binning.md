# Variant Binning

BioBin-style rare-variant aggregation report.

## Purpose

Given a multi-sample VCF (and optional phenotype file), this report:

1. computes internal MAF from VCF genotypes,
2. selects rare variants by `maf_cutoff`,
3. maps variants to genes by genomic overlap (`entity_locations`),
4. expands to bins by `group_by`, and
5. writes output artifacts to `output_dir`.

## Supported `group_by`

- `gene`
- `gene_group`
- `locus_type`
- `pathway`

## Required Params

- `vcf_path`: path to cohort VCF (`.vcf`, `.vcf.gz`, `.vcf.bgz`)
- `output_dir`: directory where CSV/JSON artifacts are written

## Optional Params

- `phenotype_path`: CSV/TSV with sample phenotype labels
- `phenotype_sample_column` (default `SampleID`)
- `phenotype_value_column` (default `Phenotype`)
- `phenotype_control_value` (default `0`)
- `phenotype_case_values` (optional list)
- `group_by` (default `gene`)
- `maf_cutoff` (default `0.01`)
- `rare_case_control` (default `true`)
- `overall_major_allele` (default `true`)
- `build` (default `38`)
- `max_variants` (optional)
- `include_zero_counts` (default `true`)

## Artifacts

- `bin_counts.csv`
- `variant_to_bin.csv`
- `bin_definitions.csv`
- `bin_member_counts.csv`
- `sample_bin_long.csv`
- `summary.json`

The report return value is a 1-row DataFrame summary containing counts and artifact paths.
