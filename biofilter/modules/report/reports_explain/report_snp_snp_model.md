# Report Tutorial: `snp_snp_model`

## Purpose

Builds BF4 candidate interaction models in two layers:

- `gene_pair`: genes connected by shared biological groups (for example pathways)
- `snp_pair`: variant pairs expanded from those gene pairs
- `Direct Gene` mode is also supported (gene-gene links without intermediate groups)

The report starts from user seed positions (`chr:position`) and maps to variants and genes using `entity_locations`.

## Report Name

`snp_snp_model`

## Core Pipeline

1. Input seed positions (`chr:position`)
2. Resolve seed variants in `variant_masters` using only `allele_type = SNV`
   and collapse multi-allelic rows to one logical variant
3. Map seed variants to seed genes by genomic overlap in `entity_locations`
4. Expand seed genes to biological groups using `entity_relationships`
5. Expand back from those groups to additional genes
6. Build gene-gene pairs by co-membership in the same group
7. Expand variants for genes and generate SNP-SNP pairs

## Required Parameters (API)

- `input_data`: `list[str|dict]` (or a path to a text file)

## Main Parameters

- `build` (default `38`)
- `window_bp` (default `0`)
- `group_entity_groups` (default `['Pathway','Pathways']`)
  - special option: `Direct Gene`
- `group_data_sources` (optional; data source names or IDs used to filter grouping links only)
- `group_entities` (optional explicit group entity names)
- `relationship_types` (optional; default uses all relationship types)
- `gene_pair_scope`: `both_from_seed | one_from_seed | at_least_one_from_seed | any_expanded`
- `snp_pair_scope`: `both_from_seed | one_from_seed | at_least_one_from_seed | any_expanded`
- `expand_variants_from_expanded_genes` (default `True`)
- `include_gene_pairs` / `include_snp_pairs`
- `limit_variants_per_gene` (default `2000`)
- `max_snp_pairs` (default `200000`)

## Examples

API (inline inputs):

```python
df = bf.report.run(
    "snp_snp_model",
    input_data=["chr17:150", "chr17:280"],
    group_entity_groups=["Pathway"],
    gene_pair_scope="at_least_one_from_seed",
    snp_pair_scope="at_least_one_from_seed",
)
```

CLI (inputs from TXT file):

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name snp_snp_model \
  --input-file ./positions.txt \
  --param build=38 \
  --param group_entity_groups='["Pathway"]' \
  --param group_data_sources='["Reactome"]' \
  --output snp_snp_from_txt.csv
```

`positions.txt` format (one position per line):

```text
chr19:44904604
chr1:13259
chr15:63279422
```

CLI (inputs from CSV file):

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name snp_snp_model \
  --input-file ./positions.csv \
  --input-column position \
  --param build=38 \
  --param group_entity_groups='["Pathway"]' \
  --output snp_snp_from_csv.csv
```

`positions.csv` example:

```csv
sample_id,position
S1,chr19:44904604
S2,chr1:13259
S3,chr15:63279422
```

## Output Row Types

- `input`: invalid/not-found input traces
- `gene_pair`: gene-gene candidate models
- `snp_pair`: SNP-SNP models expanded from gene pairs
- `summary`: truncation/no-model messages when relevant

## Variant Selection Rules

- Uses only SNV rows when `variant_masters.allele_type` is available
- Collapses alternate-allele duplicates and keeps one deterministic row per
  logical variant (preferably by `rsid`, otherwise by locus/ref)

## Group Selection Notes

- If no `group_entity_groups` is provided, the report defaults to `Pathway/Pathways`.
- You can provide multiple group types (for example `['Pathway', 'GO']`).
- Use `Direct Gene` to model direct gene-gene links without a group intermediary.
- `group_data_sources` filters only the grouping step (gene->group, group->gene, and Direct Gene links);
  variant-to-gene mapping is unchanged.
- If group types are invalid, the report returns a friendly error listing available options.
