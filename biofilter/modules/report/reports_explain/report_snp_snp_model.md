# Report Tutorial: `snp_snp_model`

## Purpose

Builds BF4 candidate interaction models in two layers:

- `gene_pair`: genes connected by shared biological groups (for example pathways)
- `snp_pair`: variant pairs expanded from those gene pairs

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
- `group_entities` (optional explicit group entity names)
- `relationship_types` (default `['in_pathway']`)
- `gene_pair_scope`: `both_from_seed | one_from_seed | at_least_one_from_seed | any_expanded`
- `snp_pair_scope`: `both_from_seed | one_from_seed | at_least_one_from_seed | any_expanded`
- `expand_variants_from_expanded_genes` (default `True`)
- `include_gene_pairs` / `include_snp_pairs`
- `limit_variants_per_gene` (default `2000`)
- `max_snp_pairs` (default `200000`)

## Example

```python
df = bf.report.run(
    "snp_snp_model",
    input_data=["chr17:150", "chr17:280"],
    group_entity_groups=["Pathway"],
    relationship_types=["in_pathway"],
    gene_pair_scope="at_least_one_from_seed",
    snp_pair_scope="at_least_one_from_seed",
)
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
