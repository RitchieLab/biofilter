# `snp_snp_model` Report (CLI Guide)

This guide shows practical CLI examples for the `snp_snp_model` report, including how to use `group_entity_groups` and `group_data_sources` filters.

## What This Report Does

`snp_snp_model` builds candidate interaction models in two layers:

- `gene_pair`: gene-gene pairs supported by groups (for example Pathways/GO) or direct gene-gene links
- `snp_pair`: SNP-SNP pairs expanded from the supported gene pairs

High-level flow:

1. Resolve user seed positions (`chr:position`) to SNV variants
2. Map variants to seed genes (via `entity_locations`)
3. Expand genes through selected grouping entities (`group_entity_groups`)
4. Build gene-gene pairs
5. Expand to SNP-SNP pairs

## Discover Available Data Sources First

When using `group_data_sources`, the values should match loaded ETL data source names.

Use:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db etl status
```

Optional filters:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db etl status --only-active
biofilter --db-uri sqlite:///biofilter_dev.db etl status --source-system NCBI
```

Look at the `data_source` values in the output (for example: `Reactome`, `KEGG`, etc.).

## Example 1: Basic Run (No Data Source Filter)

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name snp_snp_model \
  --input chr19:44904604 \
  --input chr1:13259 \
  --input chr15:63279422 \
  --param build=38 \
  --param window_bp=0 \
  --param group_entity_groups='["Pathway"]' \
  --param gene_pair_scope=both_from_seed \
  --output snp_snp.csv
```

## Example 2: Pathway + Single Data Source (Reactome Only)

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name snp_snp_model \
  --input chr19:44904604 \
  --input chr1:13259 \
  --input chr15:63279422 \
  --param build=38 \
  --param window_bp=0 \
  --param group_entity_groups='["Pathway"]' \
  --param group_data_sources='["Reactome"]' \
  --param gene_pair_scope=both_from_seed \
  --output snp_snp_reactome.csv
```

## Example 3: Pathway + Multiple Data Sources

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name snp_snp_model \
  --input chr19:44904604 \
  --input chr1:13259 \
  --input chr15:63279422 \
  --param build=38 \
  --param group_entity_groups='["Pathway"]' \
  --param group_data_sources='["Reactome","KEGG"]' \
  --param gene_pair_scope=at_least_one_from_seed \
  --param snp_pair_scope=at_least_one_from_seed \
  --output snp_snp_reactome_kegg.csv
```

## Example 4: Direct Gene Mode

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name snp_snp_model \
  --input chr7:128858174 \
  --input chr17:12061622 \
  --param build=38 \
  --param group_entity_groups='["Direct Gene"]' \
  --param gene_pair_scope=both_from_seed \
  --param snp_pair_scope=both_from_seed \
  --output snp_snp_direct_gene.csv
```

## Example 5: Input File

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name snp_snp_model \
  --input-file ./positions.txt \
  --param build=38 \
  --param group_entity_groups='["Pathway"]' \
  --param group_data_sources='["Reactome"]' \
  --output snp_snp_from_file.csv
```

`positions.txt` should contain one `chr:position` per line.

Example `positions.txt`:

```text
chr19:44904604
chr1:13259
chr15:63279422
```

## Example 6: Input CSV File (Specific Column)

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name snp_snp_model \
  --input-file ./positions.csv \
  --input-column position \
  --param build=38 \
  --param group_entity_groups='["Pathway"]' \
  --param group_data_sources='["Reactome"]' \
  --output snp_snp_from_csv.csv
```

Example `positions.csv`:

```csv
sample_id,position
S1,chr19:44904604
S2,chr1:13259
S3,chr15:63279422
```

## Main Parameters (Quick Reference)

- `build`: genome build (default `38`)
- `window_bp`: extension window around position/gene boundaries (default `0`)
- `group_entity_groups`: grouping entity types (default Pathway/Pathways). Supports `Direct Gene`
- `group_data_sources`: optional filter for grouping relationships only
- `group_entities`: optional explicit group entity names to constrain expansion
- `relationship_types`: optional relationship type filter; if omitted, all types are used
- `gene_pair_scope`: `both_from_seed | one_from_seed | at_least_one_from_seed | any_expanded`
- `snp_pair_scope`: `both_from_seed | one_from_seed | at_least_one_from_seed | any_expanded`
- `expand_variants_from_expanded_genes`: include variant expansion from expanded genes (`true` by default)

## Output Columns to Watch

- Group support:
  - `group_support_count`
  - `group_support_ids`
  - `group_support_names`
- Data source support:
  - `data_source_support_count`
  - `data_source_support_ids`
  - `data_source_support_names`

## Notes

- `group_data_sources` affects only grouping steps (gene-group expansion and direct gene links), not variant-to-gene mapping.
- For list parameters in CLI, pass JSON-like arrays using quotes, for example:
  - `--param group_entity_groups='["Pathway","GO"]'`
  - `--param group_data_sources='["Reactome","KEGG"]'`
- Use this to inspect the report template quickly:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name snp_snp_model \
  --params-template
```
