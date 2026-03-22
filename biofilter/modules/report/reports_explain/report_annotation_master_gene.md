# Report Tutorial: `annotation_master_gene`

## Purpose

Compact Gene annotation report focused on performance.
For each input gene/alias, returns:

- resolved `entity_id`
- canonical IDs (`symbol`, `hgnc`, `ensembl`, `entrez`)
- GeneMaster metadata (`hgnc_status`, `omic_status`, `locus_group`, `locus_type`)
- gene groups membership
- build38 coordinates (`chromosome`, `start`, `end`)
- relationship summary by related entity group and total count
- optional variant count in gene range (without variant details)

## Report Name

`annotation_master_gene`

## Parameters (API)

- `input_data`: `list[str]`, input file path, or `"__ALL__"` (required)
- `include_relationships`: `bool` (default `True`)
- `include_variant_summary`: `bool` (default `True`)
- `emit_not_found_rows`: `bool` (default `True`)

## Examples

API:

```python
df = bf.report.run(
    "annotation_master_gene",
    input_data="__ALL__",
    include_relationships=False,
    include_variant_summary=False,
    emit_not_found_rows=False,
)
```

API (specific genes):

```python
df = bf.report.run(
    "annotation_master_gene",
    input_data=["BRCA1", "TP53", "HGNC:11998"],
    include_relationships=True,
    include_variant_summary=True,
    emit_not_found_rows=True,
)
```

CLI:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_gene \
  --input BRCA1 --input TP53 --input HGNC:11998 \
  --param include_relationships=true \
  --param include_variant_summary=true
```

CLI (all genes):

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_gene \
  --param input_data=__ALL__ \
  --param include_relationships=false \
  --param include_variant_summary=false \
  --param emit_not_found_rows=false
```

With input file:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_gene \
  --input-file ./genes.txt \
  --param include_variant_summary=false
```

## Notes

- Relationship summary is reported as a compact list of tuples:
  `[(<RelatedEntityGroup>, <count>), ...]`
- `total_entity_relationships` is the sum of the list above.
- Variant summary only counts overlapping variants in build38 gene interval.
- If gene location is missing in `entity_locations` (build=38), variant count is null.
- `input_data="__ALL__"` returns one row per gene entity found in `GeneMaster`.
- For large databases, prefer `include_relationships=false` and `include_variant_summary=false` for faster runtime.
