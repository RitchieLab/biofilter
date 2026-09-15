# Report Tutorial: `annotation_master_protein`

## Purpose

Compact Protein annotation report using `ProteinMaster` as canonical base.
For each input protein alias/ID (including isoform aliases), returns:

- `entity_id` (input entity)
- canonical protein context (`protein_master_id`, `protein_id`)
- `canonical_entity_id` when available
- ProteinMaster metadata (`function`, `location`, `tissue_expression`, `pseudogene_note`)
- optional Pfam summary (counts by type, optional Pfam IDs by type)
- optional relationship summary by related entity group

## Report Name

`annotation_master_protein`

## Parameters (API)

- `input_data`: `list[str]`, input file path, or `"__ALL__"` (required)
- `emit_not_found_rows`: `bool` (default `True`)
- `include_pfam_summary`: `bool` (default `True`)
- `include_pfam_details`: `bool` (default `False`)
- `max_pfam_ids_per_type`: `int` (default `20`)
- `include_relationships`: `bool` (default `False`)
- `include_aliases`: `bool` (default `True`)

## Examples

API:

```python
df = bf.report.run(
    "annotation_master_protein",
    input_data=["P04637", "P04637-2", "TP53_HUMAN"],
    include_pfam_summary=True,
    include_pfam_details=False,
    include_relationships=True,
)
```

API (`__ALL__`):

```python
df = bf.report.run(
    "annotation_master_protein",
    input_data="__ALL__",
    include_pfam_summary=True,
    include_pfam_details=False,
)
```

CLI:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_protein \
  --input P04637 --input P04637-2 --input TP53_HUMAN \
  --param include_pfam_summary=true \
  --param include_pfam_details=false \
  --param include_relationships=true
```

CLI (`__ALL__`):

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_protein \
  --param input_data=__ALL__ \
  --param include_pfam_summary=true
```

## Notes

- `ProteinMaster` stores canonical proteins; isoform context is inferred from `protein_entities`.
- `pfam_count_by_type` is compact and stable for V1.
- `pfam_ids_by_type` is optional and capped by `max_pfam_ids_per_type`.
- When `include_relationships=false`, relationship columns are returned as null.
- When `input_data="__ALL__"`, the report resolves and returns all protein entities available in `ProteinEntity`.
