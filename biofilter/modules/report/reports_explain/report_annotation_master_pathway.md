# Report Tutorial: `annotation_master_pathway`

## Purpose

Compact Pathway annotation report.
For each input pathway alias/ID, returns:

- `entity_id`
- `pathway_id`
- `pathway_description`
- pathway origin (`source_system` and `data_source`)
- optional relationship summary by related entity group

## Report Name

`annotation_master_pathway`

## Parameters (API)

- `input_data`: `list[str]`, input file path, or `"__ALL__"` (required)
- `include_relationships`: `bool` (default `False`)
- `emit_not_found_rows`: `bool` (default `True`)
- `include_aliases`: `bool` (default `True`)

## Examples

API:

```python
df = bf.report.run(
    "annotation_master_pathway",
    input_data=["R-HSA-109581", "hsa00010", "Cell cycle"],
    include_relationships=True,
    emit_not_found_rows=True,
)
```

API (`__ALL__`):

```python
df = bf.report.run(
    "annotation_master_pathway",
    input_data="__ALL__",
    include_relationships=True,
    include_aliases=False,
)
```

CLI:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_pathway \
  --input R-HSA-109581 --input hsa00010 --input "Cell cycle" \
  --param include_relationships=true
```

CLI (`__ALL__`):

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name annotation_master_pathway \
  --param input_data=__ALL__ \
  --param include_relationships=true
```

## Notes

- `entity_relationships_by_group` and `total_entity_relationships` are optional.
- When `include_relationships=false`, both columns are returned as null.
- Report does not include variant-level fields by design.
- When `input_data="__ALL__"`, the report resolves and returns all pathways available in `PathwayMaster`.
