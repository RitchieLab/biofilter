# AG 13 - Report Tutorial: `go_master_annotation`

## Purpose
Compact GO annotation report based on `GOMaster`.
For each input GO alias/ID, returns:
- GO identity (`go_id`, `name`, `namespace`)
- source/provenance fields
- optional GO DAG summary (`parent/child` counts + relation types)
- optional GO DAG details (parent/child GO IDs)
- optional relationship summary by related entity group

## Report Name
`go_master_annotation`

## Parameters (API)
- `input_data`: `list[str]`, input file path, or `"__ALL__"` (required)
- `emit_not_found_rows`: `bool` (default `True`)
- `include_aliases`: `bool` (default `True`)
- `include_go_relation_summary`: `bool` (default `True`)
- `include_go_relation_details`: `bool` (default `False`)
- `max_go_terms_per_side`: `int` (default `20`)
- `include_relationships`: `bool` (default `False`)

## Examples

API:
```python
df = bf.report.run(
    "go_master_annotation",
    input_data=["GO:0006915", "GO:0008150"],
    include_go_relation_summary=True,
    include_go_relation_details=False,
    include_relationships=True,
)
```

API (`__ALL__`):
```python
df = bf.report.run(
    "go_master_annotation",
    input_data="__ALL__",
    include_go_relation_summary=True,
    include_go_relation_details=False,
)
```

CLI:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name go_master_annotation \
  --input GO:0006915 --input GO:0008150 \
  --param include_go_relation_summary=true \
  --param include_go_relation_details=false \
  --param include_relationships=true
```

CLI (`__ALL__`):
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name go_master_annotation \
  --param input_data=__ALL__ \
  --param include_go_relation_summary=true
```

## Notes
- `go_parent_count`/`go_child_count` summarize direct edges in `go_relations`.
- `go_parent_ids`/`go_child_ids` are optional and capped by `max_go_terms_per_side`.
- `entity_relationships_by_group`/`total_entity_relationships` summarize graph edges from `entity_relationships`.
- When `input_data="__ALL__"`, the report resolves and returns all GO entities available in `GOMaster`.
