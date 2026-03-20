# AG 12 - Report Tutorial: `disease_master_annotation`

## Purpose
Compact Disease annotation report based on `DiseaseMaster`.
For each input disease alias/ID, returns:
- MONDO identity + label + description
- disease groups/subsets
- source/provenance fields
- optional xref summary by source
- optional ClinGen summary (gene count + relationship count)
- optional relationship summary by related entity group

## Report Name
`disease_master_annotation`

## Parameters (API)
- `input_data`: `list[str]`, input file path, or `"__ALL__"` (required)
- `emit_not_found_rows`: `bool` (default `True`)
- `include_aliases`: `bool` (default `True`)
- `include_xref_summary`: `bool` (default `True`)
- `include_clingen_summary`: `bool` (default `True`)
- `include_relationships`: `bool` (default `False`)

## Examples

API:
```python
df = bf.report.run(
    "disease_master_annotation",
    input_data=["MONDO:0019391", "MONDO:0005737", "cystic fibrosis"],
    include_xref_summary=True,
    include_clingen_summary=True,
    include_relationships=True,
)
```

API (`__ALL__`):
```python
df = bf.report.run(
    "disease_master_annotation",
    input_data="__ALL__",
    include_xref_summary=True,
    include_clingen_summary=True,
)
```

CLI:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name disease_master_annotation \
  --input MONDO:0019391 --input MONDO:0005737 --input "cystic fibrosis" \
  --param include_xref_summary=true \
  --param include_clingen_summary=true \
  --param include_relationships=true
```

CLI (`__ALL__`):
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name disease_master_annotation \
  --param input_data=__ALL__ \
  --param include_xref_summary=true
```

## Notes
- `clingen_*` fields summarize only relationships loaded from data source `clingen`.
- `entity_relationships_by_group`/`total_entity_relationships` are optional.
- Relationship type semantics from ClinGen may evolve; this report focuses on stable group/source-level summaries.
- When `input_data="__ALL__"`, the report resolves and returns all disease entities available in `DiseaseMaster`.
