# AG 14 - Report Tutorial: `chemical_master_annotation`

## Purpose
Compact Chemical annotation report based on `ChemicalMaster`.
For each input chemical alias/ID, returns:
- ChEBI identity (`chemical_id`) and canonical label/definition
- core physical fields (`formula`, `charge`, `mass`, `monoisotopic_mass`, `structure_id`)
- source/provenance fields (`omic_status`, source system, data source, ETL package)
- optional xref summary by source
- optional relationship summary by related entity group

## Report Name
`chemical_master_annotation`

## Parameters (API)
- `input_data`: `list[str]`, input file path, or `"__ALL__"` (required)
- `emit_not_found_rows`: `bool` (default `True`)
- `include_aliases`: `bool` (default `True`)
- `include_xref_summary`: `bool` (default `True`)
- `include_relationships`: `bool` (default `False`)

## Examples

API:
```python
df = bf.report.run(
    "chemical_master_annotation",
    input_data=["CHEBI:15377", "CHEBI:17234", "water"],
    include_xref_summary=True,
    include_relationships=True,
)
```

API (`__ALL__`):
```python
df = bf.report.run(
    "chemical_master_annotation",
    input_data="__ALL__",
    include_xref_summary=True,
    include_relationships=False,
)
```

CLI:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name chemical_master_annotation \
  --input CHEBI:15377 --input CHEBI:17234 --input "water" \
  --param include_xref_summary=true \
  --param include_relationships=true
```

CLI (`__ALL__`):
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name chemical_master_annotation \
  --param input_data=__ALL__ \
  --param include_xref_summary=true
```

## Notes
- `xref_ids_by_source` summarizes `EntityAlias` entries where `alias_type='code'`.
- `entity_relationships_by_group` and `total_entity_relationships` are optional and disabled by default for performance.
- When `include_aliases=false`, `other_aliases` is returned as null.
- When `input_data="__ALL__"`, the report resolves and returns all chemical entities available in `ChemicalMaster`.
