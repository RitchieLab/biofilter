# AG 04 - Report Tutorial: `entity_filter`

## Purpose
Validates a list of entity names and returns matching entities, including conflict/deactivation flags.

## Report Name
`entity_filter`

## Required Parameters (API)
- `input_data`: `list[str]`

## Examples

API (recommended, because this report requires input list parameters):
```python
df = bf.report.run(
    "entity_filter",
    input_data=["BRCA1", "BRCA2", "TP53", "NOT_A_GENE"],
)
```

CLI:
- CLI `report run` currently does not expose parameter injection for report-specific inputs.
- Use API for this report until CLI params are extended.

## Recommended Demo Columns
- `input_original`
- `primary_name`
- `group_name`
- `has_conflict`
- `is_deactive`
- `observation`
