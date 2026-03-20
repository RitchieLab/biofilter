# AG 15 - Report Tutorial: `platform_data_statistics`

## Purpose
Platform-level statistics report for operational dashboards.
Returns a compact long-format dataset with:
- entity counts by omic domain (`EntityGroup`)
- variant counts by chromosome (`variant_masters`)
- relationship counts by group pair (`entity_relationships`)
- datasources ingested and latest load execution metadata (`etl_packages`)

## Report Name
`platform_data_statistics`

## Output Shape
Rows are returned in long format using these columns:
- `section`
- `metric`
- `dimension_1`
- `dimension_2`
- `value_number`
- `value_text`
- `as_of`
- `note`

This shape is ideal for pivoting and charting in notebooks.

## Parameters (API)
- `sections`: `list[str]` or comma-separated string (optional)
  - Allowed values:
    - `entity_counts_by_group`
    - `variant_counts_by_chromosome`
    - `relationship_counts_by_group_pair`
    - `datasource_latest_load`
  - Default: all sections.
- `only_active_entities`: `bool` (default `True`)
  - When `True`, entity counts exclude only explicit inactive rows (`is_active=False`).
- `relationship_mode`: `"undirected" | "directed"` (default `"undirected"`)
- `include_totals`: `bool` (default `True`)

## Examples

API (all sections):
```python
df = bf.report.run(
    "platform_data_statistics",
    only_active_entities=True,
    relationship_mode="undirected",
    include_totals=True,
)
```

API (selected sections):
```python
df = bf.report.run(
    "platform_data_statistics",
    sections=["entity_counts_by_group", "datasource_latest_load"],
    include_totals=False,
)
```

CLI:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run \
  --report-name platform_data_statistics \
  --param relationship_mode=undirected \
  --param include_totals=true
```

## Notes
- Variant section depends on `variant_masters`; if unavailable, a note row is emitted.
- Relationship counts are aggregated by group pair (domain-domain view), not per entity.
- Datasource section emits multiple metrics per datasource:
  - `latest_load_package_id`
  - `latest_load_status`
  - `latest_load_end`
  - `latest_load_rows`
  - `latest_load_age_days`
