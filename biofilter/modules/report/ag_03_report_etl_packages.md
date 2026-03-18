# AG 03 - Report Tutorial: `etl_packages`

## Purpose
Detailed ETL audit report with package-level records and timing/hash fields for extract, transform, and load.

## Report Name
`etl_packages`

## Parameters (API)
- `source_system`: `str | list[str]` (optional)
- `data_sources`: `str | list[str]` (optional)
- `only_active`: `bool` (default `True`)

## Examples

CLI:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run --name etl_packages
```

API:
```python
df = bf.report.run(
    "etl_packages",
    source_system="NCBI",
    data_sources=["dbsnp_chr1", "dbsnp_chr2"],
    only_active=True,
)
```

## Recommended Demo Columns
- `package_id`
- `source_system`
- `data_source`
- `status`
- `operation_type`
- `extract_status`
- `transform_status`
- `load_status`
- `extract_minutes`
- `transform_minutes`
- `load_minutes`
