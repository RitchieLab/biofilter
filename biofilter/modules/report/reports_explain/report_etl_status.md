# AG 02 - Report Tutorial: `etl_status`

## Purpose
Shows the latest good ETL status per data source (extract/transform/load) and alignment flags.

## Report Name
`etl_status`

## Parameters (API)
- `source_system`: `str | list[str]` (optional)
- `data_sources`: `str | list[str]` (optional)
- `only_active`: `bool` (default `True`)

## Examples

CLI:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name etl_status
```

API:
```python
df = bf.report.run(
    "etl_status",
    source_system=["NCBI", "Ensembl"],
    data_sources=["dbsnp_chr1", "hgnc"],
    only_active=True,
)
```

## Recommended Demo Columns
- `source_system`
- `data_source`
- `extract_status`
- `transform_status`
- `load_status`
- `pipeline_ok`
- `latest_error`
