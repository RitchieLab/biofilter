# AG 05 - Report Tutorial: `db_pg_table_stats`

## Purpose
PostgreSQL-only report for table/storage observability:
- estimated rows
- table/index/toast/total size
- partition-aware output (leaf + parent aggregate)

## Report Name
`db_pg_table_stats`

## Parameters (API)
- `schema`: `str | list[str]` (optional)
- `table`: `str | list[str]` (optional)
- `output_columns`: `list[str]` (optional)

## Examples

CLI:
```bash
biofilter --db-uri postgresql+psycopg2://user:pass@localhost/biofilter_dev report run --report-name db_pg_table_stats
biofilter --db-uri postgresql+psycopg2://user:pass@localhost/biofilter_dev report run --report-name db_pg_table_stats --param schema=public --param 'table=["variant","entity"]'
```

API:
```python
df = bf.report.run(
    "db_pg_table_stats",
    schema="public",
    table=["variant", "entity"],
    output_columns=["schema_name", "table_name", "total_bytes", "n_indexes"],
)
```

## Notes
- Fails on non-Postgres databases by design.
- `rows_est` is catalog-estimated (fast), not exact count.
