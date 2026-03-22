# Report Tutorial: `db_pg_index_stats`

## Purpose

PostgreSQL-only report with per-index details:

- size and method
- uniqueness/primary/valid/ready flags
- optional usage stats (`idx_scan`, `idx_tup_read`, `idx_tup_fetch`)

## Report Name

`db_pg_index_stats`

## Parameters (API)

- `schema`: `str | list[str]` (optional)
- `table`: `str | list[str]` (optional)
- `index`: `str | list[str]` (optional)
- `include_index_def`: `bool` (default `True`)
- `include_usage`: `bool` (default `True`)
- `output_columns`: `list[str]` (optional)

## Examples

CLI:

```bash
biofilter --db-uri postgresql+psycopg2://user:pass@localhost/biofilter_dev report run --report-name db_pg_index_stats
biofilter --db-uri postgresql+psycopg2://user:pass@localhost/biofilter_dev report run --report-name db_pg_index_stats --param schema=public --param table=variant_masters --param include_usage=true
```

API:

```python
df = bf.report.run(
    "db_pg_index_stats",
    schema="public",
    table=["variant_masters"],
    include_usage=True,
    output_columns=["schema_name", "table_name", "index_name", "index_size", "idx_scan"],
)
```

## Notes

- Fails on non-Postgres databases by design.
- Usage counters reset on PostgreSQL restart/stat reset.
