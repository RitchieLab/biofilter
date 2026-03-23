# Troubleshooting

## Report Not Found

- Run `biofilter report list`.
- Use `--report-name` with one of the listed names.

## Input Conflict in `report run`

If you pass `--input`/`--input-file`, do not also pass input keys through params (`input_data`, `items`, `input_path`).

## Explain Page Not Found

- Check if guide exists at `biofilter/modules/report/reports_explain/report_<module>.md`.
- If missing, Biofilter will fall back to class `explain()`.

## PostgreSQL-only Reports

`db_pg_table_stats` and `db_pg_index_stats` require PostgreSQL.

## Migration/Upgrade Issues

Use:

```bash
biofilter db migrate --status
biofilter db migrate --target head
biofilter db upgrade
```

## ETL Batch Resume

If `etl update-all` was interrupted, run it again. Successful data sources are skipped.

## Report Output Not Found (Docker)

If you run BF4 in a container and export with `--output`, mount a host volume and write to that mounted path.

Example:

```bash
docker run --rm \
  -e DATABASE_URL="postgresql+psycopg2://user:pass@host:5432/db" \
  -v "$(pwd)/outputs:/workspace/outputs" \
  biofilter:bf4 \
  biofilter report run --report-name etl_status --output /workspace/outputs/etl_status.csv
```
