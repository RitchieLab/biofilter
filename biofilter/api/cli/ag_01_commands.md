# AG 01 - CLI Command Map

Functional map of active CLI commands in `biofilter/api/cli`.

## Global Entry
`biofilter [--db-uri URI] [--debug] [--version|-V] <group> <command> [options]`

- `--db-uri`: sets a global database URI.
- `--debug`: enables global debug mode.
- `--version|-V`: prints version and exits.

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report list
```

## Group `db`

### `db create-db`
- `--db-uri` (required)
- `--overwrite` (flag)
- `--debug` (flag)

Example:
```bash
biofilter db create-db --db-uri sqlite:///biofilter_dev.db --overwrite
```

### `db migrate`
- `--db-uri` (optional)
- `--debug` (flag)
- `--status` (flag)
- `--stamp-head` (flag)
- `--dry-run` (flag)
- `--force` (flag)
- `--target` (default: `head`)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db db migrate --status
```

### `db upgrade`
- `--db-uri` (optional)
- `--seed-dir` (default: `seed`)
- `--debug` (flag)
- `--force` (flag)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db db upgrade --seed-dir seed
```

### `db backup`
- `--db-uri` (optional)
- `--out` (required, file)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db db backup --out ./tests/outputs/backup.sqlite
```

### `db restore`
- `--db-uri` (optional)
- `--in` (required, file)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db db restore --in ./tests/outputs/backup.sqlite
```

### `db export`
- `--db-uri` (optional)
- `--out` (required, directory)
- `--format` (`parquet|csv`, default: `parquet`)
- `--schema-version` (default: `unknown`)
- `--chunksize` (default: `250000`)
- `--table` (optional, repeatable or comma-separated)
- `--exclude-table` (optional, repeatable or comma-separated)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db db export --out ./tests/outputs/export_sqlite --format csv
```

Example with table filters:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db db export --out ./tests/outputs/export_sqlite --format csv --table variants,variant_consequences --exclude-table etl_status
```

### `db import`
- `--db-uri` (optional)
- `--in` (required, directory)
- `--format` (`parquet|csv`, default: `parquet`)
- `--no-rebuild-indexes` (flag)
- `--no-reset-sequences` (flag)
- `--allow-missing-tables` (flag)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_target.db db import --in ./tests/outputs/export_sqlite --format csv --no-rebuild-indexes
```

Example for schema-drift import:
```bash
biofilter --db-uri sqlite:///biofilter_target.db db import --in ./tests/outputs/export_sqlite --format csv --allow-missing-tables
```

## Group `report`

### `report list`
- `--db-uri` (optional)
- `--verbose` (flag)
- `--debug` (flag)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report list --verbose
```

### `report explain`
- `--db-uri` (optional)
- `--report-name` (required)
- `--debug` (flag)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report explain --report-name etl_status
```

### `report example-input`
- `--db-uri` (optional)
- `--report-name` (required)
- `--debug` (flag)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report example-input --report-name etl_status
```

### `report available-columns`
- `--db-uri` (optional)
- `--report-name` (required)
- `--debug` (flag)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report available-columns --report-name etl_status
```

### `report refresh`
- `--db-uri` (optional)
- `--debug` (flag)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report refresh
```

### `report run`
- `--db-uri` (optional)
- `--report-name` (required, alias: `--name`)
- `--params-template` (flag, prints example_input as JSON and exits)
- `--input` (multiple, direct values)
- `--input-file` (optional, `.txt` or `.csv`)
- `--input-column` (optional, CSV column name or index)
- `--param` (multiple, `KEY=VALUE`)
- `--params-json` (optional, JSON object string)
- `--params-file` (optional, `.json|.yml|.yaml`)
- `--output` (optional, export CSV when informed)
- `--debug` (flag)

Notes:
- Use `--input/--input-file` for inputs and `--param` for report options.
- Avoid passing `input_data`, `items`, or `input_path` through `--param` when using `--input/--input-file`.

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name etl_packages --output ./tests/outputs/reports/etl_packages.csv
```

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name entity_relationship_model --input TP53 --input BRCA1 --param relationship_scope=input_to_any
```

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name entity_relationship_model --params-template
```

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name entity_relationship_model --input TP53 --param relationship_types=@./relationship_types.txt
```

## Group `etl`

### `etl update`
- `--db-uri` (optional)
- `--source-system` (multiple)
- `--data-source` (multiple)
- `--run-step` (`extract|transform|load`, multiple)
- `--force-step` (`extract|transform|load`, multiple)
- `--debug` (flag)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db etl update --data-source dbsnp_sample --run-step extract --run-step transform
```

### `etl update-all`
- `--db-uri` (optional)
- `--source-system` (multiple, optional filter)
- `--data-source` (multiple, optional filter)
- `--drop-files / --keep-files` (default: `--keep-files`)
- `--only-active / --all` (default: `--only-active`)
- `--stop-on-error` (flag)
- `--debug` (flag)

Examples:
```bash
# process all active data sources, resuming from where it stopped
biofilter --db-uri sqlite:///biofilter_dev.db etl update-all
```

```bash
# process only NCBI subset and delete raw/processed files after successful load
biofilter --db-uri sqlite:///biofilter_dev.db etl update-all --source-system NCBI --drop-files
```

### `etl restart`
- `--db-uri` (optional)
- `--data-source` (multiple)
- `--source-system` (multiple)
- `--delete-files` (flag)
- `--debug` (flag)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db etl restart --data-source dbsnp_sample --delete-files
```

### `etl rollback`
- `--db-uri` (optional)
- `--package-id` (multiple)
- `--data-source` (multiple)
- `--source-system` (multiple)
- `--delete-files` (flag, only for rollback by data-source/source-system)
- `--debug` (flag)

Examples:
```bash
# rollback a specific ETL package
biofilter --db-uri sqlite:///biofilter_dev.db etl rollback --package-id 123
```

```bash
# rollback an entire data source
biofilter --db-uri sqlite:///biofilter_dev.db etl rollback --data-source gnomad_chr22 --delete-files
```

### `etl status`
- `--db-uri` (optional)
- `--source-system` (multiple)
- `--data-source` (multiple)
- `--only-active / --all` (default: `--all`)
- `--debug` (flag)

Examples:
```bash
# all data sources with latest load result and last execution date
biofilter --db-uri sqlite:///biofilter_dev.db etl status
```

```bash
# filter by source system and active data sources only
biofilter --db-uri sqlite:///biofilter_dev.db etl status --source-system NCBI --only-active
```

### `etl explain`
- `--db-uri` (optional when using `--dtp-script`; required for `--data-source`)
- `--data-source` (multiple)
- `--dtp-script` (multiple)
- `--source-system` (multiple, optional filter for `--data-source`)
- `--debug` (flag)

Examples:
```bash
# explain by datasource name from ETL registry
biofilter --db-uri sqlite:///biofilter_dev.db etl explain --data-source hgnc
```

```bash
# explain directly by dtp script name (no DB lookup needed)
biofilter etl explain --dtp-script dtp_gene_hgnc
```

```bash
# list all available DTP explain documents
biofilter etl explain
```

### `etl index`
- `--db-uri` (optional)
- `--group` (multiple)
- `--drop-only` (flag)
- `--no-drop-first` (flag)
- `--no-write-mode` (flag)
- `--no-read-mode` (flag)
- `--debug` (flag)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db etl index --group genes --group variant
```

## Group `config`

### `config show`
- no arguments

Example:
```bash
biofilter config show
```

### `config init`
- `--path` (default: `.`)
- `--force` (flag)
- `--db-uri`
- `--data-root`

Example:
```bash
biofilter config init --path . --db-uri sqlite:///biofilter_dev.db --data-root ./biofilter_data
```

### `config get`
- positional argument `key` (example: `database.db_uri`)
- `--path` (optional)

Example:
```bash
biofilter config get database.db_uri
```

### `config set`
- positional arguments `key value`
- `--path` (optional)

Example:
```bash
biofilter config set database.db_uri sqlite:///biofilter_dev.db
```

## Notes
- Commands with local `--db-uri` override global `--db-uri`.
- DB URI resolution priority: local > global > `.biofilter.toml`.
- `etl restart` performs rollback first, then reruns extract/transform/load.
- Rollback and restart can be blocked if dependent rows exist in `entity_relationships` from newer/different loads; in this case rollback those dependent loads first.
