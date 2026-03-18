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
- `--name` (required)
- `--as-csv` (flag)
- `--output` (required when `--as-csv`)
- `--debug` (flag)

Example:
```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run --name etl_packages --as-csv --output ./tests/outputs/reports/etl_packages.csv
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
