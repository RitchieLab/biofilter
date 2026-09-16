# CLI Reference

Every command, with the options that matter. The groups split by what they
do: `report` reads a bundle, `bundle` / `etl` / `db` build one, `config`
inspects settings.

## Global

```bash
biofilter [--bundle PATH] [--db-uri URI] [--debug] COMMAND ...
biofilter --version
```

| Option | For |
|---|---|
| `--bundle PATH` | The bundle to read. What `report` needs. |
| `--db-uri URI` | A writable SQLAlchemy URI. What `etl`, `db` and `bundle plan` need. |
| `--debug` | Debug logging. Accepted by most commands individually too. |

`--bundle` wins over `--db-uri`. Both fall back to the environment
(`BIOFILTER_BUNDLE`, `DATABASE_URL`, `BIOFILTER_DB_URI`) and then to
`.biofilter.toml`.

## report

Reads a bundle. Never writes.

| Command | Options |
|---|---|
| `report list` | `--verbose` |
| `report explain` | `--report-name` |
| `report example-input` | `--report-name` |
| `report available-columns` | `--report-name` |
| `report run` | `--report-name` · `--input` `--input-file` `--input-column` · `--param` `--params-json` `--params-file` `--params-template` · `--output` |
| `report refresh` | — rebuild the report index after adding one |

`--report-name` also accepts the shorter `--name`. Everything except `run`
works without a bundle.

```bash
biofilter --bundle <path> report run --report-name annotate_gene \
  --input TP53 --output genes.csv
```

See [Reports](reports.md) and the [Report Catalog](report_catalog.md).

## bundle

Builds a bundle from a plan.

| Command | Options |
|---|---|
| `bundle plan` | `--out` · `--all-sources` · `--force` |
| `bundle build` | `--plan` `--out` `--data-root` · `--restart` · `--keep-raw` `--keep-processed` · `--min-free-gb` · `--into` `--no-assemble` |
| `bundle info` | takes the bundle directory as an argument |

See [Building Bundles](technical/building_bundles.md).

## etl

Runs one data source at a time. Useful when developing a DTP or re-running
a single source; a full build goes through `bundle build`.

| Command | Options |
|---|---|
| `etl update` | `--data-source` `--source-system` · `--run-step` `--force-step` |
| `etl update-all` | `--data-source` `--source-system` · `--only-active/--all` · `--drop-files/--keep-files` · `--stop-on-error` |
| `etl status` | `--data-source` `--source-system` · `--only-active/--all` |
| `etl explain` | `--data-source` `--source-system` `--dtp-script` |
| `etl restart` | `--data-source` `--source-system` · `--delete-files` |
| `etl rollback` | `--data-source` `--source-system` · `--package-id` · `--delete-files` |
| `etl index` | `--group` · `--drop-only` `--no-drop-first` · `--no-read-mode` `--no-write-mode` |

`--data-source` and `--source-system` are repeatable. `--run-step` and
`--force-step` are too, so a full explicit run is `--run-step extract
--run-step transform --run-step load`.

**`restart` and `rollback` destroy work.** Neither should be run
automatically or without knowing what it will remove.

See [ETL Operations](technical/etl.md).

## db

| Command | Options |
|---|---|
| `db create-db` | `--db-uri` (required) · `--overwrite` |
| `db ping` | — reachability and latency only |
| `db upgrade` | `--seed-dir` · `--force` — re-applies seeds, idempotent |
| `db verify` | `--in` (required) · `--no-hashes` · `--schema` |
| `db export` | `--out` · `--format` · `--table` `--exclude-table` · `--chunksize` · `--schema-version` · `--no-checksums` · `--include-partition-children` |
| `db import` | `--in` · `--format` · `--allow-missing-tables` · `--no-rebuild-indexes` · `--no-reset-sequences` |
| `db backup` | `--out` |
| `db restore` | `--in` |

`db verify` needs no database — it validates a bundle against its own
manifest, and `--schema` makes it exit non-zero on drift, for CI.

**`restore` overwrites.** Confirm the target before running it.

See [Database Operations](technical/database.md).

## config

| Command | Options |
|---|---|
| `config show` | — prints what actually resolved, and from where |
| `config get SECTION.KEY` | `--path` |
| `config set SECTION.KEY VALUE` | `--path` |
| `config init` | `--path` · `--force` · `--db-uri` `--data-root` |

```bash
biofilter config show
biofilter config get database.bundle
```

See [Configuration](technical/configuration.md).
