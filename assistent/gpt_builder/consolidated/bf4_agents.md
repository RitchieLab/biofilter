# BF4 Operational Guides



<!-- ===== SOURCE FILE: biofilter_agents/ag_db_en.md ===== -->

# AG DB - Database Operations in Biofilter (CLI/API)

Detailed guide for database administration in Biofilter.

Covers:
- database creation
- migrations and upgrade (schema + seeds)
- backup and restore (physical snapshot)
- export and import (logical table-level clone)
- validation commands
- API usage
- LLM assistant playbook

---

## 1) Goal

This guide helps you operate `biofilter db` safely in dev, staging, and production.

Main commands in the `db` group:
- `create-db`
- `migrate`
- `upgrade`
- `backup`
- `restore`
- `export`
- `import`

---

## 2) Strategy Overview

Use this simple rule:

1. **Bootstrap a new database (schema + seeds)**: `db create-db` — this is the
   only command that creates the domain tables. `db migrate` / `db upgrade` do
   **not** create the initial schema (the Alembic head carries no table DDL).
2. **Refresh seeds / version tracking on an existing schema**: `db upgrade`
   (migrate to head + idempotent seed upsert)
3. **Run ETL**: use `etl` group commands
4. **Monitor**: `report etl_status` and `report etl_packages`

> **PostgreSQL caveat:** the target database must already exist before BF4 can
> connect to it. Create the empty database first (`createdb <name>`), then run
> `db create-db ... --overwrite`.

When moving data across environments:
- physical snapshot: `backup` / `restore`
- logical table bundle: `export` / `import`

---

## 3) DB Commands (CLI)

## 3.1 `biofilter db create-db`

The canonical bootstrap command. It creates **all domain tables** (`create_all`)
and loads the **seed data** in a single step. Use this — not `migrate`/`upgrade` —
to stand up a new database.

**SQLite** — creates the file itself, no pre-creation needed:

```bash
biofilter db create-db --db-uri "sqlite:///biofilter_dev.db"
```

**PostgreSQL** — the database must already exist (BF4 connects on startup), so
create it first, then bootstrap with `--overwrite`:

```bash
createdb -O admin biofilter_dev
biofilter db create-db --db-uri "postgresql+psycopg2://admin:admin@localhost:5432/biofilter_dev" --overwrite
```

`--overwrite` only bypasses the "database already exists" guard; it is **not**
destructive — `create_all` is idempotent and never drops data.

When to use:
- any new environment (SQLite or PostgreSQL)
- the correct first step of a from-scratch bootstrap

---

## 3.2 `biofilter db migrate`

Runs Alembic migrations.

Upgrade to head:

```bash
biofilter db migrate --target head
```

Revision status:

```bash
biofilter db migrate --status
```

Dry-run SQL:

```bash
biofilter db migrate --dry-run
```

Stamp head without DDL (advanced):

```bash
biofilter db migrate --stamp-head --force
```

Upgrade with force:

```bash
biofilter db migrate --target head --force
```

Notes:
- `--force` is for risky/advanced scenarios.
- `--stamp-head` should be used carefully in controlled environments.

---

## 3.3 `biofilter db upgrade`

Runs the upgrade flow on an **existing** schema:
- migrate to `head`
- apply seeds (idempotent upsert)

```bash
biofilter db upgrade
```

With explicit seed dir:

```bash
biofilter db upgrade --seed-dir seed
```

With force:

```bash
biofilter db upgrade --force
```

Practical rule:
- `db upgrade` does **not** create the schema — it assumes the tables already
  exist (built by `db create-db`). Use it to refresh seeds and align the Alembic
  revision, not to bootstrap a fresh database.

---

## 3.4 `biofilter db backup`

Creates a physical snapshot of the current database.

```bash
biofilter db backup --out ./backups/biofilter_dev.snapshot
```

Examples:
- SQLite: file copy
- PostgreSQL: dump flow compatible with restore

Best practices:
- create backups before sensitive migrations
- include timestamp/version in backup path naming

---

## 3.5 `biofilter db restore`

Restores a physical snapshot.

```bash
biofilter db restore --in ./backups/biofilter_dev.snapshot
```

Warning:
- restore overwrites current target DB state.
- confirm target `db_uri` before execution.

---

## 3.6 `biofilter db export`

Exports a logical clone bundle (`manifest.json` + `tables/`).

```bash
biofilter db export --out ./exports/biofilter_bundle --format parquet
```

With table filters:

```bash
biofilter db export \
  --out ./exports/biofilter_bundle \
  --format csv \
  --table variants,variant_consequences \
  --exclude-table etl_status
```

Useful options:
- `--schema-version`
- `--chunksize`
- `--table` (include)
- `--exclude-table` (exclude)

---

## 3.7 `biofilter db import`

Imports a previously exported logical bundle.

```bash
biofilter db import --in ./exports/biofilter_bundle --format parquet
```

Variants:

```bash
biofilter db import \
  --in ./exports/biofilter_bundle \
  --format csv \
  --no-rebuild-indexes \
  --no-reset-sequences \
  --allow-missing-tables
```

When to use:
- replicate state across environments
- load a controlled logical snapshot

---

## 4) Recommended Flows

### 4.1 First bootstrap (new environment)

```bash
biofilter config show

# PostgreSQL: create the empty database first
createdb -O admin biofilter_dev

# create schema + seeds (the actual bootstrap)
biofilter db create-db --db-uri "postgresql+psycopg2://admin:admin@localhost:5432/biofilter_dev" --overwrite

# (optional) baseline Alembic + refresh seeds
biofilter db migrate --force
biofilter db upgrade
```

> SQLite is simpler — `biofilter db create-db --db-uri "sqlite:///biofilter_dev.db"`
> creates the file, schema, and seeds in one go (no `createdb`, no `--overwrite`).

### 4.2 Safe deployment flow

```bash
biofilter db backup --out ./backups/pre_deploy.snapshot
biofilter db migrate --status
biofilter db migrate --target head
biofilter db upgrade
biofilter db migrate --status
```

### 4.3 Logical replication across environments

Source:

```bash
biofilter db export --out ./exports/prod_bundle --format parquet
```

Target:

```bash
biofilter db import --in ./exports/prod_bundle --format parquet
```

---

## 5) Post-Operation Quick Validation

Check revision:

```bash
biofilter db migrate --status
```

Check active config:

```bash
biofilter config show
```

Check ETL support reports:

```bash
biofilter report run --name etl_status
biofilter report run --name etl_packages
```

---

## 6) API Usage (Python)

`DBComponent` usage example:

```python
from biofilter import Biofilter

bf = Biofilter(db_uri="postgresql+psycopg2://bioadmin:change_me@localhost:5432/biofilter_dev")
bf.db.connect()

# migrate
bf.db.migrate(action="upgrade", target="head", force=False)

# upgrade (schema + seed upsert)
bf.db.upgrade(seed_dir="seed")

# backup
bf.db.backup("./backups/dev.snapshot")

# export bundle
bf.db.export(out_dir="./exports/dev_bundle", fmt="parquet")
```

Higher-risk actions:

```python
# restore
bf.db.restore("./backups/dev.snapshot")

# import bundle
bf.db.import_(
    in_dir="./exports/dev_bundle",
    fmt="parquet",
    rebuild_indexes=True,
    reset_postgres_sequences=True,
    allow_missing_tables=False,
)
```

---

## 7) Common Errors and Fixes

- **DB connection error**
  - validate `database.db_uri` with `biofilter config show`
  - test host/port/user/password at PostgreSQL level

- **Schema mismatch with code**
  - run `biofilter db migrate --status`
  - apply `biofilter db migrate --target head`

- **Seeds not reflected**
  - run `biofilter db upgrade`

- **Import failing due to missing tables**
  - use `--allow-missing-tables` when appropriate
  - or re-export a complete bundle

- **Postgres sequence problems after import**
  - avoid `--no-reset-sequences` unless you know what you are doing

---

## 8) LLM Assistant Playbook (DB Ops)

Minimum checklist before destructive commands:
- confirm target environment (`db_uri`)
- confirm recent backup availability
- confirm maintenance window (for production)

Recommended assistant sequence:

1. `biofilter config show`
2. `biofilter db migrate --status`
3. If needed, `biofilter db backup --out ...`
4. `biofilter db migrate --target head`
5. `biofilter db upgrade`
6. `biofilter db migrate --status`
7. Validate with ETL support reports

Safety rules:
- never execute `restore` without explicit confirmation
- never use `stamp-head` without clear justification
- always provide a final summary (action, environment, result, risks)

Suggested base prompt:

```text
You are operating the Biofilter DB module.
1) Show active config and migration status.
2) Execute migration to head and seed upgrade.
3) Validate final status.
4) Report summary with risks and next step.
Do not execute restore/stamp-head without explicit confirmation.
```

---

## 9) Short Reference Script (DB Day-0)

```bash
# validate context
biofilter config show
biofilter db --help

# bootstrap schema + seeds
biofilter db migrate --target head --force
biofilter db upgrade

# validate
biofilter db migrate --status

# optional: snapshot
biofilter db backup --out ./backups/post_upgrade.snapshot
```

---

## 10) Internal References

- start guide: `biofilter_agents/ag_start.md`
- ETL guide (PT): `biofilter_agents/ag_etl_pt.md`
- ETL guide (EN): `biofilter_agents/ag_etl_en.md`
- command map: `biofilter/api/cli/ag_01_commands.md`
- DB CLI group: `biofilter/api/cli/groups/db.py`
- DB component API: `biofilter/core/components/db_component.py`



<!-- ===== SOURCE FILE: biofilter_agents/ag_etl_en.md ===== -->

# AG ETL - Update and Operations (CLI/API/Reports)

Detailed guide to run and monitor ETL in Biofilter, covering:
- CLI usage
- API usage (Python/Notebook)
- support reports
- recommended flow for long and resumable runs
- playbook for LLM assistants

---

## 1) Goal

This guide explains how to:
- update one or more DataSources manually (`etl update`)
- update many DataSources sequentially with resume support (`etl update-all`)
- monitor status and audit history (`etl status`, `etl_status`, `etl_packages`)
- restart or rollback when needed (`etl restart`, `etl rollback`)

---

## 2) Quick Concepts

- **DataSource**: ETL source unit (for example: `hgnc`, `dbsnp_chr1`, `gnomad_chr22`).
- **ETL pipeline**: `extract -> transform -> load`.
- **ETLPackage**: execution record for ETL stages.
- **Resume behavior**: in `update-all`, DataSources already successful are skipped.

---

## 3) Prerequisites

- DB configured (`--db-uri` or `.biofilter.toml`).
- ETL paths configured (`[etl].data_root`) when needed.
- Python environment ready (`poetry run ...` is recommended during development).

Validation example:

```bash
poetry run biofilter etl --help
```

---

## 4) ETL Commands (CLI)

### 4.1 `biofilter etl update`

Manual, explicit update for a selected subset.

Common usage:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl update --data-source hgnc
```

Specific steps:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl update \
  --data-source dbsnp_chr22 \
  --run-step extract --run-step transform --run-step load
```

Force a step:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl update \
  --data-source hgnc \
  --force-step extract
```

Important:
- If no `--source-system` and no `--data-source` is passed, command aborts by design.

---

### 4.2 `biofilter etl update-all`

Sequential update for multiple DataSources, with resume-friendly behavior.

Basic (all active):

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl update-all
```

Filter by source system:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl update-all \
  --source-system NCBI
```

Drop raw/processed files after each successful load:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl update-all \
  --drop-files
```

Stop on first failure:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl update-all \
  --stop-on-error
```

Current behavior:
- resolves DataSources in deterministic order (`data_source_id` ascending)
- checks latest `load` status per DataSource
- skips DataSources already in success state
- runs `extract -> transform -> load` for pending ones
- with `--drop-files`, deletes `raw/processed` only after successful load
- prints a final summary: `selected`, `skipped`, `processed`, `succeeded`, `failed`

---

### 4.3 `biofilter etl status`

Quick operational view by DataSource: success/fail + latest execution time.

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl status
```

With filter:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl status \
  --source-system NCBI --only-active
```

---

### 4.4 `biofilter etl restart`

Rollback DataSource data and rerun full ETL pipeline.

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl restart \
  --data-source gnomad_chr22
```

With file cleanup:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl restart \
  --data-source gnomad_chr22 \
  --delete-files
```

---

### 4.5 `biofilter etl rollback`

Rollback without rerunning ETL.

By package:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl rollback --package-id 123
```

By DataSource:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl rollback \
  --data-source gnomad_chr22 \
  --delete-files
```

---

## 5) Operational Support Reports

### 5.1 `etl_status` (DataSource-level consolidated view)

- consolidated ETL state per DataSource
- includes DataSources with no packages yet

CLI:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db report run --name etl_status
```

API:

```python
df_status = bf.report.run("etl_status", only_active=False)
```

Useful columns:
- `source_system`
- `data_source`
- `extract_status`
- `transform_status`
- `load_status`
- `pipeline_ok`
- `latest_error`

---

### 5.2 `etl_packages` (detailed audit)

- raw package history
- best for debugging failures and timing

CLI:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db report run --name etl_packages
```

API:

```python
df_pkg = bf.report.run("etl_packages", only_active=False)
```

Useful columns:
- `package_id`
- `operation_type`
- `status`
- `extract_status`, `transform_status`, `load_status`
- `extract_minutes`, `transform_minutes`, `load_minutes`

---

## 6) API Usage (Python/Notebook)

### 6.1 Setup

```python
from biofilter import Biofilter
import pandas as pd

bf = Biofilter(db_uri="sqlite:///biofilter_dev.db", debug_mode=False)
bf.db.connect()
```

---

### 6.2 Run a targeted update

```python
bf.etl.update(
    data_sources=["hgnc"],
    run_steps=["extract", "transform", "load"],
    force_steps=[],
)
```

---

### 6.3 Run resumable update-all

```python
summary = bf.etl.update_all(
    source_system=None,
    data_sources=None,
    drop_files_on_success=False,
    only_active=True,
    stop_on_error=False,
)
print(summary)
```

Example output:

```python
{
    "selected": 120,
    "skipped": 95,
    "processed": 25,
    "succeeded": 24,
    "failed": 1,
}
```

---

### 6.4 Monitoring in Notebook

```python
df_status = bf.report.run("etl_status", only_active=False)
display(
    df_status[
        ["source_system", "data_source", "extract_status", "transform_status", "load_status", "pipeline_ok", "latest_error"]
    ].sort_values(["source_system", "data_source"])
)
```

```python
df_pkg = bf.report.run("etl_packages", only_active=False)
display(
    df_pkg[
        ["package_id", "created_at", "source_system", "data_source", "operation_type", "status", "load_status"]
    ].sort_values(["package_id"], ascending=False).head(50)
)
```

---

## 7) Recommended Operational Flow

1. Check current state with `etl status` + `etl_status` report.
2. Run `etl update-all` (first cycles usually with `--keep-files`).
3. Investigate failures in `etl_packages`.
4. Fix source/input/runtime issues.
5. Run `etl update-all` again (resume skips already completed DataSources).
6. After stability, consider `--drop-files` to reduce disk usage.

---

## 8) Quick Troubleshooting

- **Error: "No source_system or data_sources provided. Aborting."**
  - expected for `etl update`; provide explicit target.
  - use `etl update-all` for batch runs.

- **DataSource does not progress in `update-all`**
  - check latest load package in `etl_packages`.
  - check `latest_error` in `etl_status`.

- **Intermittent processing failure**
  - rerun `update-all`; flow is resumable.
  - use `--stop-on-error` only when you want early interruption.

- **Low disk space**
  - run with `--drop-files` after validating stable loads.

---

## 9) LLM Assistant Playbook

### 9.1 Pre-run checklist

- confirm target `db_uri`
- confirm run mode (`update` vs `update-all`)
- confirm file policy (`--drop-files` vs `--keep-files`)
- log executed command and timestamp

### 9.2 Recommended strategy

1. Run `etl status`.
2. If broad backlog exists, run `etl update-all --only-active`.
3. After run, collect:
   - `report run --name etl_status`
   - `report run --name etl_packages`
4. Deliver summary with:
   - processed/succeeded/failed/skipped
   - failed DataSources
   - next recommended action

### 9.3 Safety rules

- do not run rollback automatically without explicit confirmation
- avoid `--drop-files` by default on sensitive environments
- prefer `update-all` for controlled resumable operation
- always report failures with context (`data_source`, stage, error)

### 9.4 Suggested base prompt

```text
You are operating Biofilter ETL.
1) Run `biofilter etl status` and summarize pending items.
2) Run `biofilter etl update-all --only-active`.
3) At the end, run reports `etl_status` and `etl_packages`.
4) Provide a summary: succeeded/failed/skipped, failed data_sources, recommendations.
Do not execute rollback without confirmation.
```

---

## 10) Internal References

- CLI command map: `biofilter/api/cli/ag_01_commands.md`
- ETL CLI group: `biofilter/api/cli/groups/etl.py`
- ETL manager: `biofilter/modules/etl/etl_manager.py`
- Reports:
  - `biofilter/modules/report/reports/report_etl_status.py`
  - `biofilter/modules/report/reports/report_etl_packages.py`

---

## 11) Document Status

- file: `biofilter_agents/ag_etl_en.md`
- scope: ETL operations (CLI/API/Reports)
- intended future use: source material for official docs



<!-- ===== SOURCE FILE: biofilter_agents/ag_report_en.md ===== -->

# AG Report - Report Operations in Biofilter (CLI/API/Explain Guides)

Detailed guide for working with the Biofilter report layer.

Covers:
- report discovery and introspection
- report execution via CLI and API
- dynamic parameter passing (`--input`, `--param`, JSON/YAML)
- explain guide architecture (`reports_explain`)
- authoring pattern for new reports
- LLM assistant playbook

---

## 1) Goal

This guide helps you run and maintain reports in a way that scales as new reports are added, without changing CLI support code for each report.

Key design principles:
- report logic lives in `modules/report/reports/report_*.py`
- report explain/tutorial content lives in `modules/report/reports_explain/report_*.md`
- CLI is generic and dynamic (`report run` with generic parameter injection)

---

## 2) Report Architecture

Each report is composed of:

1. Python report module:
- path: `biofilter/modules/report/reports/report_<something>.py`
- typically defines:
  - `name`
  - `description`
  - `run()`
  - `available_columns()`
  - `example_input()`
  - optional `explain()` fallback

2. Explain/Tutorial markdown:
- path: `biofilter/modules/report/reports_explain/report_<something>.md`
- used by `biofilter report explain`

Explain resolution behavior:
- first tries `reports_explain/report_<module>.md`
- then tries legacy paths (if present)
- if no guide exists, falls back to report class `explain()`

This gives you dynamic explain docs per report while keeping backwards compatibility.

---

## 3) Discover and Inspect Reports (CLI)

List reports:

```bash
biofilter report list
biofilter report list --verbose
```

Show explain/tutorial:

```bash
biofilter report explain --report-name etl_status
```

Show expected example input from report class:

```bash
biofilter report example-input --report-name entity_relationship_model
```

Show available output columns:

```bash
biofilter report available-columns --report-name etl_packages
```

Refresh report cache:

```bash
biofilter report refresh
```

---

## 4) Run Reports (CLI)

Basic run:

```bash
biofilter report run --report-name etl_status
```

Export CSV:

```bash
biofilter report run --report-name etl_packages --output ./etl_packages.csv
```

Show params template (from `example_input()`):

```bash
biofilter report run --report-name entity_relationship_model --params-template
```

Pass direct inputs:

```bash
biofilter report run --report-name entity_filter --input BRCA1 --input TP53
```

Pass input file:

```bash
biofilter report run --report-name entity_filter --input-file ./entities.txt
biofilter report run --report-name entity_filter --input-file ./entities.csv --input-column symbol
```

Pass generic parameters:

```bash
biofilter report run \
  --report-name entity_relationship_model \
  --input TP53 --input BRCA1 \
  --param relationship_scope=input_to_any \
  --param deduplicate_pairs=true
```

Pass parameter files:

```bash
biofilter report run --report-name entity_relationship_model --params-file ./params.yaml
biofilter report run --report-name entity_relationship_model --params-json '{"relationship_scope":"input_to_any"}'
```

Large value from file in a single param:

```bash
biofilter report run \
  --report-name entity_relationship_model \
  --input TP53 \
  --param relationship_types=@./relationship_types.txt
```

Note:
- `--report-name` is the canonical option (`--name` is still accepted as alias).

---

## 5) Inputs vs Params (Important Rule)

Use:
- `--input` / `--input-file` for report inputs (`input_data`)
- `--param` for report options (scope, filters, toggles, limits, etc.)

Avoid mixing input channels:
- if `--input`/`--input-file` is provided, do not pass `input_data`, `items`, or `input_path` through `--param`/JSON/YAML.
- CLI enforces this and returns a friendly error to prevent ambiguous execution.

---

## 6) Parameter Parsing Behavior

`--param KEY=VALUE` coercion rules:
- `true` / `false` -> boolean
- `null` / `none` -> `None`
- JSON/py-literal values are parsed when possible:
  - lists: `["a","b"]`
  - dicts: `{"k":"v"}`
  - numbers: `123`, `4.5`
- `@path` loads value from file
- `@@something` escapes a literal `@something`

`--params-file` supports:
- `.json`
- `.yml`
- `.yaml`

If JSON/YAML root is not a dict, it is mapped to `{"input_data": <value>}`.

---

## 7) Run Reports via API (Notebook/Python)

Setup:

```python
from biofilter import Biofilter

bf = Biofilter(db_uri="sqlite:///biofilter_dev.db", debug_mode=False)
```

Examples:

```python
df_status = bf.report.run("etl_status", only_active=False)

df_rel = bf.report.run(
    "entity_relationship_model",
    input_data=["TP53", "BRCA1", "NOT_FOUND_ENTITY"],
    relationship_scope="input_to_any",
)

```

Introspection in API:

```python
print(bf.report.explain("etl_status"))
print(bf.report.example_input("entity_relationship_model"))
print(bf.report.available_columns("etl_packages"))
```

---

## 8) Built-in Reports (Current)

- `etl_status`
- `etl_packages`
- `entity_filter`
- `entity_relationship_model`
- `variant_gene_location_model`
- `db_pg_table_stats` (Postgres only)
- `db_pg_index_stats` (Postgres only)
- `qry_template`

Always use `biofilter report list --verbose` to confirm what is available in your runtime.

---

## 9) Authoring New Reports (Recommended Pattern)

For a new report `my_report`:

1. Create Python module:
- `biofilter/modules/report/reports/report_my_report.py`

2. Define:
- `name = "my_report"`
- `description`
- `run()`
- `available_columns()`
- `example_input()`

3. Create explain guide:
- `biofilter/modules/report/reports_explain/report_my_report.md`

4. Add tests:
- unit tests for report behavior
- optional integration tests via CLI/API

5. Validate:

```bash
biofilter report list --verbose
biofilter report explain --report-name my_report
biofilter report run --report-name my_report --params-template
```

Result:
- new reports become self-documented and executable without changing CLI support code.

---

## 10) Troubleshooting

If report is not found:
- run `biofilter report list`
- check exact report name
- use friendly suggestions from CLI output

If explain does not show markdown:
- verify file exists at `reports_explain/report_<module>.md`
- ensure filename matches report module pattern

If parameter parsing fails:
- test with `--params-template` first
- use `--params-json` or `--params-file` for complex objects
- quote JSON properly in shell

If Postgres-only reports fail:
- confirm DB is PostgreSQL for `db_pg_table_stats` and `db_pg_index_stats`

---

## 11) LLM Assistant Playbook

When an assistant runs reports:

1. Discover:
- `report list --verbose`

2. Understand:
- `report explain --report-name <report>`
- `report run --report-name <report> --params-template`

3. Execute:
- start with minimal command
- add `--input` / `--param` progressively
- export with `--output` when needed

4. Diagnose:
- prefer `etl_packages` for ETL-level audit
- prefer `etl_status` for quick consolidated health

This flow keeps report operations deterministic, explainable, and easy to automate.



<!-- ===== SOURCE FILE: biofilter_agents/ag_start.md ===== -->

# AG Start - Biofilter Setup and First Run (CLI/API)

Practical onboarding guide to start Biofilter from scratch.

This guide covers:
- installation (`pip install biofilter` and source mode)
- PostgreSQL database setup
- `.biofilter.toml` initialization and config commands
- schema bootstrap (`db create-db`; then optional `db migrate` / `db upgrade`)
- first ETL commands
- status/audit reports
- notebook/API quickstart

---

## 1) Quick Outcome

By the end, you will be able to:
- run `biofilter --help`
- connect Biofilter to your DB
- run migrations and seeds
- run ETL and monitor status

---

## 2) Installation

### 2.1 Option A - Install from PyPI

```bash
pip install biofilter
```

Then validate:

```bash
biofilter --help
```

### 2.2 Option B - Install from source (recommended for contributors)

```bash
git clone <your_repo_url>
cd biofilter
pip install -e .
```

Or with Poetry:

```bash
poetry install
poetry run biofilter --help
```

---

## 3) Prepare PostgreSQL

If you already have a PostgreSQL DB ready, skip to section 4.

Example with `psql` (adjust names/passwords for your environment):

```sql
CREATE ROLE bioadmin WITH LOGIN PASSWORD 'change_me';
CREATE DATABASE biofilter_dev OWNER bioadmin;
GRANT ALL PRIVILEGES ON DATABASE biofilter_dev TO bioadmin;
```

Connection string example:

```text
postgresql+psycopg2://bioadmin:change_me@localhost:5432/biofilter_dev
```

---

## 4) Initialize `.biofilter.toml`

Create template in project root:

```bash
biofilter config init --path .
```

Or prefill DB and data root:

```bash
biofilter config init \
  --path . \
  --db-uri "postgresql+psycopg2://bioadmin:change_me@localhost:5432/biofilter_dev" \
  --data-root "./biofilter_data"
```

Show resolved config:

```bash
biofilter config show
```

Get one value:

```bash
biofilter config get database.db_uri
```

Set one value:

```bash
biofilter config set database.db_uri "postgresql+psycopg2://bioadmin:change_me@localhost:5432/biofilter_dev"
```

Set ETL data root:

```bash
biofilter config set etl.data_root "./biofilter_data"
```

---

## 5) Bootstrap Database Schema

The schema and seeds are created by `db create-db`. This is the **only** command
that builds the domain tables — `db migrate` / `db upgrade` do not create the
initial schema (the Alembic head carries no table DDL).

### 5.1 Create schema + seeds (`db create-db`)

**SQLite** — creates the file, tables, and seeds in one step:

```bash
biofilter db create-db --db-uri "sqlite:///biofilter_dev.db"
```

**PostgreSQL** — the database must already exist before BF4 can connect, so create
the empty database first, then bootstrap with `--overwrite`:

```bash
createdb -O admin biofilter_dev
biofilter db create-db --db-uri "postgresql+psycopg2://admin:admin@localhost:5432/biofilter_dev" --overwrite
```

`--overwrite` only bypasses the "already exists" guard; it is **not** destructive
(`create_all` is idempotent and never drops data).

### 5.2 (Optional) Baseline Alembic and refresh seeds

After the schema exists, you can stamp the migration baseline and re-apply seeds
idempotently:

```bash
biofilter db migrate --force   # applies/stamps migrations up to head
biofilter db upgrade           # migrate to head + idempotent seed upsert
```

Useful diagnostics:

```bash
biofilter db migrate --status
biofilter db migrate --dry-run
```

> **Pitfall:** running `db migrate --target head` / `db upgrade` on an empty
> database *without* `db create-db` first reports "Schema up-to-date" but leaves
> the database with no domain tables. Always bootstrap with `db create-db`.

---

## 7) First Validation Checks

List top-level commands:

```bash
biofilter --help
```

Check ETL command group:

```bash
biofilter etl --help
```

Check report command group:

```bash
biofilter report --help
```

Check DB command group:

```bash
biofilter db --help
```

---

## 8) First ETL Execution

### 8.1 Single DataSource

```bash
biofilter etl update --data-source hgnc
```

### 8.2 Batch resumable execution

```bash
biofilter etl update-all
```

Useful variants:

```bash
biofilter etl update-all --source-system NCBI
biofilter etl update-all --drop-files
biofilter etl update-all --stop-on-error
```

---

## 9) Monitor ETL Progress and Results

### 9.1 Fast operational status

```bash
biofilter etl status
```

### 9.2 Report: consolidated status

```bash
biofilter report run --name etl_status
```

### 9.3 Report: package audit

```bash
biofilter report run --name etl_packages
```

Export report to CSV:

```bash
biofilter report run --name etl_packages --output ./etl_packages.csv
```

---

## 10) API / Notebook Quickstart

```python
from biofilter import Biofilter

bf = Biofilter(
    db_uri="postgresql+psycopg2://bioadmin:change_me@localhost:5432/biofilter_dev",
    debug_mode=False,
)
bf.db.connect()

# ETL
summary = bf.etl.update_all(only_active=True)
print(summary)

# Reports
status_df = bf.report.run("etl_status", only_active=False)
pkg_df = bf.report.run("etl_packages", only_active=False)

print(status_df.head())
print(pkg_df.head())
```

---

## 11) Common Issues

- **`No source_system or data_sources provided. Aborting.`**
  - expected for `etl update`; pass a target.
  - use `etl update-all` for broad runs.

- **Migration not applied / revision mismatch**
  - run `biofilter db migrate --status`.
  - apply `biofilter db migrate --target head`.

- **Seeds not available after migration**
  - run `biofilter db upgrade`.

- **Wrong database target**
  - run `biofilter config show`.
  - confirm `database.db_uri`.

---

## 12) Minimal LLM Operator Playbook

Recommended sequence for an automation assistant:

1. `biofilter config show`
2. `biofilter db migrate --status`
3. New database? `biofilter db create-db --db-uri <uri> [--overwrite]` (Postgres: `createdb` first) — creates schema + seeds
4. Existing schema? `biofilter db upgrade` (migrate to head + seed refresh)
5. `biofilter etl update-all --only-active`
6. `biofilter etl status`
7. `biofilter report run --name etl_status`
8. `biofilter report run --name etl_packages`

Safety rules:
- do not run rollback automatically without explicit approval
- avoid `--drop-files` by default in production
- include command outputs and summary in every run report

---

## 13) Suggested First-Day Command Script

```bash
# 1) validate CLI
biofilter --help

# 2) initialize config
biofilter config init --path . \
  --db-uri "postgresql+psycopg2://bioadmin:change_me@localhost:5432/biofilter_dev" \
  --data-root "./biofilter_data"

# 3) check config
biofilter config show

# 4) bootstrap DB (schema + seeds)
#    Postgres must exist first; create-db is what builds the tables.
createdb -O bioadmin biofilter_dev
biofilter db create-db --db-uri "postgresql+psycopg2://bioadmin:change_me@localhost:5432/biofilter_dev" --overwrite

#    (optional) baseline Alembic + refresh seeds
biofilter db migrate --force
biofilter db upgrade

# 5) run ETL
biofilter etl update-all

# 6) monitor
biofilter etl status
biofilter report run --name etl_status
biofilter report run --name etl_packages
```

---

## 14) Internal References

- ETL operation guide: `biofilter_agents/ag_etl_en.md`
- CLI command map: `biofilter/api/cli/ag_01_commands.md`
- ETL CLI group: `biofilter/api/cli/groups/etl.py`
- DB commands: `biofilter/api/cli/groups/db.py`
- Config commands: `biofilter/api/cli/groups/config.py`
