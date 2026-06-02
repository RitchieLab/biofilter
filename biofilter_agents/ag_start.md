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
