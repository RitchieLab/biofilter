# AG Start - Biofilter Setup and First Run (CLI/API)

Practical onboarding guide to start Biofilter from scratch.

This guide covers:
- installation (`pip install biofilter` and source mode)
- PostgreSQL database setup
- `.biofilter.toml` initialization and config commands
- schema bootstrap (`db migrate`, `db upgrade`)
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

### 5.1 Run migrations to `head`

```bash
biofilter db migrate --target head
```

Force mode (only when required):

```bash
biofilter db migrate --target head --force
```

Useful diagnostics:

```bash
biofilter db migrate --status
biofilter db migrate --dry-run
```

### 5.2 Apply master seeds (idempotent)

```bash
biofilter db upgrade
```

With explicit seed dir (default is `seed`):

```bash
biofilter db upgrade --seed-dir seed
```

Force flag is available:

```bash
biofilter db upgrade --force
```

Note:
- `db upgrade` is equivalent to migration to head + seed upserts.

---

## 6) Optional: Create DB via CLI

For environments where creating the DB via CLI is desired:

```bash
biofilter db create-db --db-uri "sqlite:///biofilter_dev.db"
```

With overwrite:

```bash
biofilter db create-db --db-uri "sqlite:///biofilter_dev.db" --overwrite
```

For PostgreSQL, many teams prefer creating DB/user with `psql` first, then running `migrate` + `upgrade`.

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
3. `biofilter db migrate --target head`
4. `biofilter db upgrade`
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

# 4) bootstrap DB
biofilter db migrate --target head --force
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
