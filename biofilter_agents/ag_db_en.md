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

1. **Apply schema**: `db migrate --target head`
2. **Apply master data (seed upsert)**: `db upgrade`
3. **Run ETL**: use `etl` group commands
4. **Monitor**: `report etl_status` and `report etl_packages`

When moving data across environments:
- physical snapshot: `backup` / `restore`
- logical table bundle: `export` / `import`

---

## 3) DB Commands (CLI)

## 3.1 `biofilter db create-db`

Creates a new Biofilter database at the provided URI.

```bash
biofilter db create-db --db-uri "sqlite:///biofilter_dev.db"
```

With overwrite:

```bash
biofilter db create-db --db-uri "sqlite:///biofilter_dev.db" --overwrite
```

When to use:
- new local environment (especially SQLite)
- quick initial bootstrap

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

Runs the canonical upgrade flow:
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
- in most cases, prefer `db upgrade` for complete bootstrap.

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
biofilter db migrate --target head --force
biofilter db upgrade
```

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

