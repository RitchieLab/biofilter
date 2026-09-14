# Connecting to a Database

Biofilter needs a data backend to run any report. You have three options:

- **Option A** — connect to a database that already exists (someone else manages it).
- **Option B** — bootstrap a new local database, then run the ETL to populate it.
- **Option C** — read a Parquet bundle directly, with no database server at all.

Pick the one that matches your situation.

---

## Option A — Connect to an existing database

Use this when you have a connection string from a colleague, a shared lab instance, or a managed deployment.

### What you need

A connection URL in SQLAlchemy format:

```
postgresql+psycopg2://<user>:<password>@<host>:<port>/<database>
```

Example: `postgresql+psycopg2://bioadmin:secret@db.example.com:5432/biofilter_prod`

### Setting it

You can configure the connection in two ways. Pick whichever feels cleaner.

**Via configuration file** (persistent across runs):

```bash
biofilter config init --path .
biofilter config set database.db_uri "postgresql+psycopg2://bioadmin:secret@db.example.com:5432/biofilter_prod"
```

**Via environment variable** (preferred in containers, CI, or short-lived shells):

```bash
export DATABASE_URL="postgresql+psycopg2://bioadmin:secret@db.example.com:5432/biofilter_prod"
```

### Verify the connection

Show the resolved configuration:

```bash
biofilter config show
```

Test that the database is actually reachable:

```bash
biofilter db ping
```

If the ping succeeds, you'll see the engine, host, database name, and latency. You're done — skip to [Find a report that fits your need](finding_reports.md).

---

## Option B — Bootstrap a new local database

Use this when you want to run BF4 fully on your own machine. Two engines are supported:

| Engine         | Best for                                 | Notes                                   |
| -------------- | ---------------------------------------- | --------------------------------------- |
| **SQLite**     | Quick start, single user, light datasets | No setup, file-based                    |
| **PostgreSQL** | Production, multi-user, full data        | Recommended for variants and large ETLs |

### 1. Initialize configuration

```bash
biofilter config init --path .
```

This creates a `.biofilter.toml` in the current directory. Set the database URI and the directory that will hold raw and processed ETL files:

```bash
# SQLite (simplest)
biofilter config set database.db_uri "sqlite:///./biofilter_dev.sqlite3"

# OR PostgreSQL
biofilter config set database.db_uri "postgresql+psycopg2://bioadmin:secret@localhost:5432/biofilter_dev"

biofilter config set etl.data_root "./biofilter_data"
```

Validate:

```bash
biofilter config show
```

### 2. Create the schema

The schema is built by `db create-db`, which creates **all tables** and loads the
**seed data** (entity groups, relationship types, source systems, …) in a single
step. This is the canonical bootstrap command — `db migrate` and `db upgrade`
only handle version tracking and seed refresh on a database that already has a
schema; they do **not** create the initial tables.

**PostgreSQL** — the target database must already exist before BF4 can connect to
it (the client connects on startup). Create the empty database first, then
bootstrap:

```bash
# create the empty database (role must have CREATEDB)
createdb -O admin biofilter_dev

# confirm it is reachable
biofilter db ping

# create tables + load seeds
biofilter db create-db --db-uri "postgresql+psycopg2://admin:admin@localhost:5432/biofilter_dev" --overwrite
```

`--overwrite` is required here because the (empty) database already exists; it is
**not** destructive — table creation uses `create_all` (idempotent) and never drops
data.

**SQLite** — no pre-creation needed; `create-db` creates the file itself:

```bash
biofilter db create-db --db-uri "sqlite:///./biofilter_dev.sqlite3"
```

#### (Optional) Stamp the Alembic baseline and refresh seeds

After the schema exists, you can baseline the migration version and re-apply seeds
idempotently. This is useful so future incremental migrations have a known
starting point:

```bash
biofilter db migrate --force   # stamps/applies migrations up to head
biofilter db upgrade           # re-applies seeds (idempotent: created=0 on a fresh DB)
```

> **Note:** running `db migrate --target head` / `db upgrade` on an empty database
> without first running `db create-db` will report "Schema up-to-date" but leave the
> database without any domain tables — the Alembic head does not contain the table
> DDL. Always bootstrap with `db create-db` first.

### 3. Run your first ETL

This pulls and ingests data for a single source. Start with `hgnc` (small, fast, no dependencies):

```bash
biofilter etl update --data-source hgnc
biofilter etl status
```

`etl status` shows which data sources are loaded and when. From here you can add more sources (`gene_ncbi`, `reactome`, `mondo`, …) as needed.

For the full ETL operations guide, see [ETL](../etl.md).

---

## Option C — Read a Parquet bundle (no database server)

Use this when someone has given you a **Parquet bundle** — a directory of
`.parquet` files that is a point-in-time snapshot of the knowledge base. This
is the standard setup on HPC clusters, where running a database server is not
practical.

Nothing to install or start: point Biofilter at the bundle's `tables/`
directory using the `parquet://` scheme.

```bash
export BIOFILTER_DB_URI="parquet:///shared/bundles/bf4_2026_06/tables"

biofilter report list
biofilter report run --report-name annotation_master_gene --input APOE --output apoe.csv
```

Behind the scenes Biofilter queries the files with DuckDB. Every report works
unchanged — they run through the same ORM layer as the other backends.

Two things to know:

- The backend is **read-only**. ETL runs and migrations need a PostgreSQL or
  SQLite target. Refreshing the data means getting a new bundle.
- The URI must point at `tables/`, not at the bundle root that holds
  `manifest.json`.

For the full reference — how views are registered, the partitioned-table
caveat, performance numbers, and how bundles are produced — see
[Parquet Backend](../parquet_backend.md).

---

## Next step

Now that you can talk to a database, [find a report](finding_reports.md) and [run it](running_reports.md).
