# Connecting to a Database

Biofilter needs a database to run any report. You have two options:

- **Option A** — connect to a database that already exists (someone else manages it).
- **Option B** — bootstrap a new local database, then run the ETL to populate it.

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

```bash
biofilter config show
```

You're done. Skip to [Find a report that fits your need](finding_reports.md).

---

## Option B — Bootstrap a new local database

Use this when you want to run BF4 fully on your own machine. Two engines are supported:

| Engine | Best for | Notes |
|---|---|---|
| **SQLite** | Quick start, single user, light datasets | No setup, file-based |
| **PostgreSQL** | Production, multi-user, full data | Recommended for variants and large ETLs |

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

```bash
biofilter db migrate --target head
biofilter db upgrade
```

The first command applies all schema migrations. The second loads the seed data (entity groups, relationship types, source systems).

### 3. Run your first ETL

This pulls and ingests data for a single source. Start with `hgnc` (small, fast, no dependencies):

```bash
biofilter etl update --data-source hgnc
biofilter etl status
```

`etl status` shows which data sources are loaded and when. From here you can add more sources (`gene_ncbi`, `reactome`, `mondo`, …) as needed.

For the full ETL operations guide, see [ETL](../etl.md).

---

## Next step

Now that you can talk to a database, [find a report](finding_reports.md) and [run it](running_reports.md).
