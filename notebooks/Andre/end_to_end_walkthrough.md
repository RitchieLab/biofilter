# End-to-End Walkthrough (PostgreSQL)

A complete, copy-paste-friendly run that takes you from an empty machine to a
working Biofilter database with data loaded and a report returning results.

This walkthrough uses **PostgreSQL** as the backend and installs Biofilter from
**PyPI** into a virtual environment. Every step shows the command and a trimmed
sample of its output so you know what "success" looks like.

> **Prerequisites**
>
> - PostgreSQL running locally (this guide assumes a server reachable at
>   `localhost:5432`).
> - Python 3.10+ available on your `PATH`.

---

## 1. Create the PostgreSQL role and database

Biofilter connects to an **existing** database on startup, so the role and the
database must exist before any `biofilter` command runs. Create a dedicated login
role with `CREATEDB`, then a database it owns.

```bash
psql postgres -c "CREATE ROLE admin LOGIN PASSWORD 'admin' CREATEDB;"
# CREATE ROLE

psql postgres -c "CREATE DATABASE biofilter_dev OWNER admin;"
# CREATE DATABASE
```

> `admin`/`admin` is a weak credential — fine for a local dev box, never for
> anything exposed.

### Verify the role and the connection

```bash
psql postgres -c "\du admin"
#      List of roles
#  Role name | Attributes
# -----------+------------
#  admin     | Create DB

# Connect as the new role to confirm the database is reachable
psql "postgresql://admin:admin@localhost/biofilter_dev" -c "SELECT current_user, current_database();"
#  current_user | current_database
# --------------+------------------
#  admin        | biofilter_dev
```

---

## 2. Set up the Python environment and install Biofilter

Use an isolated virtual environment so Biofilter and its dependencies don't leak
into your system Python.

```bash
# Activate the project's virtual environment (create it first with
# `python -m venv venv` if it does not exist yet)
source venv/bin/activate

# (optional) upgrade pip
pip install --upgrade pip

# Install Biofilter from PyPI — pulls SQLAlchemy, Alembic, pandas, psycopg2, etc.
pip install biofilter
# ...
# Successfully installed biofilter-4.1.3 alembic-1.18.4 sqlalchemy-2.0.50 pandas-2.3.3 ...
```

### Confirm the CLI is available

```bash
biofilter --help
# Usage: biofilter [OPTIONS] COMMAND [ARGS]...
#
#   Biofilter 4 CLI - Omics Knowledge Platform
#
# Commands:
#   config  Configuration inspection and helpers.
#   db      Database transfer utilities (backup/restore/export/import).
#   etl     Run and manage ETL pipelines.
#   report  Run and manage reports.
```

---

## 3. Initialize the configuration file

Creating a `.biofilter.toml` means you won't have to pass `--db-uri` on every
command — the CLI reads the connection string from this file.

```bash
biofilter config init
# ✅ Created: /Users/.../bf4-test/.biofilter.toml
```

Point the config at the PostgreSQL database created in step 1:

```bash
biofilter config set database.db_uri "postgresql+psycopg2://admin:admin@localhost/biofilter_dev"
# ✅ Set database.db_uri = postgresql+psycopg2://admin:admin@localhost/biofilter_dev
```

Validate the resolved configuration:

```bash
biofilter config show
# 📄 Biofilter configuration
# Config file:
#   /Users/.../bf4-test/.biofilter.toml
# Resolved values:
#   db_uri: postgresql+psycopg2://admin:admin@localhost/biofilter_dev
```

The generated `.biofilter.toml` looks like this (note `auto_create = false` — the
database must already exist, which is why step 1 comes first):

```toml
[database]
db_uri = "postgresql+psycopg2://admin:admin@localhost/biofilter_dev"
echo_sql = false
auto_create = false

[etl]
data_root = "./biofilter_data"
allow_parallel = true
max_workers = 8

[reports]
default_output_format = "dataframe"
warn_on_empty = true

[logging]
level = "INFO"
log_to_file = true
log_file = "./biofilter.log"
```

---

## 4. Create the schema and seed the database

`db create-db` is the command that actually builds the schema: it creates **all
tables** (including the partitioned variant tables) and loads the **seed data**
(source systems, data sources, entity groups, relationship types, genome
assemblies, …).

> `--overwrite` is required because the database already exists (we created it in
> step 1). It only bypasses the "already exists" guard — it is **not** destructive:
> table creation uses `create_all`, which is idempotent and never drops data.

```bash
biofilter db create-db --db-uri "postgresql+psycopg2://admin:admin@localhost/biofilter_dev" --overwrite
# [INFO] 📦 Bootstrapping models...
# [INFO] 🏗️  Creating tables...
# [INFO] ✅ Ensured variant_masters partitions
# [INFO] ✅ Tables created successfully (PostgreSQL).
# [INFO] 🌱 Seeding initial data...
# [INFO] Seeded: ETLSourceSystem            | applied=15 created=15 updated=0 skipped=0
# [INFO] Seeded: ETLDataSource              | applied=69 created=69 updated=0 skipped=0
# [INFO] Seeded: EntityGroup                | applied=14 created=14 updated=0 skipped=0
# [INFO] Seeded: EntityRelationshipType     | applied=9  created=9  updated=0 skipped=0
# [INFO] Seeded: GenomeAssembly             | applied=49 created=49 updated=0 skipped=0
# [INFO] ✅ Database created at postgresql+psycopg2://admin:admin@localhost/biofilter_dev
```

`created=N` on every seed table confirms a fresh build. (Re-running the command
later would show `created=0 updated=N` — the seeds are idempotent upserts.)

---

## 5. Inspect ETL status (nothing loaded yet)

`etl status` lists every registered data source and whether it's loaded. On a
fresh database all sources show `status=never`.

```bash
biofilter etl status
#                Domain  active source_system            data_source data_version status last_execution
#              Chemical    True           EBI                  chebi        1.1.0  never           None
#                  Gene    True          HGNC                   hgnc        1.1.0  never           None
#                  Gene    True       Ensembl                ensembl        1.1.0  never           None
#               Variant    True        gnomAD            gnomad_chr1        4.1.0  never           None
#               ... (69 sources total; gnomAD per-chromosome and dbSNP shards listed individually)
```

`active=False` rows (e.g. `omim`, the `dbsnp_*` shards) are registered but skipped
by `etl update-all`; load them explicitly only if you need them.

---

## 6. Run your first ETL (HGNC)

Start with `hgnc` — it's small, fast, and has no upstream dependencies. The
pipeline runs `extract → transform → load`.

```bash
biofilter etl update --data-source hgnc
# [INFO] 🔁 Starting ETL for 'hgnc' (source_system_id=2, data_source_id=2)
# [INFO] ⬇️  Fetching JSON from API: https://rest.genenames.org/fetch/all ...
# [INFO] ✅ HGNC file downloaded to ./biofilter_data/raw/HGNC/hgnc/hgnc_data.json
# [INFO] ✅ [Extract] Completed for 'hgnc' (hash=7823a4b3...)
# [INFO] ⚙️  [Transform] Running for 'hgnc'
# [INFO] ✅ HGNC data transformed and saved at biofilter_data/processed/HGNC/hgnc/master_data
# [INFO] 🚚 [Load] Running for 'hgnc'
# [INFO] 📥 Loading hgnc data into the database...
# [INFO] ✅ [Load] Completed for 'hgnc'
# [INFO] 🎉 ETL pipeline finished for 'hgnc'
```

Raw downloads land under `./biofilter_data/raw/` and processed parquet under
`./biofilter_data/processed/`, as configured by `etl.data_root`.

---

## 7. Run a report

With HGNC loaded, genes resolve by symbol or HGNC ID. Run the
`annotation_master_gene` report against a few inputs:

```bash
biofilter report run --report-name annotation_master_gene \
  --input BRCA1 --input TP53 --input HGNC:11998
# [INFO] Report 'annotation_master_gene' completed in 0.01 minutes (0.40 seconds).
# input_value  entity_id gene_symbol    hgnc_id      ensembl_id entrez_id hgnc_status ... status   note
#      BRCA1        22854       BRCA1  HGNC:1100 ENSG00000012048       672    Approved ... partial  No EntityLocation build=38 found for this gene.
#       TP53        37311        TP53 HGNC:11998 ENSG00000141510      7157    Approved ... partial  No EntityLocation build=38 found for this gene.
#  HGNC:11998       37311        TP53 HGNC:11998 ENSG00000141510      7157    Approved ... partial  ...
```

The report resolves all three inputs to entities and returns HGNC/Ensembl/Entrez
identifiers and gene groups.

> **Why `status=partial`?** Only HGNC has been loaded so far. Genomic coordinates
> (`chromosome`, `start_position`, …) and cross-source relationships come from
> other DTPs (`gene_ensembl`, `gene_ncbi`, variant sources, …). The
> `No EntityLocation build=38 found` note is expected at this stage. Load more
> sources to fill those columns.

or a modeling:

```bash
biofilter report run \
  --report-name variant_modeling \
  --input :11796321:G:A \
  --input rs699 \
  --input rs2228671 \
  --input rs12740374 \
  --input rs17464857 \
  --param group_entity_groups=Pathway,GO,Disease \
  --output chr1_variant_pairs_rich.csv
```

---

## What's next

- Load more sources individually (`biofilter etl update --data-source ensembl`)
  or everything active at once (`biofilter etl update-all --only-active`).
- Re-run `etl status` to watch `status` flip from `never` to a loaded state.
- Browse the [report catalog](../report_catalog.md) for the full set of reports.
- For deeper database operations (backup/restore, migrations), see
  [Database](../database.md).
