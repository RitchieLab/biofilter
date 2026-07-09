# BF4 User Documentation



<!-- ===== SOURCE FILE: docs/source/cli_reference.md ===== -->

# CLI Reference

## Global

```bash
biofilter [--db-uri URI] [--debug] COMMAND ...
```

Groups:

- `config`
- `db`
- `etl`
- `report`

## Config

- `biofilter config show`
- `biofilter config get SECTION.KEY`
- `biofilter config set SECTION.KEY VALUE`
- `biofilter config init --path .`

## DB

- `biofilter db ping`
- `biofilter db create-db`
- `biofilter db migrate`
- `biofilter db upgrade`
- `biofilter db backup`
- `biofilter db restore`
- `biofilter db export`
- `biofilter db import`

## ETL

- `biofilter etl update`
- `biofilter etl update-all`
- `biofilter etl explain`
- `biofilter etl status`
- `biofilter etl restart`
- `biofilter etl rollback`
- `biofilter etl index`

## Report

- `biofilter report list`
- `biofilter report explain --report-name <name>`
- `biofilter report example-input --report-name <name>`
- `biofilter report available-columns --report-name <name>`
- `biofilter report run --report-name <name> [options]`

Key `report run` options:

- `--input`, `--input-file`, `--input-column`
- `--param`, `--params-json`, `--params-file`
- `--params-template`
- `--output`



<!-- ===== SOURCE FILE: docs/source/configuration.md ===== -->

# Configuration

Biofilter resolves settings from:
1. command-line options (highest priority)
2. environment variables (`DATABASE_URL` or `BIOFILTER_DB_URI`)
3. `.biofilter.toml`
4. internal defaults

## Common Commands

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
biofilter config set database.db_uri "sqlite:///biofilter_dev.db"
```

Initialize template:

```bash
biofilter config init --path .
```

## Typical Keys

- `database.db_uri`
- `etl.data_root`

## Tips

- Prefer `--db-uri` in CI or one-off commands.
- Prefer `DATABASE_URL` in containers and orchestrators.
- Prefer `.biofilter.toml` for local development defaults.



<!-- ===== SOURCE FILE: docs/source/database.md ===== -->

# Database Operations

## Core Commands

Create DB:

```bash
biofilter db create-db --db-uri "sqlite:///biofilter_dev.db"
```

Check DB:

```bash
biofilter db ping --db-uri "sqlite:///biofilter_dev.db"
```

Migrate schema:

```bash
biofilter db migrate --target head
biofilter db migrate --status
```

Upgrade schema + master seeds:

```bash
biofilter db upgrade
```

Backup / restore:

```bash
biofilter db backup --out ./backups/dev.snapshot
biofilter db restore --in ./backups/dev.snapshot
```

Export / import logical bundle:

```bash
biofilter db export --out ./exports/biofilter_bundle --format parquet
biofilter db import --in ./exports/biofilter_bundle --format parquet
```

## Recommended Flow

```bash
biofilter db migrate --target head
biofilter db upgrade
biofilter db migrate --status
```



<!-- ===== SOURCE FILE: docs/source/entity_and_omics.md ===== -->

# Entity Model and Omics Domains

## Why `Entity` Exists

Biofilter 4 uses an entity-centric model so different biological domains can share identity and relationships.

Instead of keeping each source isolated, BF4 stores a common entity layer and links domain records to it. This enables cross-domain queries and reusable knowledge.

## Core Entity Objects

At the center of the schema:

- `EntityGroup`
  - semantic type bucket (for example: Variants, Genes, Proteins, Diseases)
- `Entity`
  - persistent concept record with activity/conflict flags and ETL provenance
- `EntityAlias`
  - names/codes/synonyms from multiple systems (`alias_type`, `xref_source`)
- `EntityRelationshipType`
  - relationship semantics (typed edge meaning)
- `EntityRelationship`
  - directed link between two entities with provenance

Practical effect:

- you can resolve aliases from many sources to one entity identity
- you can traverse relationships across domains without hardcoded paths

## Domain-Specific Master Data

The entity core is complemented by domain tables (master data), such as:

- genes (`GeneMaster` and gene-related tables)
- variants (variant master/effects/GWAS tables)
- proteins (`ProteinMaster`, Pfam links)
- pathways (`PathwayMaster`)
- gene ontology (`GOMaster`, `GORelation`)
- diseases (`DiseaseMaster`)
- chemicals (`ChemicalMaster`)

These domain tables provide rich attributes, while entities/aliases/relationships provide integration.

## Omics Domains in BF4

### Operational Domains (current)

Domains with active schema + ETL/report usage today:

- Variants
- Genes
- Proteins
- Pathways
- Gene Ontology
- Diseases
- Chemicals

These groups define semantic space and allow gradual expansion without redesigning the core model.

## How This Appears in ETL and Reports

- ETL loads source-specific master/relationship data and writes provenance (`ETLPackage`).
- Reports such as `entity_filter` and `entity_relationship_model` operate directly on this entity layer.
- Because identities are persistent, updates can be incremental and still query-consistent across domains.



<!-- ===== SOURCE FILE: docs/source/etl.md ===== -->

# ETL Operations

ETL is how Biofilter ingests, normalizes, and versions knowledge from external sources.

## Main Commands

Update selected sources:

```bash
biofilter etl update --data-source hgnc
```

Resumable batch update:

```bash
biofilter etl update-all
biofilter etl update-all --source-system NCBI
biofilter etl update-all --drop-files
```

Status overview:

```bash
biofilter etl status
biofilter etl status --source-system NCBI --only-active
```

Explain a DTP process:

```bash
biofilter etl explain --data-source hgnc
biofilter etl explain --dtp-script dtp_gene_hgnc
```

Restart with rollback + rerun:

```bash
biofilter etl restart --data-source gnomad_chr22
```

Rollback only:

```bash
biofilter etl rollback --package-id 123
biofilter etl rollback --data-source gnomad_chr22 --delete-files
```

## Monitoring Pair

- `biofilter etl status` for quick operational view.
- `biofilter report run --report-name etl_packages` for detailed audit.

## File Lifecycle (Raw and Processed)

By default, BF4 uses:

- download path: `./downloads`
- processed path: `./processed`

For each data source, ETL stages typically use:

- raw files: `<download_path>/<source_system>/<data_source>/...`
- processed outputs: `<processed_path>/<source_system>/<data_source>/...`

You will commonly see parquet files in the processed stage (e.g., `master_data.parquet`, relationship datasets).

`etl update-all --drop-files` can remove raw/processed directories after successful load for each data source.

## ETL Package Tracking

Each ETL run writes package metadata into the database, including:

- operation type (`extract`, `transform`, `load`, `rollback`)
- step status and timestamps
- hash linkage to support skip/up-to-date behavior
- error messages in package stats when failures happen

This is the foundation for resumable updates and for ETL audit reports.



<!-- ===== SOURCE FILE: docs/source/getting_started/connecting_db.md ===== -->

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

## Next step

Now that you can talk to a database, [find a report](finding_reports.md) and [run it](running_reports.md).



<!-- ===== SOURCE FILE: docs/source/getting_started/finding_reports.md ===== -->

# Finding a Report

BF4 ships with a growing set of reports — entity lookups, neighborhood summaries, variant annotations, ETL status, and more. Three ways to find the one that fits your need.

## 1. Browse the catalog

The [Report Catalog](../report_catalog.md) is the canonical index. It groups reports by purpose (ETL monitoring, entity exploration, variant analysis, modeling) and gives you, for each one:

- A one-line description of what it does.
- A link to its **Explain Guide** (parameters, output columns, examples).
- A link to a **Notebook tutorial** that runs end-to-end.

Use the catalog when you want to scan everything available.

## 2. Ask the GPT assistant

For natural-language questions like _"I have a list of genes from a GWAS — which report should I run to see what pathways they touch?"_, BF4 ships with a GPT assistant kit in the `assistent/` folder of the repository. It contains:

- A system prompt tuned for BF4 terminology.
- A FAQ.
- A manifest of all reports with their inputs, outputs, and use cases.

Link to GPT BF4 Assistent: [BF4 Assistent](https://chatgpt.com/g/g-6887cf80355c8191ab3f88bbd8955e0d-biofilter-4-assistant)

## 3. Use the CLI to introspect

If you already have BF4 installed and just want a quick list:

```bash
biofilter report list
```

For details on a specific report:

```bash
biofilter report explain --report-name entity_filter
```

This prints the full Explain Guide directly in your terminal, including parameters and example invocations.

## Common starting points

If you're new and not sure where to start, these reports are good entry points:

| Report                        | Use it when                                                           |
| ----------------------------- | --------------------------------------------------------------------- |
| `etl_status`                  | You want to see what data is loaded in the database                   |
| `entity_filter`               | You have a list of names and want to check which exist in BF4         |
| `entity_neighborhood_summary` | You have an entity and want to see everything connected to it (1-hop) |
| `annotation_master_gene`      | You want to browse the full gene catalog                              |

## Next step

Picked one? [Run your first report](running_reports.md).



<!-- ===== SOURCE FILE: docs/source/getting_started/index.md ===== -->

# Getting Started

Biofilter 4 (BF4) is a biological knowledge platform that resolves entities (genes, proteins, pathways, diseases, variants), tracks their relationships, and exposes them through ready-to-use reports.

This section walks you through your first run end-to-end. Pick the path that matches your situation and follow it in order.

## Choose your path

### I just want to run reports against a database that already exists

You have access to a Biofilter database, you don't need to do any data ingestion yourself.

1. [Install Biofilter](installing.md) — pick **pip** (recommended) or **Docker**.
2. [Connect to the database](connecting_db.md) — read **Option A: connect to an existing database**.
3. [Find a report that fits your need](finding_reports.md) — the catalog and the GPT assistant help here.
4. [Run your first report](running_reports.md) — CLI and Python API examples.

### I'm setting up my own Biofilter from scratch

You want a local database (SQLite for testing or PostgreSQL for production), populated by running the ETL yourself.

1. [Install Biofilter](installing.md) — pick **pip** or **source** if you'll contribute back.
2. [Connect to the database](connecting_db.md) — read **Option B: bootstrap a new database**.
3. Run the ETL pipeline (covered in **Option B** of the same page).
4. [Run your first report](running_reports.md) once the ETL completes.

## What you'll need

- **Python 3.10+** for pip-based installation, or **Docker** if you prefer containers.
- A **database connection string** if you're connecting to an existing instance — get this from whoever administrates it.
- Roughly **1 TB of disk space** if you're bootstrapping your own local DB with the full data.

## Where this guide stops

This Getting Started track is intentionally minimal. Once you can run a report, the rest of the documentation goes deeper:

- [Report catalog](../report_catalog.md) — every available report with descriptions and tutorials.
- [Configuration](../configuration.md) — full options for `.biofilter.toml`.
- [Database](../database.md) — schema, migrations, backup/restore.
- [ETL](../etl.md) — managing data sources, ETL packages, and rollbacks.
- [System overview](../system_overview.md) — architecture and design rationale.
- [Troubleshooting](../troubleshooting.md) — common errors and fixes.



<!-- ===== SOURCE FILE: docs/source/getting_started/installing.md ===== -->

# Installing Biofilter

Three installation methods, in order of simplicity. Pick **one**.

## Which one should I use?

| Method     | Best for                                           | Requires                |
| ---------- | -------------------------------------------------- | ----------------------- |
| **pip**    | Most users — running reports, notebooks, scripting | Python 3.10+            |
| **Docker** | Avoiding any Python setup, reproducible CI runs    | Docker                  |
| **Source** | Contributors, debugging, modifying BF4 itself      | Python 3.10+ and Poetry |

## pip (recommended)

```bash
pip install biofilter
biofilter --help
```

That's it — `biofilter` is now available as a CLI command and the `biofilter` Python package is importable.

To verify:

```bash
biofilter --help
python -c "from biofilter import Biofilter; print('OK')"
```

## Docker

Build the application-only image:

```bash
docker build -t biofilter:bf4 -f docker/Dockerfile "https://github.com/RitchieLab/biofilter.git#biofilter3r"
```

Run any Biofilter command inside the container, passing the database URL via environment variable:

```bash
docker run --rm -it \
  -e DATABASE_URL="postgresql+psycopg2://user:pass@host:5432/biofilter_dev" \
  -v "$(pwd):/workspace" \
  --entrypoint /bin/bash \
  biofilter:bf4
```

To save report outputs to your local filesystem, mount a volume:

```bash
docker run --rm \
  -e DATABASE_URL="postgresql+psycopg2://user:pass@host:5432/biofilter_dev" \
  -v "$(pwd)/outputs:/workspace/outputs" \
  biofilter:bf4 \
  biofilter report run --report-name etl_status --output /workspace/outputs/etl_status.csv
```

## From source

For contributors or anyone modifying BF4 itself.

```bash
git clone https://github.com/RitchieLab/biofilter.git
cd biofilter
poetry install
poetry run biofilter --help
```

## Next step

Once installed, [connect to a database](connecting_db.md) — either an existing instance or a fresh local one.



<!-- ===== SOURCE FILE: docs/source/getting_started/running_reports.md ===== -->

# Running Your First Report

Two ways to run any report: from the command line (CLI) or from Python (notebook or script). Both produce the same output. Pick whichever fits your workflow.

## CLI — quickest path

List what's available:

```bash
biofilter report list
```

Run a report and print the result to the terminal:

```bash
biofilter report run --report-name etl_status
```

Save the output to a CSV file:

```bash
biofilter report run --report-name etl_status --output etl_status.csv
```

Pass parameters with `--param KEY=VALUE`:

```bash
biofilter report run \
  --report-name entity_filter \
  --input "BRCA1" \
  --input "TP53" \
  --param match_mode=exact
```

For input lists too long for the command line, use `--input-file`:

```bash
biofilter report run \
  --report-name entity_filter \
  --input-file ./genes.txt
```

To see what parameters a report accepts:

```bash
biofilter report explain --report-name entity_filter
```

## Python API — best for notebooks and scripts

```python
from biofilter import Biofilter

bf = Biofilter()  # picks up DB from .biofilter.toml or DATABASE_URL

df = bf.report.run(
    "entity_filter",
    input_data=["BRCA1", "TP53", "APOE"],
    match_mode="exact",
)

print(f"{len(df)} rows")
df.head()
```

Every report returns a pandas `DataFrame`, so you can chain it with the rest of your analysis without saving to disk first.

## A complete first example

Here's a full session — install, connect, run:

```bash
# Install
pip install biofilter

# Configure
biofilter config init --path .
biofilter config set database.db_uri "postgresql+psycopg2://user:password@db.example.com:5432/database_name"

# Run
biofilter report list
biofilter report run --report-name etl_status --output etl_status.csv
```

Open `etl_status.csv` in your favorite tool and you'll see the current state of every data source in the database.

## Next steps

- Browse the [Report Catalog](../report_catalog.md) for what else you can do.
- Each report has a notebook tutorial in [`notebooks/Templates/`](https://github.com/RitchieLab/biofilter/tree/biofilter3r/notebooks/Templates) — copy one and adapt it.
- For deeper CLI options, see the [CLI Reference](../cli_reference.md).
- For Python API patterns, see [Reports](../reports.md).



<!-- ===== SOURCE FILE: docs/source/index.md ===== -->

# Biofilter Documentation

Lightweight, user-focused documentation for running Biofilter today.

This documentation is intentionally practical:
- install/configure quickly (PyPI, source, or Docker)
- bootstrap database and run ETL
- run reports via CLI/API
- troubleshoot common operational issues

```{toctree}
:maxdepth: 2
:caption: Getting Started

getting_started/index
getting_started/installing
getting_started/connecting_db
getting_started/finding_reports
getting_started/running_reports
```

```{toctree}
:maxdepth: 2
:caption: Reference

system_overview
entity_and_omics
developer_extensions
configuration
database
schema
etl
reports
report_catalog
cli_reference
troubleshooting
```



<!-- ===== SOURCE FILE: docs/source/report_catalog.md ===== -->

# Report Catalog

Complete index of all reports available in Biofilter 4.
Each report has a **name** (used in CLI and Python API), a brief description,
and links to its explain guide and interactive notebook tutorial where available.

For general usage — how to run, list, and introspect reports — see [Reports](reports.md).

---

## Running any report

```bash
# CLI
biofilter report run --report-name <name> [--param KEY=VALUE ...] [--output file.csv]
biofilter report explain --report-name <name>
biofilter report run --report-name <name> --params-template
```

```python
# Python API
df = bf.report.run("<name>", param1=value1, param2=value2)
```

---

## ETL & Platform Monitoring

Reports for inspecting the state of the ETL pipeline and the knowledge base.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `etl_status` | Current status of all ETL packages (active, last run, row counts) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_etl_status.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__etl_status.ipynb) |
| `etl_packages` | Full provenance log of all ETL executions with timestamps and file hashes | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_etl_packages.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__etl_packages.ipynb) |
| `platform_data_statistics` | Row counts and coverage metrics across all master tables | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_platform_data_statistics.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__platform_data_statistics.ipynb) |
| `db_pg_table_stats` | PostgreSQL table sizes, row estimates, and bloat metrics *(PostgreSQL only)* | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_db_pg_table_stats.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__db_pg_table_stats.ipynb) |
| `db_pg_index_stats` | PostgreSQL index usage, size, and scan counts *(PostgreSQL only)* | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_db_pg_index_stats.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__db_pg_index_stats.ipynb) |

---

## Entity & Relationship

Reports for exploring the biological entity graph.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `entity_filter` | Filter and list entities (genes, pathways, diseases, …) by type, source, or name pattern | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_entity_filter.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__entity_filter.ipynb) |
| `entity_relationship_model` | Retrieve all entities related to an input list through shared biological groups (pathways, diseases, GO, PPI) | — | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__entity_relationship_model.ipynb) |
| `entity_neighborhood_summary` | Resolve heterogeneous inputs (gene:, disease:, pathway:, …) and return a 1-hop neighborhood summary grouped by neighbor type | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_entity_neighborhood_summary.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__entity_neighborhood_summary.ipynb) |

---

## Annotation Masters

Reference tables exposing the full content of each biological domain in the knowledge base.
Useful for exploring available terms before using them as filters in other reports.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `annotation_master_gene` | All genes with HGNC symbol, Ensembl ID, locus, and source provenance | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_master_gene.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotations_master_gene.ipynb) |
| `annotation_master_pathway` | All pathways across all source systems (Reactome, KEGG, …) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_master_pathway.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotations_master_pathway.ipynb) |
| `annotation_master_protein` | All proteins with UniProt IDs and gene mappings | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_master_protein.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotations_master_protein.ipynb) |
| `annotation_master_disease` | All diseases with MONDO/ClinGen IDs and gene associations | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_master_disease.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotations_master_disease.ipynb) |
| `annotation_master_go` | All Gene Ontology terms (BP, MF, CC) with gene memberships | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_master_go.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotations_master_go.ipynb) |
| `annotation_master_chemical` | All chemical compounds (ChEBI) with gene and pathway associations | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_master_chemical.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotations_master_chemical.ipynb) |
| `annotation_master_variant` | Full annotation for input variants: frequencies, pathogenicity scores, VEP consequences per transcript, AlphaMissense | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_master_variant.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotations_master_variant.ipynb) |

---

## Variant Analysis

Reports for annotating and filtering genomic variants.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `variant_binning` | Assign variants to genomic bins; useful for burden-test preparation | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_binning.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__variant_binning.ipynb) |
| `variant_gene_location_model` | Map variants to overlapping gene loci with distance and region annotations | — | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__variant_gene_location_model.ipynb) |
| `variant_annotation_expanded` | Full annotation expansion for a variant list (consequence, AF, predictions) | — | — |
| `variant_single_gene_annotation` | **Phase 1** — Given a seed variant, returns the seed gene and all partner genes sharing biological context | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_single_gene_annotation.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__variant_single_gene_annotation.ipynb) |
| `gene_to_variant_filtering` | **Phase 2** — Collect and filter variants across a gene list with SQL-level pathogenicity filters | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_gene_to_variant_filtering.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__gene_to_variant_filtering.ipynb) |
| `annotation_variant_regulatory_evidence` | Variant ↔ gene regulatory evidence (eQTL / sQTL). Accepts gene symbols, rsids, or chr:pos as input; returns one row per (variant × tissue × regulated gene) with effect size and p-value | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_variant_regulatory_evidence.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__annotation_variant_regulatory_evidence.ipynb) |

---

## Variant Interaction Modeling

Direct variant-to-variant interaction modeling from a pre-genotyped input list.
Both variants in every pair come from the input — no DB expansion.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `variant_modeling` | Input variants → gene overlap → group co-membership → Variant×Variant pairs with group_support_count weight | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_modeling.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__variant_modeling.ipynb) |

---

## SNP×SNP Interaction Pipeline

Reports implementing the biologically-informed SNP×SNP interaction workflow.
See the full pipeline tutorial and methods document for end-to-end guidance.

| Resource | Link |
|---|---|
| Pipeline notebook | [pipeline__from_single_variant_to_interactions.ipynb](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb) |
| Pipeline methods doc | [pipeline__from_single_variant_to_interactions.md](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/pipeline__from_single_variant_to_interactions.md) |

| Report | Phase | Description | Explain | Notebook |
|---|---|---|---|---|
| `variant_single_gene_annotation` | Phase 1 | Seed variant → partner gene list via biological network | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_single_gene_annotation.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__variant_single_gene_annotation.ipynb) |
| `gene_to_variant_filtering` | Phase 2 | Gene list → filtered, annotated variant set (Lista A) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_gene_to_variant_filtering.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__gene_to_variant_filtering.ipynb) |
| `variant_list_intersect` | Phase 2.5 | Lista A ∩ Lista B → Lista C (genotyped subset, PLINK-ready) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_list_intersect.md) | [Pipeline notebook](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb) |
| `snp_snp_pair_generator` | Phase 3 | Lista D → annotated interaction pairs with configurable pairing strategy | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_snp_snp_pair_generator.md) | [Pipeline notebook](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb) |
| `snp_snp_model` | Legacy | Earlier SNP×SNP pair model — expands variants from gene loci (superseded by `variant_modeling`) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_snp_snp_model.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__snp_snp_model.ipynb) |

---

## Pathway Burden Pipeline

Pipeline for prioritising pathways given a list of significant genes (e.g., ExWAS hits) and a target pathway list, using cross-source convergence scoring.

| Resource | Link |
|---|---|
| Pipeline notebook | [pipeline__pathway_burden_score.ipynb](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/pipeline__pathway_burden_score.ipynb) |
| Pipeline methods doc | [pipeline__pathway_burden_score.md](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/pipeline__pathway_burden_score.md) |

---

## Utilities

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `template` | Blank report template for development and testing | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_template.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/notebooks/Templates/reports__qry_template.ipynb) |

---

## Coverage summary

| Status | Count |
|---|---|
| Reports with explain guide + notebook | 20 |
| Reports with explain guide only | 2 (`variant_list_intersect`, `snp_snp_pair_generator` — covered by pipeline notebook) |
| Reports with notebook only | 2 (`entity_relationship_model`, `variant_gene_location_model`) |
| Reports with neither | 1 (`variant_annotation_expanded`) |
| **Total** | **25** |



<!-- ===== SOURCE FILE: docs/source/reports.md ===== -->

# Reports

Reports are the main read interface over Biofilter knowledge and ETL provenance.

For the complete index of all available reports with links to explain guides and notebook tutorials, see the **[Report Catalog](report_catalog.md)**.

## Discover and Inspect

List reports:

```bash
biofilter report list
biofilter report list --verbose
```

Explain report:

```bash
biofilter report explain --report-name etl_status
```

Show example input:

```bash
biofilter report example-input --report-name entity_relationship_model
```

Show output columns:

```bash
biofilter report available-columns --report-name etl_packages
```

## Run Reports

Basic run:

```bash
biofilter report run --report-name etl_status
```

Export CSV:

```bash
biofilter report run --report-name etl_packages --output ./etl_packages.csv
```

Template-driven params:

```bash
biofilter report run --report-name entity_relationship_model --params-template
```

## Dynamic Parameter Injection

Inputs:

```bash
biofilter report run --report-name entity_filter --input BRCA1 --input TP53
biofilter report run --report-name entity_filter --input-file ./entities.csv --input-column symbol
```

Options:

```bash
biofilter report run --report-name entity_relationship_model \
  --input TP53 --input BRCA1 \
  --param relationship_scope=input_to_any \
  --param deduplicate_pairs=true
```

JSON/YAML params:

```bash
biofilter report run --report-name entity_relationship_model --params-json '{"relationship_scope":"input_to_any"}'
biofilter report run --report-name entity_relationship_model --params-file ./params.yaml
```

Load one param from file:

```bash
biofilter report run --report-name entity_relationship_model --input TP53 --param relationship_types=@./relationship_types.txt
```

## Explain Guides

`report explain` prefers markdown guides stored in:

- `biofilter/modules/report/reports_explain/report_<module>.md`

If a guide file is missing, Biofilter falls back to the report class `explain()` method.

This model keeps report documentation maintainable:
- update the report module when behavior changes
- update the paired explain markdown for user-facing guidance

## Practical Examples

Repository-level example guides:

- `docs/reports/snp_snp_model.md`



<!-- ===== SOURCE FILE: docs/source/system_overview.md ===== -->

# System Overview

## What Is Biofilter 4 (BF4)?

Biofilter 4 is a persistent, entity-centric biological knowledge platform.

In practice, BF4 is designed to:
- ingest biological data sources through ETL
- normalize and store knowledge in a local or shared database
- expose this knowledge through CLI, Python API, SQL, and reports

The key idea is persistence: build once, reuse across many analyses.

## High-Level Architecture

BF4 has four practical layers:

1. Knowledge Storage (Database)
- relational schema for entities, aliases, relationships, and ETL metadata

2. ETL Orchestration
- `extract -> transform -> load` pipelines per data source
- package-level tracking and status history

3. Data Access and Report Layer
- generic report manager
- dynamic report execution with shared CLI/API contracts

4. User Interfaces
- CLI (`biofilter ...`)
- Python API (`bf = Biofilter(...)`)
- notebooks and SQL workflows

## Deployment Modes

BF4 supports two common modes:

- Local managed database (for development, isolated workflows)
- Shared database (team/centralized operations)
- Containerized app-only runtime with external database (portable execution)

Both modes use the same CLI/API patterns.

## ETL Data Lifecycle

For each data source, BF4 follows a staged lifecycle:

1. Extract
- source files are downloaded to a raw staging area

2. Transform
- raw files are normalized into curated intermediate outputs (typically parquet)

3. Load
- curated outputs are loaded into the database

Operationally, this enables:
- resumable updates
- selective rollback/restart
- optional cleanup of raw/processed files after successful loads

## Provenance and Reproducibility

Each ETL step execution is tracked via ETL packages, including:
- data source identity
- operation type (`extract`, `transform`, `load`, `rollback`)
- status and timestamps
- hash linkage across steps
- error notes/stats when failures occur

This metadata is used by:
- `biofilter etl status`
- `etl_status` and `etl_packages` reports

## Report Explain Guides

Report tutorials/explains are stored as markdown files in:

- `biofilter/modules/report/reports_explain/report_<module>.md`

`biofilter report explain --report-name <name>` prefers these guides. If not found, BF4 falls back to the report class `explain()` method.

For a focused explanation of the entity-centric model and current omics domains, see [Entity Model and Omics Domains](entity_and_omics.md).



<!-- ===== SOURCE FILE: docs/source/troubleshooting.md ===== -->

# Troubleshooting

## Report Not Found

- Run `biofilter report list`.
- Use `--report-name` with one of the listed names.

## Input Conflict in `report run`

If you pass `--input`/`--input-file`, do not also pass input keys through params (`input_data`, `items`, `input_path`).

## Explain Page Not Found

- Check if guide exists at `biofilter/modules/report/reports_explain/report_<module>.md`.
- If missing, Biofilter will fall back to class `explain()`.

## PostgreSQL-only Reports

`db_pg_table_stats` and `db_pg_index_stats` require PostgreSQL.

## Migration/Upgrade Issues

Use:

```bash
biofilter db migrate --status
biofilter db migrate --target head
biofilter db upgrade
```

## ETL Batch Resume

If `etl update-all` was interrupted, run it again. Successful data sources are skipped.

## Report Output Not Found (Docker)

If you run BF4 in a container and export with `--output`, mount a host volume and write to that mounted path.

Example:

```bash
docker run --rm \
  -e DATABASE_URL="postgresql+psycopg2://user:pass@host:5432/db" \
  -v "$(pwd)/outputs:/workspace/outputs" \
  biofilter:bf4 \
  biofilter report run --report-name etl_status --output /workspace/outputs/etl_status.csv
```
