# BF4 Support FAQ



<!-- ===== SOURCE FILE: assistent/assistant_faq_seed.md ===== -->

# BF4 FAQ Seed

High-signal answers to bootstrap end-user support. Organized by task:
run reports → connect to data → build/update data → recover → diagnose.

Report subcommands (`run`, `explain`, `available-columns`, `example-input`)
take `--name` (the older `--report-name` still works as an alias). Both
`BIOFILTER_DB_URI` and `DATABASE_URL` are read as the database URI.

---

## A) Running reports (most common)

### A1) I just want to run a report and get a CSV

```bash
biofilter report run \
  --name annotation_master_gene \
  --input APOE \
  --output apoe.csv
```

Result: `apoe.csv` in the current directory.

To pass several values, repeat `--input` (there is no comma-separated form):

```bash
biofilter report run --name annotation_master_gene \
  --input APOE --input TP53 --input BRCA1 --output genes.csv
```

For long lists, use `--input-file` (see A4).

### A2) What reports can I run?

```bash
biofilter report list
biofilter report list --verbose      # includes descriptions
```

Commonly used, user-facing reports:

| Report | Typical input |
|---|---|
| `annotation_master_gene` | Gene symbols (e.g. `APOE`) |
| `annotation_master_variant` | rsIDs (`rs429358`), `chr:pos`, or `chr:pos:ref:alt` |
| `annotation_master_disease` | Disease names or MONDO IDs |
| `annotation_master_pathway` | Pathway names or Reactome/KEGG IDs |
| `annotation_master_chemical` | Chemical names or ChEBI IDs |
| `annotation_master_protein` | UniProt accessions |
| `annotation_master_go` | GO terms |
| `variant_modeling` | rsIDs — builds SNP×SNP pairs via shared biological groups |

### A3) How do I know what a report accepts (inputs, params, columns)?

```bash
biofilter report explain --name annotation_master_variant
```

Shows accepted input formats, parameters, and output columns.

### A4) How do I pass many inputs from a file?

One item per line:

```bash
biofilter report run --name annotation_master_gene \
  --input-file genes.txt --output genes.csv
```

CSV file — pick the column:

```bash
biofilter report run --name annotation_master_gene \
  --input-file cohort.csv --input-column symbol --output genes.csv
```

### A5) How do I pass report options?

```bash
biofilter report run --name entity_relationship_model \
  --input TP53 --input BRCA1 \
  --param relationship_scope=input_to_any \
  --param deduplicate_pairs=true
```

Or from JSON / YAML:

```bash
biofilter report run --name entity_relationship_model \
  --params-json '{"relationship_scope":"input_to_any"}'
biofilter report run --name entity_relationship_model \
  --params-file ./params.yaml
```

Get a starter template of a report's parameters:

```bash
biofilter report run --name entity_relationship_model --params-template
```

### A6) I got "report not found"

List valid names, then use the exact one:

```bash
biofilter report list --verbose
```

Keep inputs in `--input`/`--input-file` — do not pass `input_data` via
`--param` (that raises an input-conflict error).

---

## B) Connecting to a database

### B1) How does BF4 know which database to use?

Resolution order:

1. `--db-uri "<uri>"` on the command
2. environment variable `BIOFILTER_DB_URI` or `DATABASE_URL`
3. `.biofilter.toml` (`database.db_uri`)

Check the active one:

```bash
biofilter config show
biofilter db migrate --status
```

### B2) I only want to run reports against a shared snapshot (no install)

Point at a Parquet bundle — read-only via DuckDB, no server, no ETL:

```bash
export BIOFILTER_DB_URI="parquet:///abs/path/to/bundle/tables"
biofilter report run --name annotation_master_gene --input APOE --output apoe.csv
```

Or per command:

```bash
biofilter --db-uri "parquet:///abs/path/to/bundle/tables" \
  report run --name annotation_master_gene --input APOE --output apoe.csv
```

`parquet://` cannot modify or delete data — it is safe for read-only querying
and safe for many concurrent users on shared storage.

### B3) On the LPC (shared cluster), how do I start?

```bash
source /project/hall_shared/hall_shared.sh
module load biofilter/4.2.0        # puts BF4 on PATH and sets the snapshot URI
biofilter --version                # expected: biofilter 4.2.0
biofilter report run --name annotation_master_gene --input APOE --output apoe.csv
```

The module sets `BIOFILTER_DB_URI` for you, so no `--db-uri` is needed.
See `notebooks/Templates/lpc__quickstart.md` for the full walkthrough.

---

## C) Installing / creating your own database

Use this path when you need to update data yourself or run write operations.
Supported backends: PostgreSQL (production) and SQLite (local/dev).

### C1) Bootstrap a new database from scratch

```bash
# 1) point at your target DB (example: local SQLite)
export BIOFILTER_DB_URI="sqlite:///./biofilter.db"

# 2) create schema and seed
biofilter db migrate --target head
biofilter db upgrade
biofilter db migrate --status      # confirm current revision == head
```

For PostgreSQL, use e.g.
`postgresql+psycopg2://user:pass@host:5432/biofilter` as the URI.

### C2) Populate it with data (ETL)

```bash
biofilter etl update-all           # resumable batch over pending sources
biofilter etl status               # monitor progress
```

### C3) Turn my database into a shareable Parquet bundle

```bash
biofilter db export --out ./bundle --format parquet
```

This writes `manifest.json` + `tables/`. Others can then read it directly:

```bash
biofilter --db-uri "parquet:///abs/path/bundle/tables" report list
```

Load a bundle into another PostgreSQL/SQLite database:

```bash
biofilter db import --in ./bundle --format parquet
```

---

## D) Updating the data (ETL)

### D1) Update one data source only

```bash
biofilter etl update --data-source hgnc
```

### D2) Update all pending sources (resumable)

```bash
biofilter etl update-all
```

### D3) Update only one source system

```bash
biofilter etl update-all --source-system NCBI
```

### D4) "No source_system or data_sources provided"

`etl update` needs a target: `--source-system` or `--data-source`. To run a
resumable batch without explicit targets, use `biofilter etl update-all`.

### D5) Check ETL status

```bash
biofilter etl status
biofilter etl status --source-system NCBI --only-active
biofilter etl status --data-source hgnc
```

### D6) Clean up raw/processed files after a successful update

```bash
biofilter etl update-all --drop-files      # remove files
biofilter etl update-all --keep-files      # preserve files (safer default)
```

`--drop-files` is not recommended by default in production.

---

## E) Recovery (risky — confirm first)

Back up before anything destructive:

```bash
biofilter db backup --out ./backups/biofilter_backup
```

### E1) Roll back a specific ETL package

```bash
biofilter etl rollback --package-id 123
```

### E2) Roll back / restart one data source

```bash
biofilter etl rollback --data-source gnomad_chr22 --delete-files
biofilter etl restart  --data-source gnomad_chr22 --delete-files
```

`--delete-files` removes downloaded/processed files — use with care.

---

## F) Diagnosing problems

### F1) Which version am I running?

```bash
biofilter --version
```

### F2) Is my database reachable / initialized?

```bash
biofilter db ping                  # connectivity only
biofilter db migrate --status      # schema revision vs head
```

### F3) Turn on debug logging

```bash
biofilter report run --name etl_status --debug
biofilter etl update --data-source hgnc --debug
```

### F4) Where do report explain guides live?

```
biofilter/modules/report/reports_explain/report_<name>.md
```

Or just run `biofilter report explain --name <name>`.
