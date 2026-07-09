# BF4 Assistant Eval Set

Prompts to validate assistant quality after each context refresh. The audience
is end users, so evals emphasize **running reports** and **choosing a database
path (Parquet bundle vs building your own)**.

For each test, verify:

- command correctness
- argument correctness
- practical, user-facing guidance (no code-internals dumps)
- no invented reports, flags, or data sources

---

## Running reports

### Test 1
Prompt: "I have a list of genes. How do I annotate them and get a CSV?"
Expected:
- `biofilter report run --name annotation_master_gene --input "..."` or `--input-file`
- `--output <file>.csv`

### Test 2
Prompt: "How do I see which reports exist and what they do?"
Expected:
- `biofilter report list --verbose`

### Test 3
Prompt: "What inputs does annotation_master_variant accept?"
Expected:
- `biofilter report explain --name annotation_master_variant`
- mentions rsID / chr:pos / chr:pos:ref:alt

### Test 4
Prompt: "How do I read inputs from a CSV column called symbol?"
Expected:
- `--input-file <file>.csv --input-column symbol`

### Test 5
Prompt: "How do I run entity_relationship_model with relationship_scope=input_to_any?"
Expected:
- `--input ...`
- `--param relationship_scope=input_to_any`

### Test 6
Prompt: "I used a report name that does not exist. What now?"
Expected:
- `biofilter report list --verbose`, then use the exact `--name`

---

## Database path (Parquet bundle vs own DB)

### Test 7
Prompt: "I just want to run reports against a shared snapshot, I don't want to install a server."
Expected:
- set `BIOFILTER_DB_URI` (or `--db-uri`) to `parquet:///.../tables`
- note: read-only, no server, no ETL

### Test 8
Prompt: "How does BF4 decide which database to use?"
Expected:
- order: `--db-uri` → `BIOFILTER_DB_URI` / `DATABASE_URL` → `.biofilter.toml`

### Test 9
Prompt: "On the LPC, how do I get started?"
Expected:
- `source /project/hall_shared/hall_shared.sh`
- `module load biofilter/<version>`
- module sets the DB URI; then `biofilter report run ...`

### Test 10
Prompt: "How do I create a brand-new database from scratch?"
Expected:
- set a DB URI (sqlite/postgres)
- `db migrate --target head` → `db upgrade` → `db migrate --status`

### Test 11
Prompt: "How do I turn my database into a shareable Parquet bundle?"
Expected:
- `biofilter db export --out ./bundle --format parquet`
- produces `manifest.json` + `tables/`

---

## Updating data (ETL)

### Test 12
Prompt: "How do I update only HGNC?"
Expected:
- `biofilter etl update --data-source hgnc` (optional `--debug`)

### Test 13
Prompt: "How do I update all pending sources and remove files afterward?"
Expected:
- `biofilter etl update-all --drop-files`
- mention resumable behavior and that `--drop-files` is not a safe default

### Test 14
Prompt: "Difference between etl update and etl update-all?"
Expected:
- `update`: requires `--source-system` or `--data-source`
- `update-all`: resumable batch

### Test 15
Prompt: "How do I check ETL status for one source system?"
Expected:
- `biofilter etl status --source-system <name>` (optional `--only-active`)

---

## Recovery and diagnostics

### Test 16
Prompt: "How do I roll back package 123?"
Expected:
- `biofilter etl rollback --package-id 123`
- caution note + suggest backup first

### Test 17
Prompt: "Which version am I running and is my DB initialized?"
Expected:
- `biofilter --version`
- `biofilter db migrate --status` (and/or `db ping`)

---

## Out-of-scope handling

### Test 18
Prompt: "How is the DuckDB connection implemented in the code?"
Expected:
- assistant states this is implementation detail outside its knowledge base
- points to maintainer / repository instead of guessing

---

## Failure signals (reject answers)

- Invented commands, report names, flags, or data sources.
- Missing required flags where needed.
- Claims of execution success without execution evidence.
- Answering source-code/implementation questions as if grounded.
- Recommending destructive commands without a caution note.
- Contradictions with project docs / CLI.
