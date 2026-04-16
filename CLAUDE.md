# CLAUDE.md — Biofilter 4

Context guide for Claude Code to work on the Biofilter 4 project.

---

## What is Biofilter 4

Biofilter 4 (BF4) is a persistent, entity-centric biological knowledge platform developed at the Ritchie Lab (Penn Medicine). It replaces transient file-based annotation workflows with a versioned, reusable, and queryable knowledge base.

**Author:** Andre Rico (`andreluis.rico@pennmedicine.upenn.edu`)  
**Current version:** 4.1.2  
**Active branch:** `biofilter3r` (active development — APIs and schema still evolving)  
**Docs (Sphinx / Read the Docs):** https://biofilter.readthedocs.io/en/latest/

---

## Folder structure

```
biofilter/               # Main Python package
  api/cli/               # Click CLI (main.py, groups/)
  core/components/       # db_component, etl_component, report_component, settings_component
  modules/
    db/                  # SQLAlchemy ORM, Alembic migrations, JSON seeds
    etl/                 # ETLManager + 19 DTPs (Data Transformation Packages)
    report/              # ReportManager + 17 reusable reports
  utils/                 # config, logger, version helpers
  biofilter.py           # Main Python API facade

biofilter_agents/        # Operational guides (LLM-ready) for CLI/ETL/DB/Report
assistent/               # GPT assistant kit (system prompt, FAQ, manifest)
biofilter_data/          # Downloads and processed files before DB ingestion
biofilter_legacy/        # Archived legacy code (v2 and v3)
docs/source/             # Sphinx documentation source
notebooks/               # Tutorials and examples (Andre/, Templates/)
scripts/                 # Admin and debug scripts (target of launch.json configs)
tests/                   # unit/, integration/, contract tests
docker/                  # Dockerfile and container docs
temp/                    # Created during binning queries — disposable
```

---

## 4-layer architecture

### 1. Database layer (`modules/db/`)
- SQLAlchemy 2.x ORM + Alembic migrations
- Entity-centric model: `Entity`, `EntityAlias`, `EntityRelationship`, `EntityRelationshipType`, `EntityGroup`
- Domain master tables: `GeneMaster`, `VariantMaster`, `ProteinMaster`, `PathwayMaster`, `DiseaseMaster`, `ChemicalMaster`, `GOMaster`
- Provenance tracking via `ETLPackage`
- SQLite (dev) and PostgreSQL (production)
- Seeds in JSON: `biofilter/modules/db/seed/`

### 2. ETL layer (`modules/etl/`)
- 19 active DTPs: `hgnc`, `gene_ncbi`, `gene_ensembl`, `uniprot`, `uniprot_relationships`, `reactome`, `reactome_relationships`, `kegg`, `go`, `pfam`, `mondo`, `mondo_relationships`, `biogrid`, `clingen`, `chebi`, `gwas`, `variant_gnomad`, `variant_alphamissense`, `variant_ncbi`
- Pipeline: `extract → transform → load` with file-hash-based skip logic
- Raw files → `<data_root>/downloads/`, processed → `<data_root>/processed/` (parquet)
- `ETLManager` orchestrates execution, tracking, rollback, and resume

### 3. Report layer (`modules/report/`)
- 17 reports with dynamic parameters (no CLI changes needed when adding new reports)
- Each report has a paired: `report_*.py` + `reports_explain/report_*.md`
- `ReportManager` handles discovery, indexing, and routing

### 4. Interaction layer (`api/cli/` + `biofilter.py`)
- Click CLI with 4 command groups: `config`, `db`, `etl`, `report`
- Python facade: `Biofilter(db_uri=...)` exposing `.db`, `.etl`, `.report`, `.settings`
- Supports `DATABASE_URL` env var (Docker-ready)

---

## Development commands

```bash
# Install in editable mode (recommended for dev)
poetry install

# Run CLI
poetry run biofilter --help

# Run all tests
poetry run pytest

# Run by layer
poetry run python -m pytest -q tests/unit/
poetry run python -m pytest -q tests/integration/cli/test_cli_db_lifecycle.py
poetry run python -m pytest -q tests/integration/cli/test_cli_db_postgres_lifecycle.py -m postgres

# Coverage
coverage run --rcfile=.coveragerc-core -m pytest
coverage report --rcfile=.coveragerc-core -m

# Tox (multiple Python versions)
tox

# Build Sphinx docs
cd docs && make html
```

---

## Typical operational workflow

```bash
# 1) Validate config and database state
biofilter config show
biofilter db migrate --status

# 2) Bootstrap (new environment)
biofilter db migrate --target head --force
biofilter db upgrade

# 3) ETL
biofilter etl update-all
biofilter etl status

# 4) Monitoring reports
biofilter report run --report-name etl_status
biofilter report run --report-name etl_packages

# 5) Explore available reports
biofilter report list --verbose
biofilter report explain --report-name <name>
```

---

## Python API (notebook usage)

```python
from biofilter import Biofilter

bf = Biofilter(db_uri="postgresql+psycopg2://user:pass@localhost:5432/biofilter_dev")
bf.db.connect()

# ETL
summary = bf.etl.update_all(only_active=True)

# Reports
df_status = bf.report.run("etl_status", only_active=False)
df_rel    = bf.report.run("entity_relationship_model",
                           input_data=["TP53", "BRCA1"],
                           relationship_scope="input_to_any")
```

---

## Language conventions

- All project artifacts must be written in **English**: source code, comments, documentation, `.md` files, commit messages, report explain guides, DTP explain guides.
- Conversations with the developer may be in Portuguese or English.

---

## Code conventions

- **ORM over raw SQL:** prefer SQLAlchemy ORM logic; avoid manual SQL unless justified
- **Python 3.10+** — no backwards-compatibility shims
- **New DTPs:** create `biofilter/modules/etl/dtps/dtp_<name>.py` + doc at `dtps_explain/dtp_<name>.md`
- **New reports:** create `report_<name>.py` + `reports_explain/report_<name>.md`; the CLI discovers them automatically with no changes to support code
- **Report parameters via CLI:**
  - `--input` / `--input-file` for input data (`input_data`)
  - `--param KEY=VALUE` for options and filters
  - Do not mix input channels

---

## Extending: new DTP

1. `biofilter/modules/etl/dtps/dtp_<name>.py` — class with `extract()`, `transform()`, `load()`
2. `biofilter/modules/etl/dtps_explain/dtp_<name>.md` — source, behavior, caveats
3. Register in the seed or active DataSources list
4. Test with `biofilter etl update --data-source <name>`

## Extending: new Report

1. `biofilter/modules/report/reports/report_<name>.py` — `name`, `description`, `run()`, `available_columns()`, `example_input()`
2. `biofilter/modules/report/reports_explain/report_<name>.md` — tutorial, parameters, column descriptions
3. Validate: `biofilter report list --verbose` + `biofilter report explain --report-name <name>`

---

## Safety and operational rules

- **Never run `rollback` or `restore` automatically** — require explicit confirmation
- **Before destructive migrations:** create a backup (`biofilter db backup --out ...`)
- **`--drop-files` in ETL:** do not use by default in production
- **`--stamp-head`:** only in controlled environments with clear justification
- **Bundle `import`:** confirm target environment before executing

---

## Internal documentation (priority order)

1. `biofilter_agents/ag_start.md` — onboarding and first run
2. `biofilter_agents/ag_etl_en.md` — full ETL operations
3. `biofilter_agents/ag_db_en.md` — database operations
4. `biofilter_agents/ag_report_en.md` — report operations
5. `biofilter/api/cli/ag_01_commands.md` — full CLI command map
6. `biofilter/modules/etl/dtps_explain/` — per DTP
7. `biofilter/modules/report/reports_explain/` — per report
8. `docs/source/` — published Sphinx documentation

**Note:** `biofilter_legacy/` contains archived code (v2/v3) — not a reference for active development.

---

## Infrastructure

- **VPS server:** BF4 in production + PostgreSQL on the same server
- **Docker:** available to run the CLI without installing BF4 locally (`docker/Dockerfile`)
- **Tooling:** Poetry, tox, pytest, sphinx, testcontainers (Postgres in tests)
- **Local config:** `.biofilter.toml` at project root (do not commit credentials)

---

## Known documentation gaps

- Some `reports_explain/` files are minimal stubs — to be revisited
- Notebooks in `notebooks/` have no index — acceptable if used for personal development

## Notes

- `dtp_variant_ncbi.py` is kept as a backup DTP (replaced by gnomAD as primary variant source); no explain doc required for now
