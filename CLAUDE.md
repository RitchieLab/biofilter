# CLAUDE.md — Biofilter 4

Context guide for Claude Code to work on the Biofilter 4 project.

---

## What is Biofilter 4

Biofilter 4 (BF4) is a persistent, entity-centric biological knowledge platform developed at the Ritchie Lab (Penn Medicine). It replaces transient file-based annotation workflows with a versioned, reusable, and queryable knowledge base.

**Author:** Andre Rico (`andreluis.rico@pennmedicine.upenn.edu`)  
**Current version:** 4.3.0  
**Active branch:** `release/4.3.0` (active development — APIs and schema still evolving)  
**Docs (Sphinx / Read the Docs):** https://biofilter.readthedocs.io/en/latest/

---

## Folder structure

```
biofilter/               # Main Python package
  api/cli/               # Click CLI (main.py, groups/)
  core/components/       # db_component, etl_component, report_component, settings_component
  modules/
    db/                  # SQLAlchemy ORM, JSON seeds
    etl/                 # ETLManager + 21 DTPs (Data Transformation Packages)
    report/              # ReportManager + 16 reusable reports
  utils/                 # config, logger, version helpers
  biofilter.py           # Main Python API facade

biofilter_agents/        # Operational guides (LLM-ready). ag_start + ag_report
                         # are for bundle readers; ag_db + ag_etl for maintainers
assistent/               # GPT assistant kit. The manifest is the single source
                         # of truth for its knowledge base; both sync paths read it
biofilter_data/          # downloads/, processed/, staging/, bundles/
docs/source/             # Sphinx docs, split by audience:
                         #   root + getting_started/ — the scientist
                         #   technical/ — building bundles, internals
notebooks/               # templates/ — one notebook per report
                         # lpc__quickstart.md, lpc__deploy.md — Penn LPC
adr/                     # Architecture decisions, not tied to a release
biofilter_legacy/
  bf4_420/               # Frozen 4.2.x: notebooks, scripts, docs snapshot
  bf2x_code/, loki_code/ # Archived v2/v3 code
tests/                   # unit/, integration/, contract tests
docker/                  # Dockerfile and container docs
temp/                    # Created during binning queries — disposable
```

---

## 4-layer architecture

### 1. Database layer (`modules/db/`)
- SQLAlchemy 2.x ORM, schema built with `create_all` (no migrations:
  a schema change produces a new bundle, not an in-place migration)
- Entity-centric model: `Entity`, `EntityAlias`, `EntityRelationship`, `EntityRelationshipType`, `EntityGroup`
- Domain master tables: `GeneMaster`, `VariantMaster`, `ProteinMaster`, `PathwayMaster`, `DiseaseMaster`, `ChemicalMaster`, `GOMaster`
- Provenance tracking via `ETLPackage`
- Throwaway SQLite during a build; read-only parquet bundle in
  production. PostgreSQL still works but is no longer the target.
- Seeds in JSON: `biofilter/modules/db/seed/`

### 2. ETL layer (`modules/etl/`)
- 21 DTPs: `hgnc`, `gene_ncbi`, `gene_ensembl`, `uniprot`, `uniprot_relationships`, `reactome`, `reactome_relationships`, `kegg`, `kegg_relationships`, `go`, `pfam`, `mondo`, `mondo_relationships`, `biogrid`, `clingen`, `chebi`, `gwas`, `variant_gnomad_joint`, `variant_gnomad_vep`, `variant_alphamissense`, `variant_eqtl_gtex`
- Pipeline: `extract → transform → load` with file-hash-based skip logic
- Raw files → `<data_root>/downloads/`, processed → `<data_root>/processed/` (parquet)
- `ETLManager` orchestrates execution, tracking, rollback, and resume

### 3. Report layer (`modules/report/`)
- 16 reports with dynamic parameters (no CLI changes needed when adding new reports)
- Each report has a paired: `report_*.py` + `reports_explain/report_*.md`
- `ReportManager` handles discovery, indexing, and routing

### 4. Interaction layer (`api/cli/` + `biofilter.py`)
- Click CLI with 5 command groups: `bundle`, `config`, `db`, `etl`, `report`
- Python facade: `Biofilter(bundle=...)` for a bundle, or `Biofilter(db_uri=...)` for any SQLAlchemy URI; exposes `.db`, `.etl`, `.report`, `.settings`
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
# 1) Plan what the bundle covers
biofilter bundle plan --out bundle_plan.json

# 2) Build it: staging SQLite, both branches, then assembly
biofilter bundle build --plan bundle_plan.json

# 3) Inspect the result
biofilter bundle info ./biofilter_data/bundles/<YYYYMMDD>
biofilter db verify --in ./biofilter_data/bundles/<YYYYMMDD> --schema

# 4) Monitoring reports
biofilter report run --report-name platform_etl_status
biofilter report run --report-name platform_etl_packages

# 5) Explore available reports
biofilter report list --verbose
biofilter report explain --report-name <name>
```

---

## Python API (notebook usage)

```python
from biofilter import Biofilter

# Reading: a bundle is a directory — point at the directory, not at its
# tables/. A bundle is read-only, so reports work and the ETL does not.
bf = Biofilter(bundle="./biofilter_data/bundles/20260914")

result = bf.report.run("annotate_gene", input_data=["TP53", "BRCA1"])
df = result.to_pandas()          # every report returns a ReportResult
result.provenance["bundle_id"]   # which build these ids belong to
result.write("genes.csv")        # export: one table, lossy on purpose
result.save("./runs/apoe")       # keep: every table, nothing lost

# Some reports answer in more than one shape.
stats = bf.report.run("platform_data_statistics")
stats.extra_tables["storage"]    # beside stats.table

# Writing: the ETL needs a writable URI, never a bundle.
bf_dev = Biofilter(db_uri="postgresql+psycopg2://user:pass@localhost:5432/biofilter_dev")
summary = bf_dev.etl.update_all(only_active=True)
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

A report is four artifacts, and all four ship together:

1. `biofilter/modules/report/reports/report_<name>.py` — `name`, `description`, `requires`, `run()`, `available_columns()`, `example_input()`
2. `biofilter/modules/report/reports_explain/report_<name>.md` — reference: parameters, columns, how to read the result
3. `notebooks/templates/reports__<name>.ipynb` — worked example; copy `reports__TEMPLATE.ipynb`
4. `tests/unit/report/test_report_<name>.py` — against the fixture bundle in `tests/unit/conftest.py`
5. Validate: `biofilter --bundle <path> report list` + `report explain --report-name <name>`

A report returns one table. When its answer is genuinely two shapes,
`self.emit("<name>", table)` attaches another instead of flattening them
into one or writing the second out as a file. When it copes with
something the reader should know about, `self.warn(msg, **context)` logs
it and records it in the result's provenance.

`result.save(dir)` / `ReportResult.load(dir)` round-trip the whole thing
— every table as parquet, provenance in a manifest, in a bundle's own
layout so `Bundle.open` reads a saved result and DuckDB queries it.
`result.write(path)` is the other job: exporting CSV or parquet for
something else to read, flattening nested columns, lossy on purpose.

Reports are discovered by being in the package — nothing to register.
The frozen `report_legacy` module is gone: every report was rewritten
against the bundle (ADR-004 §2.11), so there is no relational path and
no second place to look. Shared helpers live in modules that do not
start with `report_`, which is what keeps them from being discovered as
reports: `_annotation.py` (the annotate family's SQL), `_resolution.py`
(turning a typed name into entities), `_variants.py` (reading rsIDs and
positions) and `_cohort.py` (reading VCF / PLINK files).

---

## Safety and operational rules

- **Never run `rollback` or `restore` automatically** — require explicit confirmation
- **Before overwriting a bundle:** don't. A published bundle cannot be
  rebuilt once its sources move on; build a new one instead.
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

- **Production:** read-only Parquet bundle, built by `bundle build`, on the Penn LPC (`/project/hall_shared/datasets/biofilter/<YYYYMMDD>`), accessed via `--db-uri parquet:///...`. The VPS was decommissioned; its PostgreSQL deployment procedure is kept for reference in `biofilter_legacy/bf4_420/notebooks/Templates/lpc__deploy.md` (Appendix A).
- **Local dev:** PostgreSQL `biofilter_dev`. Note its entity IDs are a different ID space from the bundle — never export from it over the bundle.
- **Docker:** one image (`docker/Dockerfile`), published to Docker Hub and
  to GHCR as `biofilter-hpc`. It carries no data: bind the bundle at
  `/bundle` read-only and a writable `/workspace` for output.
- **Tooling:** Poetry, tox, pytest, sphinx, testcontainers (Postgres in tests)
- **Local config:** `.biofilter.toml` at project root — `[database] bundle` for
  reading (a directory, relative to the file), `db_uri` only for writing;
  bundle wins. Do not commit credentials.

---

## Known documentation gaps

- `docs/source/guides/` does not exist yet. It is the agreed home for
  task-shaped pages written as questions ("which variants in these genes
  are plausibly damaging"), as opposed to the current organization by
  system component. The model for one is
  `biofilter_legacy/bf4_420/notebooks/Andre/adsp/step_01/README.md`.
- Only the current release's notebooks live at the root (`notebooks/`),
  unversioned on purpose: the directory carried a `_430` suffix while the
  4.2.x set sat beside it, and that set has moved under
  `biofilter_legacy/`. Versioning the path again would mean renaming it,
  and every reference to it, each release.
  Everything from 4.2.x — notebooks, scripts and a snapshot of its docs —
  is frozen under `biofilter_legacy/bf4_420/`, so `docs/` can be rewritten
  for 4.3.0 without stranding anyone still reading a 4.2.0 bundle.

## Notes

