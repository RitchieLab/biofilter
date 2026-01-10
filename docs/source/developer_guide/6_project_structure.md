# Project Structure

This section describes the development tooling, repository structure, and
operational artifacts used in the Biofilter 4 codebase.
It is intended for contributors and developers who need to understand how the
project is organized, built, tested, and maintained.

---

## Project layout overview

At the top level, the Biofilter repository is organized to clearly separate:

- core application logic,
- data management and ETL,
- developer tooling,
- documentation and notebooks,
- test and release artifacts.

A simplified overview:

```

biofilter/
├── alembic/          # Database migrations
├── cli/              # CLI commands and entrypoints
├── core/             # Core orchestration logic
├── db/               # Database models, seeds, schema
├── etl/              # ETL framework and DTPs
├── query/            # Query layer
├── report/           # Report framework
├── tools/            # Developer and admin utilities
├── utils/            # Shared helper functions
├── biofilter.py      # Main Biofilter class
├── cli.py            # CLI bootstrap

```

Outside the main package:

```

├── notebooks/        # Executable examples and tutorials
├── scripts/          # Utility and maintenance scripts
├── docs/             # User and developer documentation
├── tests/            # Automated test suite
├── biofilter_data/   # Local data artifacts (ignored in prod)
├── versions/         # Versioned releases / snapshots

````

This structure is designed to keep runtime code, development artifacts, and
user-facing examples clearly separated.

---

## Dependency management (Poetry)

Biofilter 4 uses **Poetry** for dependency management and packaging.

Key files:

- `pyproject.toml` – authoritative dependency and package definition
- `poetry.lock` – locked dependency versions for reproducibility

Common developer commands:

```bash
poetry install
poetry update
poetry run biofilter --help
````

Poetry ensures:

* reproducible environments,
* clean dependency resolution,
* consistent CLI entrypoints.

---

## Code style and quality tools

Biofilter 4 follows standard Python tooling for code quality.

### Formatting

**Black** is used for automatic code formatting:

```bash
black biofilter/
```

### Linting and testing

* **pytest** is used for unit and integration tests
* **tox** orchestrates multi-environment testing

Relevant files:

* `pytest.ini`
* `tox.ini`
* `.coveragerc`

Example usage:

```bash
pytest
tox
```

---

## Logging artifacts

### Runtime logs

Biofilter writes execution logs to a file named:

```
biofilter.log
```

This file captures:

* ETL execution details,
* warnings and errors,
* debug-level traces when enabled.

Logs are ephemeral and environment-specific, intended primarily for debugging
and local inspection.

### Persistent ETL metadata

In contrast, **ETL Packages** are stored in the database and represent the
authoritative execution record.

This distinction is intentional:

* logs → transient, verbose, diagnostic
* ETL packages → persistent, structured, auditable

Developers should rely on **ETL reports** for operational monitoring rather than
log files.

---

## Configuration artifacts

### `.biofilter.toml`

The `.biofilter.toml` file defines project-level configuration, including:

* database connections,
* ETL behavior,
* logging defaults.

This file allows Biofilter to operate consistently across:

* CLI,
* Python API,
* ETL pipelines,
* queries,
* reports.

### `.env`

Environment variables may be used for sensitive values (e.g. credentials),
especially in shared, HPC, or cloud environments.

---

## Notebooks, scripts, and documentation

### Notebooks (`notebooks/`)

This folder contains:

* executable tutorials,
* report and query examples,
* schema exploration notebooks.

Notebooks serve both as documentation and as informal integration tests for the
API.

### Scripts (`scripts/`)

Utility scripts for:

* maintenance tasks,
* data inspection,
* one-off operations.

These scripts are not part of the core runtime but are useful for developers and
operators.

### Documentation (`docs/`, `docs-dev/`)

* `docs/` – user-facing documentation
* `docs-dev/` – developer-oriented notes and drafts

---

## Build and release artifacts

* `dist/` – packaged releases
* `build/` – intermediate build artifacts
* `versions/` – tagged or archived versions

These directories support:

* PyPI publishing,
* internal distribution,
* reproducible releases.

---

## Summary

This section provides a developer-oriented view of how Biofilter 4 is structured
and maintained.

By separating:

* core runtime code,
* ETL and knowledge integration logic,
* developer tooling,
* documentation and examples,

Biofilter 4 supports scalable development, reproducible builds, and long-term
maintainability.
