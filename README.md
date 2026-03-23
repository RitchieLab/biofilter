# Biofilter 4

**Biofilter 4** is a persistent, entity-centric biological knowledge platform designed to support gene-centric annotation, filtering, and modeling workflows through a unified and extensible data architecture.

This branch (`biofilter3r`) contains the active development of **Biofilter 4**, representing a major evolution of the Biofilter framework with a redesigned schema, modern ETL architecture, and multiple interaction layers.

📚 **Documentation**:  
👉 https://biofilter.readthedocs.io/en/latest/

---

## What is Biofilter 4?

Biofilter 4 provides a **persistent, versioned biological knowledge base** that replaces traditional file-based annotation workflows with a reusable, query-driven platform.

Instead of repeatedly generating transient annotation files, Biofilter 4 enables users to:

- ingest curated biological knowledge once,
- store it in a normalized, entity-based schema,
- reuse and query that knowledge across analyses, projects, and environments.

Biofilter 4 is designed to support both **exploratory research** and **production-scale workflows**.

---

## Core Concepts: Entities, Domains, and Relationships

Biofilter organizes biological knowledge around three core concepts:

- **Entities**
  - Canonical biological objects (for example Gene, Variant, Disease, Protein, Pathway).

- **Domains**
  - Functional/omics contexts used to structure and interpret entities and their links.

- **Entity Relationships**
  - A relational layer that connects entities across domains and behaves like a graph traversal surface (including multi-hop relationship discovery) while staying in a SQL-native environment.

This design lets users recover cross-omics relationships and reuse them directly in reports for:

- annotation workflows,
- filtering and prioritization workflows,
- relationship-driven analyses that support downstream statistical modeling.

---

## Key Features

- **Entity-centric data model**
  - Canonical entities (Gene, Variant, Disease, Protein, Pathway, etc.)
  - Rich alias and cross-reference support

- **Persistent knowledge layer**
  - Versioned ETL packages
  - Full provenance tracking by data source and load

- **Modular ETL architecture**
  - Data Transformation Packages (DTPs)
  - Explicit separation of master data and relationships

- **High-performance ingestion**
  - Managed indexing strategy
  - Optimized for large-scale sources (e.g. dbSNP, UniProt)

- **Multiple interaction layers**
  - Python API
  - ORM-based data access
  - Reusable Reports
  - Command-line interface (CLI)

- **Multi-database support**
  - SQLite (local development)
  - PostgreSQL (production and large-scale deployments)

---

## Architecture Overview

At a high level, Biofilter 4 consists of:

- **ETL Layer**
  - Ingests external biological sources into a normalized schema
  - Tracks execution via ETL Packages

- **Core Schema**
  - Entity, Alias, Relationship, and Domain Master tables
  - Designed for extensibility and long-term evolution

- **Data Access Layer**
  - ORM-backed, Python-first access to the knowledge base
  - Foundation for reports and advanced analysis

- **Report Layer**
  - Curated, reusable biological queries
  - Standardized outputs as pandas DataFrames

---

## Repository Structure (simplified)

```text
biofilter/
├── alembic/          # Database migrations
├── cli/              # CLI commands and entrypoints
├── core/             # Core orchestration logic
├── db/               # Database models and schema
├── etl/              # ETL framework and DTPs
├── report/           # Report framework
├── tools/            # Developer and admin utilities
├── utils/            # Shared helpers
├── biofilter.py      # Main Biofilter entry point
├── cli.py            # CLI bootstrap

docs/
├── source/           # Sphinx documentation source
└── requirements.txt  # Documentation build requirements
````

---

## Documentation

The full **User Guide** and **Developer Guide** are hosted on Read the Docs:

📖 **[https://biofilter.readthedocs.io/en/latest/](https://biofilter.readthedocs.io/en/latest/)**

The documentation covers:

* Installation and setup
* Data sources and ETL design
* Writing DTPs
* Managed indexes
* Entity and alias registration
* Data access and report internals
* Writing and extending reports
* Developer tooling and project structure

---

## Run with Docker (Container)

Biofilter 4 can be executed as an application-only container, using an external database via `DATABASE_URL`.

Build from this repository:

```bash
docker build -t biofilter:bf4 -f docker/Dockerfile .
```

Run CLI with external DB:

```bash
docker run --rm \
  -e DATABASE_URL="postgresql+psycopg2://user:password@host:5432/biofilter_prod" \
  biofilter:bf4
```

Run a report and save output to your local machine:

```bash
docker run --rm \
  -e DATABASE_URL="postgresql+psycopg2://user:password@host:5432/biofilter_prod" \
  -v "$(pwd)/outputs:/workspace/outputs" \
  biofilter:bf4 \
  biofilter report run \
    --report-name etl_status \
    --output /workspace/outputs/etl_status.csv
```

Open an interactive shell in the container:

```bash
docker run --rm -it \
  -e DATABASE_URL="postgresql+psycopg2://user:password@host:5432/biofilter_prod" \
  -v "$(pwd):/workspace" \
  --entrypoint /bin/bash \
  biofilter:bf4
```

For full container documentation (publishing, multi-arch, GitHub Actions), see:

- [docker/README.md](docker/README.md)

---

## Status

* **Current version**: Biofilter 4 (active development)
* **Schema**: Entity-centric, versioned
* **ETL**: Modular DTP-based ingestion
* **Stability**: Actively evolving; APIs and schema may continue to evolve prior to a formal 4.0 release

---

## Contributing

Contributions, feedback, and design discussions are welcome.

When contributing:

* Follow existing architectural patterns (Entities, DTPs, Reports).
* Keep provenance and reproducibility as first-class concerns.
* Prefer ORM-based logic over raw SQL when possible.
* Document new features in the appropriate section of the docs.

---

## License

MIT License. See [LICENSE](LICENSE).

---

## Acknowledgements

Biofilter builds on years of development and scientific usage across multiple generations of the framework. Biofilter 4 represents a continuation of this work, redesigned to support modern data volumes, richer biological relationships, and long-term sustainability.
