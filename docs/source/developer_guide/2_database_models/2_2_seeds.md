# Database Seeds

Biofilter 4 uses database seed files to initialize and maintain essential
reference data required for the system to operate correctly. Seeds provide a
controlled and reproducible mechanism to bootstrap a new database and to evolve
core metadata over time **without requiring a full database rebuild**.

Seeds are **not biological knowledge**. Instead, they define the structural and
semantic foundation upon which biological data is later ingested.

---

## What are seeds?

Seeds are structured JSON files stored in the `db/seed/` directory.
They are applied during:

- initial database creation (cold start),
- controlled system updates,
- introduction of new reference metadata (e.g. new data sources or source systems).

Each seed file contains a curated set of records that are **idempotent and safe
to reapply**, ensuring consistency across environments.

---

## Purpose of seeds

Seeds serve several critical roles in Biofilter 4:

- establish required reference tables,
- define controlled vocabularies and enumerations,
- ensure consistent identifiers across deployments,
- decouple system metadata from biological ingestion pipelines.

By separating seed data from ETL-driven biological data, Biofilter 4 maintains a
clean boundary between **platform configuration** and **domain knowledge**.

---

## Seed lifecycle and cold start behavior

When a new database is created:

1. the schema is initialized,
2. all seed files are applied,
3. no biological knowledge is loaded.

This state is referred to as a **cold start database**.

A cold start database contains:

- a valid schema,
- required metadata,
- reference entities needed for ETL execution.

However, it contains **no genes, variants, pathways, or relationships** until
ETL pipelines are executed.

---

## Overview of seed files

Below is a high-level overview of the main seed files and their purpose:

### `initial_source_systems.json`
Defines authoritative source systems  
(e.g. `NCBI`, `Ensembl`, `UniProt`).

### `initial_data_sources.json`
Defines individual data sources associated with source systems  
(e.g. `dbSNP`, `HGNC`).

### `initial_entity_group.json`
Defines high-level entity groups  
(e.g. Genes, Variants, Diseases, Pathways).

### `initial_entity_relationship_types.json`
Defines allowed relationship types between entities  
(e.g. gene–disease, gene–pathway).

### `initial_genome_assemblies.json`
Registers supported genome assemblies  
(e.g. GRCh37, GRCh38).

### `initial_omic_status.json`
Defines omic-level status classifications used across domains.

### `initial_etl_processes.json`
Registers known ETL process identifiers and execution semantics.

### `initial_metadata.json`
Stores general system-level metadata required at runtime.

### `initial_config.json`
Provides default system configuration values stored in the database.

Each seed file targets a **specific responsibility** and is intentionally kept
small, explicit, and human-readable.

---

## Seeds vs ETL ingestion

It is important to distinguish seeds from ETL ingestion.

### Seeds

- define system structure and semantics,
- are applied centrally and infrequently,
- do **not** depend on external biological files.

### ETL pipelines

- ingest biological knowledge,
- generate entities, relationships, and domain records,
- produce ETL packages and provenance metadata.

Seeds enable ETL pipelines to operate consistently, but **they do not replace
ETL**.

---

## Updating seeds over time

Seeds may evolve as Biofilter 4 grows:

- new source systems may be added,
- new entity groups or relationship types may be introduced,
- controlled vocabularies may expand.

These updates are applied **incrementally** and do not require recreating the
database.

When combined with Alembic migrations, seeds allow Biofilter to evolve safely
while **preserving existing biological knowledge**.

---

## Design philosophy

The seed system reflects a core Biofilter design principle:

> **Structural knowledge should be explicit, versioned, and reproducible.**

By externalizing critical metadata into seed files, Biofilter 4 ensures that:

- deployments are consistent across environments,
- system behavior is transparent,
- infrastructure changes do not invalidate biological data.

---

## Takeaway for developers

Seeds define **how Biofilter works**, not **what Biofilter knows**.

If you are:

- adding a new data source,
- introducing a new relationship type,
- expanding controlled vocabularies,

you are likely working with **seeds**, not ETL.

Biological knowledge always enters the system through **ETL pipelines** — seeds
exist to make that process deterministic, auditable, and reproducible.
