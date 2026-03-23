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
