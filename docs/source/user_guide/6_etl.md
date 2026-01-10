# ETL Pipelines

ETL pipelines are responsible for populating, updating, and maintaining the
Biofilter knowledge base. They ingest external biological data sources,
normalize identifiers, create or update entities, and materialize explicit
relationships used by queries and reports.

While most users interact with Biofilter through reports and queries, ETL
pipelines are typically executed by infrastructure, data engineering, or data
curation teams.

---

## ETL in Biofilter: Core Concepts

In Biofilter 4, each data source is ingested through a **Data Transformation
Package (DTP)**.

A DTP is a Python module that fully defines how a given data source is handled,
including:

- how data is extracted,
- how raw data is transformed and normalized,
- how curated knowledge is loaded into the database.

Each execution of a DTP produces an **ETL Package**, which represents a single,
traceable ingestion event.

---

## ETL Packages and Provenance Tracking

Every time a DTP is executed, Biofilter creates a package record that captures:

- source system (e.g. NCBI, HGNC),
- data source name and version,
- execution timestamps,
- execution status (success, failed, partial),
- warnings and error summaries.

These packages provide a persistent audit trail, allowing users to know exactly
when and how biological knowledge was updated.

---

## Full and Incremental Loads

Biofilter ETL supports both **full** and **incremental** ingestion strategies.

- **Full loads** rebuild all data associated with a given DTP.
- **Incremental loads** update only what has changed since the last execution.

Incremental execution enables:

- frequent updates without expensive full reloads,
- selective rollback or reprocessing of a single package,
- updating a specific data source without affecting others.

Importantly, an incremental package can be reverted or reprocessed without
deleting all data previously produced by the same DTP.

---

## Why Incremental Loads Matter in Biofilter 4

The distinction between full and incremental loads in Biofilter 4 is not merely
a performance optimization. It is a deliberate design choice driven by how
biological knowledge is integrated and preserved.

Biofilter 4 models biological concepts using **persistent entities** and
**shared relationships** across multiple data sources. A single entity (for
example, a gene or variant) may participate in relationships discovered,
inferred, or curated by many independent DTPs.

Blindly rebuilding all data for one source could unintentionally remove valid
relationships introduced by other sources.

Incremental loads were introduced primarily to support this **entity-centric
architecture**. By updating only the data contributed by a specific DTP
execution, Biofilter can refresh or correct a data source without deleting
entities or relationships derived from other sources.

This behavior is especially important for:

- master data (e.g. genes, variants, pathways),
- shared entities referenced across multiple domains,
- relationships inferred or curated by independent data sources.

In contrast, full loads are appropriate when a complete rebuild is explicitly
desired or when a data source is fully isolated from others.

By supporting both strategies, Biofilter 4 balances **data freshness**,
**knowledge preservation**, and **cross-source consistency**.

---

## Running ETL Pipelines (Python API)

ETL pipelines can be executed interactively via the Python API.

```python
from biofilter import Biofilter

bf = Biofilter()
bf.update(
    data_sources="mondo",
    run_steps="extract",
    force_steps=True,
)
````

---

## Running ETL Pipelines (CLI)

ETL pipelines can also be executed via the command-line interface.

```bash
biofilter etl update \
  --db-uri sqlite:///biofilter.db \
  --source-system HGNC \
  --run-step extract \
  --run-step transform \
  --run-step load
```

---

## Orchestration and Automated Updates

Biofilter ETL can be integrated into an orchestration layer that periodically
checks upstream data sources for new versions.

When a new release is detected, the orchestrator can automatically:

* trigger the appropriate DTP,
* generate a new ETL Package,
* update the knowledge base incrementally.

This design enables automated, reproducible data refresh cycles without manual
intervention.

---

## Monitoring ETL with Reports

Biofilter exposes ETL status and provenance through **ETL Reports**, allowing
users to monitor ingestion activity directly from the API.

### ETL Status Report

```python
from biofilter import Biofilter

bf = Biofilter()
df = bf.report.run(
    "etl_status",
    source_system="ncbi",
)
```

This report provides a high-level view of:

* current ETL state,
* latest execution status per data source,
* recent failures or warnings.

### ETL Package History Report

```python
df = bf.report.run(
    "etl_packages",
    source_system="ncbi",
)
```

This report lists individual ETL packages and can be used to:

* track update history,
* audit data provenance,
* identify failed or partial loads,
* correlate knowledge updates with downstream analysis results.

---

## Logs and Observability

During ETL execution, Biofilter generates:

* structured logs written to `biofilter.log` in the execution directory,
* persistent package metadata stored in the database.

While logs capture low-level execution details, **ETL Packages** are the
authoritative record of success, failure, warnings, and timestamps.

Users are encouraged to rely on:

* `etl_status` reports for operational monitoring,
* `etl_packages` reports for auditing and historical analysis.

---

## Summary

ETL pipelines form the foundation of Biofilter 4. Through DTPs and ETL Packages,
Biofilter provides a traceable, flexible, and scalable ingestion framework that
supports both full and incremental updates, centralized or local deployments,
and robust monitoring through reports and logs.
