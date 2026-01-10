# ETL

## High-level ETL architecture

Biofilter 4 implements ETL as a **first-class internal subsystem**, tightly
integrated with the database schema, entity model, and provenance tracking.

At a high level:

- **ETLManager** orchestrates ingestion
- **DTPs** implement source-specific logic
- **ETLSourceSystem / ETLDataSource** define provenance and routing
- **ETLPackages** track every execution
- **Entities** act as the integration backbone

---

## Project structure (ETL layer)

The ETL system lives under the `biofilter/etl` package:

```text
biofilter/
└── etl/
    ├── dtps/              # All active Data Transformation Packages
    ├── mixins/            # Shared ETL utilities and helpers
    ├── etl_manager.py     # Central ETL orchestrator
    ├── conflict_manager.py
````

---

## DTP registry

Each file under `etl/dtps/` represents one **active data source ingestion
pipeline**.

Examples:

* `dtp_gene_hgnc.py`
* `dtp_gene_ncbi.py`
* `dtp_go.py`
* `dtp_uniprot.py`
* `dtp_variant_ncbi.py`
* `dtp_reactome.py`

This structure makes ETL **explicit, discoverable, and extensible**.

---

## Source System vs Data Source (core concept)

Biofilter distinguishes **where data comes from** from **what data is being
ingested**.

### Source System

A **Source System** represents the upstream provider or authority.

Examples:

* NCBI
* Ensembl
* UniProt
* Reactome
* MONDO

Stored in: `ETLSourceSystem`

---

### Data Source

A **Data Source** represents a specific dataset or channel within a Source
System.

Examples:

* NCBI → dbSNP
* NCBI → Genes
* UniProt → Proteome
* Reactome → Pathways

Stored in: `ETLDataSource`

➡️ A single Source System can have **multiple Data Sources**, each mapped to a
specific DTP.

---

## ETLManager: the orchestration layer

The **ETLManager** is the central orchestrator responsible for:

* resolving source systems and data sources,
* locating the correct DTP,
* executing ETL steps in the correct order,
* managing execution context,
* creating ETL Packages,
* handling incremental vs full loads.

From a developer perspective:

> **ETLManager does not contain source-specific logic.**
> It coordinates and standardizes execution.

When an ETL run is triggered:

1. The manager looks up `ETLSourceSystem` and `ETLDataSource`
2. Resolves the corresponding DTP
3. Executes `extract → transform → load`
4. Records execution as an `ETLPackage`

---

## Data Transformation Packages (DTPs)

A **DTP** is a Python class that implements all ingestion logic for **one Data
Source**.

### Design principles

All DTPs:

* follow a shared interface,
* inherit from a common base class,
* use standardized logging,
* share helper mixins,
* emit structured provenance.

---

### Core methods

Every DTP implements three primary methods:

```python
def extract(self):
    ...

def transform(self):
    ...

def load(self):
    ...
```

These methods are called by the `ETLManager` in a controlled execution context.

---

## Extract: landing zone

The **extract** step retrieves raw upstream data.

Key characteristics:

* data is downloaded or copied into a landing directory,
* raw files are preserved (immutability preferred),
* no normalization occurs here.

This design supports:

* reprocessing,
* debugging,
* reproducibility,
* offline inspection.

---

## Transform: normalization & enrichment

The **transform** step converts raw inputs into standardized, tabular
representations, typically stored as **Parquet files**.

Key goals:

* normalize identifiers,
* clean and validate fields,
* apply domain-specific rules,
* preserve higher granularity than the relational DB.

**Important design choice**

Transformed data is often **richer and more granular** than what is loaded into
the database.

This enables:

* exploratory analysis via DuckDB,
* ad hoc annotation pipelines,
* future NoSQL or data lake integrations.

---

## Load: entity-first ingestion

The **load** step ingests normalized Parquet data into the relational schema.

Load order is **strict and intentional**:

1. **Entities**
2. **Domain master records**
3. **Relationships**

---

### Why entity-first matters

Entities are shared across:

* domains,
* data sources,
* DTPs.

This prevents:

* duplicate identities,
* accidental deletion of cross-source relationships,
* broken incremental updates.

---

## Incremental vs full loads (developer view)

Incremental loads exist because of the **entity-centric architecture**, not just
performance.

* **Full loads** rebuild data for a source
* **Incremental loads** update only the delta introduced by a DTP execution

Incremental packages:

* do not delete entities created by other sources,
* can be reverted independently,
* preserve cross-domain relationships.

This is critical when:

* multiple sources annotate the same gene or variant,
* master data evolves asynchronously.

---

## Conflict detection & curation

Biofilter includes a **conflict management layer** to detect inconsistencies in
master data.

Example conflicts:

* two HGNC IDs pointing to the same Entrez ID,
* inconsistent identifiers across sources,
* incompatible mappings for canonical entities.

Conflicts are:

* detected during load,
* recorded explicitly,
* surfaced via reports and APIs,
* resolvable through curated actions.

This ensures that data quality issues are **visible, not silent**.

---

## ETL Packages & provenance

Every execution generates an **ETLPackage** record containing:

* execution timestamps,
* success / failure status,
* record counts,
* warnings and errors (JSON),
* source and version metadata.

ETL Packages are **persistent** and form the backbone of:

* auditing,
* monitoring,
* rollback,
* reporting.

---

## ETL monitoring via reports

ETL state should be monitored using **reports**, not logs.

Examples:

```python
bf.report.run(
    "etl_status",
    source_system="ncbi",
    data_sources=["dbsnp_chr6"],
)
```

```python
bf.report.run(
    "etl_packages",
    source_system="ncbi",
)
```

These reports provide a **stable operational view** independent of runtime
environments.

---

## Logs vs ETL metadata (developer distinction)

**Logs (`biofilter.log`)**

* high-granularity
* execution-time only
* environment-dependent

**ETL Packages**

* persistent
* structured
* authoritative

➡️ Logs are for **debugging**
➡️ ETL Packages are for **auditing and monitoring**

---

## Extending Biofilter with new DTPs

To add a new data source:

1. Create a new DTP under `etl/dtps/`
2. Register its Source System and Data Source
3. Implement `extract`, `transform`, `load`
4. Follow entity-first loading rules
5. Emit provenance correctly

This makes Biofilter **plug-and-play at the ingestion layer**.

---

## Architectural takeaway (developer)

Biofilter 4 ETL is designed to:

* preserve biological identity,
* support incremental evolution,
* prevent cross-source data loss,
* enable long-term knowledge accumulation.

**The ETL layer is not just ingestion —
it is the engine that maintains a living biological knowledge base.**


