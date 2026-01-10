# Architecture

## System Overview
Biofilter 4 is a **persistent biological knowledge platform** designed to
ingest heterogeneous third-party resources into a unified, queryable
knowledge base.

At a system level, Biofilter 4 consists of four major components:

### Knowledge Storage (Database)
A normalized relational schema that stores biological entities, aliases,
explicit relationships, and domain-specific master data.

### Ingestion (ETL Framework)
A standardized Extract–Transform–Load pipeline that downloads, normalizes,
and integrates external data sources with full provenance and version
tracking.

### Query Layer
A set of programmatic and report-driven interfaces that expose the knowledge
base for annotation, filtering, and modeling workflows.

### User Interfaces
Multiple access patterns depending on execution context, including:

- Python API
- SQL
- Command-line interface (CLI)
- Interactive notebooks

These interfaces support local execution, HPC environments, cloud platforms,
and automated pipelines.

---

## High-Level Data Flow

External biological data sources are integrated through domain-specific
ingestion packages. During ingestion, Biofilter 4:

- normalizes identifiers,
- records aliases and cross-references,
- creates or updates entities,
- materializes explicit relationships within the knowledge base.

Biofilter 4 can operate against either:

- a **shared central knowledge database**, provided as *Knowledge as a Service
  (KaaS)*, or
- a **user-managed local database**.

In a KaaS deployment, biological knowledge is curated, versioned, and
maintained centrally. Users do not need to create or manage a database
schema; they simply configure Biofilter 4 to connect to the existing
knowledge base and immediately begin querying integrated biological
knowledge.

Alternatively, users may create and manage a local Biofilter 4 database
(e.g., using SQLite). This mode enables local ingestion of custom data
sources, experimental datasets, or isolated workflows requiring full control
over the knowledge base.

Once a knowledge base is available—shared or local—users primarily interact
with Biofilter 4 through queries and reports, enabling iterative analysis
without repeatedly rebuilding biological knowledge.

---

## Database Schema

Biofilter 4 was designed to balance **consistency**, **flexibility**, and
**extensibility**.

To achieve this, the database schema combines principles inspired by
**star-schema organization** and **knowledge graph modeling**, implemented
within a relational database.

At the center of the schema, entities and their relationships form a highly
connected structure that closely resembles a knowledge graph. These
connections explicitly capture biological relationships and naturally
support analyses that benefit from network-based representations.

Surrounding this core, domain-specific master data extend entities with rich,
specialized attributes while preserving shared biological identity and
relationships.

### Layered Schema Organization

Biofilter 4 uses a layered relational schema organized around three
conceptual layers.

#### Entity & Relationship Core

The core layer provides stable biological identities and explicit
relationships between entities. It captures:

- entity identity,
- multiple names and aliases per entity,
- typed relationships between entities.

This layer serves as the central integration surface of the system.

#### Domain-Specific Master Data

Each biological domain (e.g. genes, variants, proteins, pathways) maintains
its own specialized tables and rich attribute sets, optimized for domain-
specific data and query patterns.

Domain records are linked back to the shared entity core to preserve global
consistency across domains.

#### ETL Metadata and Provenance

Biofilter 4 records where knowledge originated and how it was integrated,
including:

- source system metadata,
- data source versions,
- ingestion packages,
- execution timestamps and status.

This layer ensures traceability, auditability, and reproducibility of the
integrated knowledge.

### Design Implications

By separating entity identity, relationships, and domain-specific attributes,
Biofilter 4 enables the platform to scale to new omics domains while
preserving a stable and consistent integration surface.

The result is a relational knowledge base that combines the auditability of
structured schemas with the flexibility of graph-inspired modeling.

---

## ETL Framework

The Biofilter 4 ETL framework supports both the **persistent knowledge
database**, which represents the primary product of the platform, and
**intermediate data layers** that enable flexible processing and exploration
workflows.

During ingestion, external data sources are:

1. staged into a controlled input zone,
2. transformed using domain-specific normalization and cleaning rules,
3. materialized into standardized Parquet datasets.

These transformed outputs can be:

- loaded into the relational knowledge base, or
- retained in intermediate storage layers for high-granularity or exploratory
  analysis.

This design supports **hybrid data architectures**, where curated knowledge
is stored in relational form while richer or more detailed representations
remain accessible through data lakes, NoSQL systems, or analytical engines
such as DuckDB.

### Load Management and Versioning

Biofilter 4 implements explicit load management through a centralized ETL
manager.

Each execution of an ingestion process generates a distinct **Data Package**,
representing a coherent snapshot of a data source at a specific point in
time.

Data packages enable:

- provenance and lineage tracking,
- versioned updates of external sources,
- auditing of ingestion outcomes,
- reproducible knowledge builds.

By treating each ingestion run as a versioned package, Biofilter 4 ensures
that both the knowledge base and intermediate datasets can be consistently
traced and reproduced across time.

---

## Indexing & Performance

Biofilter 4 is designed to operate efficiently across a wide range of
deployment scenarios, from lightweight local environments to large-scale
production systems.

The same core data model and query patterns apply whether the platform is used
with SQLite or with more robust relational backends such as PostgreSQL.

### From Local Databases to Production Deployments

For exploratory or development workflows, Biofilter 4 can be deployed using
SQLite, enabling fast setup and minimal operational overhead.

For production environments and large-scale knowledge bases, Biofilter 4
leverages PostgreSQL features such as:

- improved concurrency,
- advanced query planning,
- storage optimization.

### Partitioning and Scalable Data Access

To support scalability, Biofilter 4 employs partitioning strategies for
high-volume domains where appropriate.

For example, variant-related tables may be partitioned by chromosome, enabling
efficient partition pruning during query execution and reducing I/O for
common gene-centric and position-based queries.

This approach allows Biofilter 4 to scale to very large variant collections
while preserving interactive performance.

### Index Management and User Control

Biofilter 4 provides a curated set of default indexing strategies that reflect
the most common query patterns across entity-centric and domain-specific
workflows.

In most cases, index creation and maintenance are handled automatically by
the platform.

Advanced users may optionally:

- drop or disable indexes to accelerate bulk ingestion,
- create custom indexes for specialized queries,
- rebuild indexes as part of resource-aware maintenance workflows.

This balance provides sensible defaults while allowing expert-level
performance tuning when required.
