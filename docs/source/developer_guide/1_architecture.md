# Architecture

Biofilter 4 was designed as an extensible omics knowledge platform rather than a
monolithic application. This chapter introduces the architectural principles
and internal structure that developers need to understand in order to extend,
customize, or contribute to the platform safely and consistently.

The goal of this chapter is not to document every internal detail, but to
establish **mental models and boundaries**: what is core, what is extensible,
and how the different layers interact.

---

## Architectural overview

At a high level, Biofilter 4 is organized around four major layers:

1. **Entity-Centric Knowledge Core**
2. **Domain-Specific Master Data**
3. **ETL & Provenance Framework**
4. **Access Layers (Reports and Queries)**

These layers are intentionally decoupled but tightly integrated through shared
identifiers and explicit contracts.

Developers should think of Biofilter 4 as a **knowledge integration engine**
that persists biological identities and relationships over time, while allowing
new domains, data sources, and access patterns to be added incrementally.

---

## Entity-centric foundation

The entity layer is the foundation of Biofilter 4.

Entities represent **persistent biological identities** (e.g. genes, variants,
diseases, pathways) independent of any single data source. Each entity:

- has a stable internal identifier,
- may have multiple aliases and external identifiers,
- participates in typed relationships with other entities,
- can be referenced by multiple domains and ETL pipelines.

Conceptually, this layer behaves like a **knowledge graph**, but it is
implemented in a relational database to support performance, auditability, and
operational simplicity.

### Developer guidance

- Do **not** duplicate biological identities in domain tables.
- Always link domain-specific records back to the entity layer.
- Relationships between concepts should be modeled **explicitly**, not inferred
  at query time.
- The entity layer is considered **core infrastructure** and should evolve
  conservatively.

---

## Domain-specific master data

Above the entity layer, Biofilter 4 supports **domain-specific master tables**,
such as genes, variants, proteins, pathways, and diseases.

Each domain:

- owns its specialized attributes and semantics,
- defines tables optimized for that domain’s data and query patterns,
- links back to entities to preserve global consistency.

This separation allows new biological domains to be added without modifying the
core entity model.

### Developer guidance

- Domain models are the **primary extension point** for new biological areas.
- Domains may evolve independently as long as entity linkage remains stable.
- Avoid embedding cross-domain logic directly into domain tables.

---

## ETL as a first-class architectural component

ETL is not an external preprocessing step in Biofilter 4—it is a **first-class
architectural concern**.

Each data source is integrated via a **Data Transformation Package (DTP)**,
which defines:

- how data is extracted,
- how it is normalized and transformed,
- how entities, relationships, and domain records are created or updated.

Every ETL execution produces an **ETL Package**, a persistent database record
that captures:

- execution metadata,
- source versions,
- timestamps,
- success or failure status,
- record counts and warnings.

This design enables traceability, reproducibility, and safe incremental updates.

### Developer guidance

- ETL logic must be **deterministic and auditable**.
- Incremental updates must preserve entities and relationships created by other
  DTPs.
- ETL Packages are the **authoritative record** of ingestion state — not logs.

---

## Access layers: Reports vs Queries

Biofilter 4 exposes its knowledge through two complementary access layers.

### Reports (high-level)

Reports are predefined, reusable query modules intended for most users. They:

- encapsulate validated biological logic,
- provide stable inputs and outputs,
- support annotation, filtering, and modeling workflows.

Reports are the **preferred interface** for end users.

### Queries (low-level)

Queries provide direct access to the data model for advanced users and
developers. They:

- expose SQLAlchemy models and relationships,
- allow exploratory analysis and debugging,
- serve as a prototyping layer for new reports.

### Developer guidance

- New user-facing functionality should usually start as a **query** and mature
  into a **report**.
- Reports should not expose internal schema complexity.
- Queries may be flexible; reports must be **stable**.

---

## Separation of concerns

Biofilter 4 enforces a deliberate separation of responsibilities:

- **Entities** define identity and relationships.
- **Domains** define biological attributes.
- **ETL** defines how knowledge enters the system.
- **Reports and Queries** define how knowledge is accessed.

This separation allows the platform to scale in both data volume and conceptual
complexity without collapsing into tightly coupled logic.

---

## What developers should (and should not) modify

### Safe extension points

- Adding new domains
- Adding new DTPs
- Creating new reports
- Writing custom queries
- Adding new relationships via ETL

### Core components (modify with care)

- Entity schemas
- Relationship semantics
- ETL package lifecycle
- Provenance models

Changes to core components should be carefully reviewed, as they affect **all**
domains and workflows.

---

## Architectural takeaway

Biofilter 4 is best understood as:

> A persistent, entity-centric biological knowledge platform with explicit
> provenance and modular access layers.

Developers are encouraged to extend the system by **adding knowledge**, not by
rewriting the core.

