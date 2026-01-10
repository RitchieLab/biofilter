# Managed Indexes

Biofilter 4 implements an explicit, **system-managed indexing strategy** designed
to balance ETL performance, query efficiency, and schema evolvability across both
SQLite and PostgreSQL backends.

Unlike ad-hoc index creation at the database level, Biofilter centralizes index
definitions in code and applies them dynamically during ETL execution and database
lifecycle events.

---

## Why Biofilter manages indexes explicitly

Biofilter databases support:

- very large bulk inserts (ETL),
- highly connected entity relationships,
- heterogeneous query patterns (annotation, filtering, modeling),
- both lightweight (SQLite) and production-grade (PostgreSQL) engines.

Creating and maintaining all indexes permanently would significantly degrade ETL
performance, especially for large data sources such as **dbSNP**, **UniProt**, or
**MONDO**.

To address this, Biofilter:

- drops indexes before heavy ETL loads,
- applies database tuning optimized for write performance,
- recreates optimized indexes after loading completes.

This strategy allows Biofilter to scale ingestion without sacrificing query
performance.

---

## DBTuningMixin: the index control layer

Index creation and removal are handled by the **DBTuningMixin**, which is used by
ETL components and DTPs.

Key responsibilities:

- switching databases into *write mode* (bulk-insert optimized),
- dropping relevant indexes before load,
- recreating indexes after load,
- applying engine-specific tuning (SQLite PRAGMAs, PostgreSQL compatibility).

This mixin supports both SQLite and PostgreSQL transparently, allowing developers
to write backend-agnostic ETL code.

---

## Index specifications: how indexes are defined

Indexes are defined as **declarative specifications**, not inline SQL.

Each domain exposes index definitions through properties such as:

- `get_entity_index_specs`
- `get_gene_index_specs`
- `get_variant_index_specs`
- `get_disease_index_specs`
- `get_go_index_specs`

Each specification follows the structure:

```python
(table_name, [column1, column2, ...])
````

Example:

```python
("entity_aliases", ["xref_source", "alias_value"])
```

Index names are generated automatically using the pattern:

```
idx_<table>_<column1>_<column2>_...
```

This guarantees consistent naming across engines and environments.

---

## When indexes are dropped and recreated

During ETL execution, index handling follows a strict lifecycle:

### Before load

* Indexes relevant to the target domain are dropped.
* Database is switched to write-optimized mode.

### During load

* Entities, master data, and relationships are inserted efficiently.

### After load

* Indexes are recreated.
* Database returns to read-optimized mode.

⚠️ **Indexes are not permanent fixtures during ETL.**
They are lifecycle-managed by the system and should be treated as such.

---

## Adding indexes for a new domain

When introducing a new omics domain or extending an existing one, developers must:

1. **Identify query access patterns**

   * lookups by ID,
   * joins to `entity_id`,
   * filters by `data_source_id`, `etl_package_id`,
   * genomic coordinates (for variants).

2. **Define index specs** in the appropriate mixin or model group.

3. **Ensure coverage for**

   * entity linkage,
   * provenance filtering,
   * common report and query paths.

### Example (Disease domain)

```python
@property
def get_disease_index_specs(self):
    return [
        ("disease_masters", ["disease_id"]),
        ("disease_masters", ["entity_id"]),
        ("disease_masters", ["data_source_id"]),
        ("disease_group_memberships", ["disease_id"]),
        ("disease_group_memberships", ["group_id"]),
    ]
```

These indexes directly support:

* disease lookups,
* joins to entities,
* filtering by source,
* group membership queries.

---

## Modifying models: index considerations

When modifying or adding columns to a model, developers should ask:

* Will this field be used in filters?
* Will it be part of joins?
* Will it appear in reports or queries?
* Is it used to scope data by source, package, or status?

If the answer is **yes**, index specifications should be updated accordingly.

Failure to do so may result in:

* slow reports,
* inefficient joins,
* degraded performance at scale.

---

## SQLite vs PostgreSQL behavior

Biofilter applies the same logical index definitions to both engines, but tuning
differs.

### SQLite

* Aggressive write-mode PRAGMAs during ETL.
* Index creation is critical for post-load performance.
* Highly sensitive to unnecessary indexes during bulk inserts.

### PostgreSQL

* Index creation is more expensive.
* Composite (multi-column) indexes are preferred.
* Partition-aware indexing is supported in variant tables.

Developers should **not** hardcode engine-specific indexes unless strictly
necessary.

---

## Indexes vs Alembic migrations

A critical distinction:

### Managed indexes (DBTuningMixin)

* created and dropped dynamically,
* optimized for runtime behavior,
* **not tracked** as schema migrations.

### Structural schema changes

* managed via Alembic,
* versioned and persistent,
* applied once per database lifecycle.

Indexes belong to **runtime optimization**, not schema definition.

---

## Best practices

* Always define indexes centrally, never inline.
* Prefer composite indexes aligned with real query patterns.
* Drop indexes before large ETL loads.
* Recreate indexes explicitly after load.
* Test performance using realistic data volumes.
* Treat indexes as part of ETL design, not an afterthought.

---

## Summary

Biofilter 4 uses a deliberate, system-managed indexing strategy to support:

* large-scale ETL,
* entity-centric joins,
* mixed analytical workloads,
* multiple database backends.

When extending Biofilter, developers must treat indexes as **first-class
architectural components**, ensuring new domains integrate cleanly without
compromising performance or scalability.
