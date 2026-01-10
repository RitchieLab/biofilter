# Query API

The Query layer is the low-level access interface to the Biofilter knowledge base.
It is designed to expose the underlying data model in a controlled,
**Python-first, ORM-backed** way, enabling advanced users and developers to
explore, validate, and extend the system beyond predefined reports.

While **Reports** represent the primary user-facing abstraction, the **Query
layer** is the foundation on which Reports are built.

---

## Purpose of the Query layer

The Query layer exists to support use cases that require:

- direct access to Biofilter domain models,
- exploratory and ad-hoc data inspection,
- ETL validation and debugging,
- development of new Reports,
- schema exploration and learning,
- advanced or experimental workflows.

It provides a safe alternative to raw database connections, preserving schema
awareness, provenance, and consistency with Biofilter’s internal architecture.

---

## Design principles

The Query layer follows a small set of core principles:

### Model-first, not table-first
Users interact with **SQLAlchemy models**, not raw tables.

### ORM-backed but SQL-capable
Most queries are built using SQLAlchemy expressions, but raw SQL is supported
when needed.

### Single source of truth
Queries operate on the same schema, models, and session used by Reports and ETL.

### Analysis-ready outputs
All query execution methods return **pandas DataFrames** by default.

---

## Query layer position in the architecture

Conceptually, the Query layer sits between the physical database and the
higher-level abstractions:

```

Database (tables, indexes)
↓
SQLAlchemy Models
↓
Query Layer (bf.query)
↓
Reports / Custom Analysis / Debugging

````

Reports internally rely on the Query layer to fetch, join, and filter data.

---

## Models vs Tables (important distinction)

In Biofilter 4:

- A **table** is the physical structure stored in the database (rows and columns).
- A **model** is the Python representation of that table, defined using SQLAlchemy.

When using the Query layer, developers interact with **models**, not tables.

This abstraction provides:

- schema awareness,
- relationship navigation,
- safer query construction,
- portability across database engines.

In short:

> Tables store the data.  
> Models define how Biofilter interacts with it.

---

## Accessing the Query layer

The Query layer is accessed through the `Biofilter` instance:

```python
from biofilter import Biofilter
bf = Biofilter()
````

Once initialized, all query functionality is exposed under:

```python
bf.query
```

The Query layer automatically uses the same database connection and session as
Reports.

---

## Query interfaces supported

Biofilter supports two complementary query styles.

---

### 1. Python-first (ORM-based)

This is the recommended and most common approach.

It uses:

* SQLAlchemy models,
* SQLAlchemy expressions,
* schema-aware joins and filters.

Example:

```python
GeneMaster = bf.query.GeneMaster
stmt = (
    bf.query.select(GeneMaster)
    .where(GeneMaster.symbol == "A1BG")
)
df = bf.query.run_model(stmt)
```

This style is ideal for:

* interactive notebooks,
* report development,
* schema exploration,
* maintainable query logic.

---

### 2. SQL-first (raw SQL)

For advanced or legacy use cases, raw SQL execution is supported:

```python
sql = """
SELECT symbol, chromosome, start_pos, end_pos
FROM gene_masters gm
JOIN entity_locations el
  ON el.entity_id = gm.entity_id
WHERE gm.symbol = 'A1BG'
  AND el.build = 38
"""
df = bf.query.run_sql(sql)
```

Raw SQL is appropriate when:

* prototyping complex joins,
* debugging ETL or indexing issues,
* reproducing legacy queries,
* performing database-specific optimizations.

Both styles return pandas DataFrames and operate on the same schema.

---

## Convenience API: `bf.query.get()`

For quick lookups and sanity checks, Biofilter provides a simplified helper:

```python
df = bf.query.get("GeneMaster", symbol="A1BG")
```

Or using the model class directly:

```python
df = bf.query.get(
    bf.query.VariantSNP,
    chromosome="1",
    position_38=175292543
)
```

This method is intended for:

* quick validation,
* exploratory inspection,
* interactive debugging.

It is **not** meant for complex joins or production-grade logic.

---

## Model-driven queries with joins

For expressive queries involving joins across domains, the Query layer exposes:

* `bf.query.select`,
* all mapped SQLAlchemy models,
* full SQLAlchemy expression power.

Example (Gene → EntityAlias):

```python
GeneMaster = bf.query.GeneMaster
Alias = bf.query.EntityAlias

stmt = (
    bf.query.select(
        GeneMaster.symbol.label("gene_symbol"),
        Alias.xref_source.label("source"),
        Alias.alias_value.label("alias"),
        Alias.alias_type.label("alias_type"),
        Alias.is_primary.label("is_primary"),
    )
    .select_from(GeneMaster)
    .join(Alias, Alias.entity_id == GeneMaster.entity_id)
    .limit(10)
)

df = bf.query.run_model(stmt)
```

This pattern—**start from a domain master, join through the Entity layer**—is a
core Biofilter idiom.

---

## Schema exploration and introspection

The Query layer exposes utilities to explore available models:

```python
bf.query.list_models()
```

This allows developers to:

* discover domain models,
* inspect available fields,
* understand relationships between domains.

A guided walkthrough is available in:

📓 **Schema Explorer**
`schema_explorer.ipynb`

---

## Relationship to Reports

Reports are built on top of the Query layer.

In practice:

* **Queries** are flexible, low-level, and developer-oriented.
* **Reports** are standardized, reusable, and user-facing.

A common development workflow is:

1. Prototype logic using the Query layer.
2. Validate performance and correctness.
3. Encapsulate the logic into a Report.

---

## When to use Queries vs Reports

### Use Reports when:

* workflows are standardized,
* outputs must be reproducible,
* logic is shared across users or projects.

### Use Queries when:

* exploring unfamiliar data,
* validating ETL ingestion,
* debugging conflicts or relationships,
* developing new Reports,
* performing one-off analyses.

---

## Summary

The Query layer is the foundation of Biofilter 4’s data access model.

It provides:

* deep, schema-aware access to the knowledge base,
* Python-first and SQL-first workflows,
* safe and consistent interaction with entities, domains, and relationships.

Together with Reports, the Query layer enables Biofilter to serve both end users
and developers without compromising flexibility, performance, or correctness.
