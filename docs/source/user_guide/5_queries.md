# Queries

Queries provide a low-level and flexible interface for exploring the Biofilter
knowledge base beyond predefined reports. They are intended for advanced users
who require deeper control, custom access patterns, or a clearer understanding
of how biological knowledge is modeled internally.

While most users interact with Biofilter through reports, queries enable
exploration, debugging, and advanced customization, all without requiring direct
interaction with raw database connections.

---

## Why Queries Exist

Reports encapsulate standardized and reusable logic. Queries exist to complement
reports by supporting:

- exploration of the underlying data model,
- validation and debugging of ETL ingestion,
- inspection of biological relationships,
- prototyping of new report logic,
- custom or experimental analysis workflows.

Through a controlled API, queries expose the database schema in a way that is
both powerful and safe, allowing most use cases to be addressed without writing
raw SQL.

---

## Queries vs Reports

- **Use Reports** when analyses are standardized, results must be reproducible,
  and logic is shared across projects.

- **Use Queries** when exploring unfamiliar data, validating ingestion results,
  building custom logic, or developing new report definitions.

In practice, queries often serve as a **development and exploration layer**,
while reports represent the **production and reuse layer**.

---

## Initializing Biofilter for Queries

Queries use the same Biofilter instance and database connection as reports.

```python
from biofilter import Biofilter

bf = Biofilter()
````

If a database URI is defined in `.biofilter.toml`, Biofilter connects
automatically. Alternatively, a database URI can be provided explicitly:

```python
bf = Biofilter("sqlite:///path/to/biofilter.db")
```

---

## Query Interfaces: Python-first vs SQL-first

Biofilter supports two complementary query styles:

* **Python-first queries**, using the Biofilter query API and SQLAlchemy models.
* **SQL-first queries**, allowing raw SQL execution for advanced or highly
  customized use cases.

Both approaches operate on the same schema and return analysis-friendly outputs,
typically as pandas DataFrames.

---

## Quick Lookups with `bf.query.get()`

For common lookups and interactive exploration, `bf.query.get()` provides a
simple and expressive entry point.

Using a model name (string):

```python
df = bf.query.get("GeneMaster", symbol="A1BG")
```

Using a model class:

```python
df = bf.query.get(
    bf.query.VariantSNP,
    chromosome="1",
    position_38=175292543,
)
```

This pattern is ideal for sanity checks, validation, and exploratory workflows.

---

## Model-driven Queries with SQLAlchemy

For more expressive queries involving filters, joins, or projections, Biofilter
exposes SQLAlchemy models through the query interface.

```python
genes = bf.query.GeneMaster
select = bf.query.select

stmt = (
    select(genes)
    .where(genes.symbol == "A1BG")
    .limit(10)
)

df = bf.query.run_model(stmt)
```

This approach enables composable query construction while preserving schema
awareness and long-term maintainability.

---

## Example Notebooks

* 📓 **Schema Explorer** (`schema_explorer.ipynb`)
  Demonstrates model discovery, column inspection, and relationship navigation.

* 📓 **Query 101** (`query__101.ipynb`)
  Introduction to discovering, running, and inspecting query methods.

---

## Example: Querying Gene Aliases (Model-driven)

The following example shows how to retrieve gene aliases by joining a domain
model (`GeneMaster`) with the entity-level alias model (`EntityAlias`). This
illustrates a common Biofilter query pattern:

> Start from a domain-specific master table, join to the entity layer, and return
> an analysis-ready result.

```python
GeneMaster = bf.query.GeneMaster
Alias = bf.query.EntityAlias

stmt = (
    bf.query.select(
        GeneMaster.symbol.label("gene symbol"),
        Alias.xref_source.label("source"),
        Alias.alias_value.label("alias"),
        Alias.alias_type.label("alias type"),
        Alias.is_primary.label("primary alias"),
    )
    .select_from(GeneMaster)
    .join(Alias, Alias.entity_id == GeneMaster.entity_id)
    # .where(Alias.is_primary.is_(True))  # uncomment to keep only primary aliases
    .limit(8)
)

df = bf.query.run_model(stmt)
```

---

## Common Variations

Filter only primary aliases:

```python
stmt = stmt.where(Alias.is_primary.is_(True))
```

Filter for a specific gene:

```python
stmt = stmt.where(GeneMaster.symbol == "A1BG")
```

Order results (e.g. primary aliases first):

```python
stmt = stmt.order_by(
    Alias.is_primary.desc(),
    Alias.alias_type,
    Alias.alias_value
)
```

---

## Running Raw SQL Queries

For maximum flexibility, Biofilter also allows direct execution of raw SQL.

```python
sql = """
SELECT gm.symbol, el.chromosome, el.start_pos, el.end_pos
FROM gene_masters gm
LEFT JOIN entity_locations el
  ON el.entity_id = gm.entity_id
WHERE gm.symbol = 'A1BG'
  AND el.build = 38
LIMIT 100;
"""
df = bf.query.run_sql(sql)
```

Raw SQL is appropriate when:

* prototyping complex joins,
* debugging ingestion or indexing issues,
* reproducing legacy queries,
* generating highly customized outputs.

📓 Full multi-join SQL examples are available in `query__101.ipynb`.

---

## Exploring the Schema Interactively

Biofilter exposes its schema through the query layer, allowing users to
introspect available models and fields.

```python
bf.query.list_models()
```

Combined with SQLAlchemy inspection, this enables a deeper understanding of how
biological domains are represented and connected.

📓 See: `schema_explorer.ipynb`

---

## Models vs Tables

In Biofilter 4:

* a **table** refers to the physical structure stored in the database,
* a **model** is the Python representation of that table exposed through the
  query API.

Models define fields, relationships, and metadata, and serve as the primary
interface for querying data programmatically.

When using the query module, users interact with **models**, not raw tables.
This abstraction provides schema awareness, relationship traversal, and safer
query construction, while still executing efficiently against the underlying
database.

> In short: **tables store the data; models provide the interface used to query
> and explore it**.

📓 *Query__run_model__vs__get*
Explains the difference between running a composed model query and using the
`get()` helper.

---

## Summary

Queries provide powerful and flexible access to the Biofilter knowledge base
using both Python-first and SQL-first workflows. They enable deep exploration of
biological relationships, validation of data ingestion, and development of
custom logic—complementing the standardized and reusable workflows provided by
reports.
