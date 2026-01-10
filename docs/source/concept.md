# Concept

Biofilter 4 adopts an **entity-centric knowledge model** that fundamentally
redefines how biological data are represented, integrated, and queried.

Rather than organizing information around predefined data types and fixed
traversal paths, Biofilter 4 models biological concepts as **persistent
entities** connected through **explicit, queryable relationships**.

This shift enables substantially greater flexibility, extensibility, and
expressive power compared to earlier versions of the Biofilter framework.

---

## From Fixed Data-Type Paths to Flexible Knowledge Navigation

In Biofilter 2 and Biofilter 3, biological knowledge was accessed through
well-defined sequences of data types, such as:


SNP → position → gene → group


This approach was effective and widely used, but it imposed a constrained view
of how biological information could be combined, extended, and explored.

Biofilter 4 removes these rigid paths.

Instead, entities are linked through explicitly modeled relationships that can
be traversed in multiple directions, depending on the analytical context.
Users are no longer restricted to a single “correct” path through the data, but
can navigate biological knowledge in ways that best fit their scientific
questions.

---

## Domain-Specific Master Data with a Shared Entity Layer

Biofilter 4 introduces the concept of **domain-specific master data**.

Each biological domain — such as genes, variants, proteins, pathways, or future
omics domains — maintains its own specialized data structures and rich
attribute sets, optimized for that domain’s needs.

At the same time, all domains are unified through a shared **Entity layer**,
where biological concepts are identified, linked, and related using knowledge
curated from specialized third-party data sources.

This design allows Biofilter 4 to support both:

- **Depth**, through rich, domain-specific representations  
- **Integration**, through a common entity and relationship framework

---

## Separation of Identity, Attributes, and Relationships

A core principle of Biofilter 4 is the separation of concerns between:

- **Entity identity**
- **Domain-specific attributes**
- **Source-specific annotations and relationships**

By decoupling these components, Biofilter 4 enables each domain to evolve
independently while preserving global consistency across the knowledge base.

New attributes or annotations can be added without redefining entity identity
or disrupting existing relationships.

This separation is essential for supporting long-term knowledge growth,
heterogeneous data sources, and evolving biological understanding.

---

## A Plug-and-Play Knowledge Platform

Biofilter 4 is designed as a **plug-and-play knowledge platform**.

New data sources, interaction types, and even entirely new biological domains
can be integrated without modifying existing schemas or workflows.

Users and developers can:

- Add new external data sources
- Define new relationships between entities
- Extend domain-specific attributes
- Create custom queries tailored to specific analytical needs

This extensibility allows Biofilter 4 to adapt naturally to new data types,
emerging research areas, and evolving analytical strategies.

---

## What This Enables in Practice

The entity-centric architecture of Biofilter 4 enables:

- Flexible navigation across biological knowledge without fixed traversal rules
- Rich, domain-specific representations with shared integration points
- Reusable and persistent biological knowledge across projects
- Custom, query-driven analysis workflows
- Long-term extensibility as new domains and data sources emerge

---

## Historical Context

Earlier versions of Biofilter organized knowledge around predefined data types
and cross-referencing rules, supported by a local knowledge database.

Biofilter 4 generalizes these original ideas into a modern, extensible knowledge
platform that emphasizes **persistent entities**, **explicit relationships**,
and **flexible user interaction**.

The foundational concepts remain, but they are now expressed through a model
designed to support contemporary data scale, diversity, and analytical
workflows.

