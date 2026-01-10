# Alembic & Schema Migrations

Biofilter 4 supports long-lived databases that evolve over time. To enable safe
and reproducible schema changes **without requiring database rebuilds**, the
platform uses **Alembic** as its schema migration framework.

This section explains how schema migrations fit into the Biofilter architecture,
when they are required, and how developers should reason about schema evolution.

---

## Why migrations are required

Unlike file-based workflows, Biofilter 4 is designed to operate as a **persistent
knowledge base**. Once biological data has been ingested and curated, rebuilding
the database from scratch is often undesirable or impractical.

Schema migrations allow developers to:

- introduce new tables or columns,
- extend existing domain models,
- evolve ETL metadata structures,
- support new features without losing data,
- upgrade production or shared databases safely.

Migrations are therefore a **core part of Biofilter’s lifecycle**, not an
afterthought.

---

## Migrations vs seeds

It is important to distinguish schema migrations from seed data updates.

### Alembic migrations

- modify database structure (tables, columns, indexes, constraints),
- apply structural changes,
- are versioned and ordered.

### Seeds

- populate or update reference metadata,
- do **not** alter schema structure,
- are idempotent and content-focused.

In practice, migrations and seeds often evolve together:  
a migration introduces a new table or column, and a seed populates initial
reference values.

---

## When to create a migration

Developers should create an Alembic migration when:

- adding a new table or relationship,
- adding or modifying a column,
- introducing new constraints or indexes,
- changing ETL metadata structures,
- evolving domain schemas in a backward-compatible way.

Migrations are **not required** for:

- ingesting new biological data,
- updating seed values only,
- running ETL pipelines.

---

## Migration design principles

Biofilter 4 follows conservative migration guidelines to preserve existing
knowledge:

- prefer additive changes (new tables or columns),
- avoid destructive operations whenever possible,
- preserve entity identifiers and relationships,
- ensure migrations are reversible when feasible,
- maintain compatibility across supported backends (SQLite and PostgreSQL).

Breaking changes should be **rare and explicitly documented**.

---

## Handling existing data safely

Because Biofilter stores persistent biological knowledge, migrations must assume
that:

- entities and relationships already exist,
- multiple DTPs may have contributed to the same records,
- ETL packages may reference historical schema states.

For this reason:

- data migration logic should be explicit,
- default values should be carefully chosen,
- nullability and backfilling should be planned.

Schema changes should **never silently invalidate** previously ingested
knowledge.

---

## Migrations in shared and local databases

Alembic migrations apply equally to:

- shared central databases (KaaS deployments),
- local standalone databases,
- development and testing environments.

In shared environments, migrations should be:

- reviewed,
- tested against representative datasets,
- applied in a controlled manner.

---

## Developer workflow overview

A typical schema evolution workflow looks like:

1. Update or add SQLAlchemy models in `db/models/`.
2. Generate an Alembic migration reflecting the changes.
3. Review and adjust the migration script.
4. Apply the migration to target databases.
5. *(Optional)* Update seed data if new reference metadata is required.

This workflow ensures that schema changes are **explicit, auditable, and
reproducible**.

---

## Database migrations with Alembic

Biofilter 4 uses Alembic as its database migration framework.
Alembic enables controlled, versioned evolution of the database schema without
requiring database recreation or data loss.

This is a critical component of Biofilter’s design, as the knowledge base is
intended to be persistent, incrementally extended, and continuously updated over
time.

---

## What is Alembic?

Alembic is a lightweight database migration tool built on top of SQLAlchemy.
It allows developers to:

- track schema changes over time,
- apply upgrades and downgrades incrementally,
- synchronize database structure across environments,
- evolve schemas without reinitializing databases.

In Biofilter 4, Alembic is used to manage:

- table additions and modifications,
- index creation and restructuring,
- constraint updates,
- schema extensions for new domains.

---

## Why Alembic matters in Biofilter 4

Biofilter 4 is designed around a persistent knowledge base where:

- entities and relationships accumulate across multiple ETL executions,
- data sources are updated independently,
- multiple teams may share the same database.

Recreating the database on every schema change would:

- destroy curated knowledge,
- break ETL provenance,
- invalidate downstream analyses.

Alembic allows Biofilter to evolve **without resetting biological knowledge**.

---

## Alembic directory structure

Within the Biofilter repository, Alembic is located at:

```text
biofilter/
└── alembic/
    ├── versions/        # Individual migration scripts
    ├── env.py           # Alembic runtime configuration
    └── script.py.mako   # Migration template
````

Key files:

* `alembic.ini` – global Alembic configuration
* `env.py` – connects Alembic to Biofilter’s SQLAlchemy metadata
* `versions/` – ordered migration files (revision history)

---

## How Alembic integrates with Biofilter

Biofilter’s SQLAlchemy models (in `biofilter.db.models`) are the **single source
of truth** for schema definitions.

Alembic is configured to:

* load Biofilter’s metadata,
* compare model definitions against the live database,
* generate migration scripts reflecting those differences.

This ensures migrations stay aligned with the actual data model.

---

## Common Alembic workflows

### Checking current database version

```bash
alembic current
```

Shows the current migration revision applied to the database.

---

### Creating a new migration

After modifying or adding models:

```bash
alembic revision --autogenerate -m "add disease groups"
```

This command:

* inspects model changes,
* generates a new migration file,
* places it in `alembic/versions/`.

⚠️ **Always review autogenerated migrations before applying them.**

---

### Applying migrations (upgrade)

```bash
alembic upgrade head
```

Applies all pending migrations to bring the database to the latest schema
version.

---

### Rolling back a migration (downgrade)

```bash
alembic downgrade -1
```

Reverts the most recent migration.

This is useful during development but should be used cautiously in shared
environments.

---

## Alembic vs database initialization

It is important to distinguish between:

### Database initialization

* creates a new database,
* loads schema + seed data,
* no prior data exists.

Typically done via:

```bash
biofilter project create
```

### Alembic migrations

* modify an existing database,
* preserve data and ETL history,
* incrementally evolve schema.

Alembic is **not used during initial database creation**, but becomes essential
afterward.

---

## Best practices for Biofilter migrations

* Always version-control migration files
* Keep migrations small and focused
* Avoid destructive operations unless explicitly intended
* Test migrations on a development database first
* Coordinate migrations carefully in shared or production environments

---

## Alembic in shared and production deployments

In shared databases (KaaS or team-level deployments):

* schema changes must be coordinated,
* migrations should be reviewed and tested,
* upgrades should be scheduled.

Alembic enables safe, auditable schema evolution across:

* local development,
* HPC clusters,
* cloud-hosted databases.

---

## Summary

Alembic is a foundational tool in Biofilter 4 that enables:

* persistent knowledge bases,
* safe schema evolution,
* long-lived databases shared across teams,
* iterative system growth without data loss.

By combining Alembic with Biofilter’s entity-centric architecture and ETL
provenance model, Biofilter 4 supports continuous biological knowledge
integration at scale.


