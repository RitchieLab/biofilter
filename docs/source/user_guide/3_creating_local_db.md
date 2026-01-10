# Create a Database

Creating a local Biofilter database is optional and only required when a shared or
pre-existing knowledge base is not available. In most deployments, users connect
to an existing Biofilter database and do not need to create or manage their own
database instance.

A new database may be created either as:

- a centrally managed database shared by a team or institution, or
- a standalone local database hosted on a single machine for isolated workflows.

---

## When to Create a Local Database

Creating a local Biofilter database is recommended when:

- no shared knowledge base is available,
- custom or experimental data sources must be integrated,
- workflows require full write access and schema control,
- development, testing, or offline execution is needed.

Local databases are typically created using **SQLite**, but **PostgreSQL** can also
be used when local scalability or concurrency is required.

---

## What to Expect from a New Database

A newly created Biofilter database starts **empty by design**, containing only the
minimal structures required for operation.

No biological knowledge is available until data is explicitly ingested.

At creation time, only **cold-start seed data** are inserted, such as:

- reference metadata,
- required system tables,
- internal configuration records.

These seeds establish the structural foundation of the database but do **not**
include curated biological content.

For details on seed data, see **Database Seeds** in the Technical Database
Documentation.

---

## Initializing a New Local Database

Biofilter 4 provides project-level helpers to initialize a new database and prepare
it for use.

---

### Using the CLI (SQLite)

```bash
biofilter project create --db-uri sqlite:///./biofilter.db --overwrite
````

---

### Using the Python API

```python
from biofilter import Biofilter

bf = Biofilter()
bf.create_new_project("sqlite:///./biofilter.db", overwrite=True)
```

This operation:

* creates the database file if it does not exist,
* initializes the Biofilter schema,
* associates the current project with the newly created database.

After initialization, the database can be reused across all Biofilter commands
within the project.

---

## Using a Local PostgreSQL Database

For users who prefer or require PostgreSQL locally, the same workflow applies by
providing a PostgreSQL database URI:

```bash
biofilter project create \
  --db-uri postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}
```

The initialization logic is identical regardless of the backend.

---

## Central vs Standalone Deployments

The same initialization process can be used to create:

* a **central database** intended to be shared across users or projects, or
* a **standalone database** used locally on a single machine.

The difference lies in how the database is hosted and accessed, **not** in how it
is created or initialized.

---

## Persisting the Database Configuration

Once a local database is initialized, Biofilter 4 can store the database connection
in the project configuration file (`.biofilter.toml`).

When a configuration file is present, Biofilter automatically reuses the configured
database connection for subsequent commands.

For details, see **User Guide → Configuration & Logs**.

---

## Local Database Lifecycle

A local Biofilter database can be:

* incrementally populated using ETL pipelines,
* modified or extended with additional data sources,
* inspected and queried via the CLI or Python API,
* archived, migrated, or rebuilt as needed.

This flexibility makes local databases well suited for exploratory research,
development, and custom workflows.

---

## Next Steps

After creating a local database, users can proceed to:

* **Running ETL Pipelines** – to ingest external or custom data sources, or
* **First Queries & Reports** – to begin exploring the knowledge base.

---

## Further Technical Details

For a deeper understanding of the database structure—including:

* tables and schemas,
* seed data,
* data models,
* indexing strategies,
* internal system metadata,

please refer to the **Technical Database Documentation**:

> *Link to be added*

