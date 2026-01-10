# Connect to a Database

Biofilter 4 is designed to work against an existing knowledge base, allowing users
to immediately query curated biological knowledge without managing local data
ingestion when a shared database is available. This deployment model is referred
to as **Knowledge as a Service (KaaS)**.

In this mode, Biofilter 4 connects to a centrally managed database where biological
knowledge is curated, versioned, and maintained independently of individual
analyses.

---

## Knowledge as a Service (KaaS)

When using a shared Biofilter knowledge base, users do **not** need to:

- create a database schema,
- download external data sources,
- or run ETL pipelines.

Instead, users simply configure a database connection and begin working with an
existing, curated knowledge base.

---

## Database Connection URLs (DB URI)

Biofilter 4 uses standard **SQLAlchemy-style database URLs** to define how it
connects to a database backend. The format of the database URI depends on the
database engine being used.

---

### SQLite (local, lightweight)

SQLite is the simplest option and requires no additional services.

```text
sqlite:///path/to/biofilter.db
````

Examples:

```text
sqlite:///biofilter.db
sqlite:////full/path/to/biofilter.db
```

Recommended for:

* local development,
* testing,
* isolated or exploratory workflows.

---

### PostgreSQL (production / shared environments)

PostgreSQL is recommended for shared or production deployments.

```text
postgresql+psycopg2://username:password@host:port/database
```

Example:

```text
postgresql+psycopg2://biouser:biokey@localhost:5432/DB
```

---

### PostgreSQL Driver Requirement

When using PostgreSQL, Biofilter 4 relies on a PostgreSQL driver.
The recommended driver is **psycopg2**.

If you encounter connection errors related to the PostgreSQL engine, make sure the
PostgreSQL client libraries are installed on your system.

Example (Linux / HPC environments):

```bash
pip install psycopg2-binary
```

In some environments, system-level PostgreSQL client libraries may also be required.
Consult your system administrator if needed.

---

## Providing the Database URI

The database URI can be provided either via the **command line** or through a
**project configuration file**.

---

### Option 1: Command-Line Option

```bash
biofilter --db-uri postgresql+psycopg2://{username}:{password}@localhost:5432/{dbname}
```

This approach is useful for:

* starting a new project,
* validating database connectivity,
* running ETL pipelines against a specific database,
* executing queries or reports without persistent configuration.

This option is particularly convenient for interactive sessions, automation
scripts, and environments where project-level configuration files are not practical.

---

### Option 2: Project Configuration File (`.biofilter.toml`)

For persistent setups, Biofilter 4 supports a project-level configuration file
named `.biofilter.toml`.

This file defines default settings for database connections, ETL behavior, logging,
and other runtime options, allowing Biofilter to operate consistently across
commands without repeatedly specifying CLI flags.

Recommended for:

* long-running projects,
* shared environments,
* reproducible workflows,
* cloud and HPC deployments.

#### Example Configuration

```toml
[database]
uri = "postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}"
```

Once defined, this database connection will be used automatically by all Biofilter
commands unless explicitly overridden via `--db-uri`.

---

## Inspecting the Active Configuration

To view the resolved configuration and verify active settings:

```bash
biofilter config show
```

This command displays values loaded from `.biofilter.toml` as well as any runtime
overrides.

---

## Managing Configuration Values via CLI

Biofilter 4 allows configuration values to be read and updated directly through
the CLI.

```bash
biofilter config set database.db_uri sqlite:///x.db
biofilter config get database.db_uri
```

These commands update the `.biofilter.toml` file and immediately affect subsequent
Biofilter executions.

---

### CLI Overrides vs Configuration File

Values provided via command-line options (such as `--db-uri`) always take precedence
over values defined in `.biofilter.toml`.

This allows temporary overrides for specific commands while preserving a stable
default configuration.

---

## Remote and Cloud-Based Deployments

Biofilter 4 was designed to operate naturally in distributed and cloud-based
environments. The Biofilter application and the underlying knowledge database do
not need to run on the same machine.

Typical deployments include:

* Biofilter running on:

  * a local workstation,
  * an HPC login or compute node,
  * a container or cloud compute service,
* with the database hosted on:

  * a managed PostgreSQL service,
  * an institutional database server,
  * a cloud-based database platform.

In these scenarios, Biofilter connects to the remote database via the database URI,
fully abstracting the database location behind the connection string.

---

## Connecting via the Python API (Notebook / Script)

In addition to the CLI, Biofilter 4 provides a Python API that can be used directly
from scripts or Jupyter notebooks.

### Basic Usage

```python
from biofilter import Biofilter
bf = Biofilter()
```

When no database URI is provided explicitly, Biofilter attempts to read it from
`.biofilter.toml`.

---

### Providing the Database URI Explicitly

```python
from biofilter import Biofilter

db_uri = "postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}"
bf = Biofilter(db_uri)
```

Upon initialization, Biofilter will:

* load the configuration,
* establish a database connection,
* report connection status via logs.

---

## Verifying the Connection

You can verify the active connection via CLI:

```bash
biofilter --version
```

If a database connection is active, the output will show the configured database
instead of:

```text
DB: <not set>
```

---

## Read-Only vs Read-Write Access

In KaaS deployments, most users connect with **read-only access**, which is fully
supported for querying and reporting.

Write access is only required when:

* running ETL pipelines,
* managing indexes,
* performing administrative tasks.

---

## Next Steps

Once connected to a knowledge base, users can proceed to:

* **First Queries & Reports** – to start exploring biological knowledge, or
* **Creating a Local Database** – to initialize and manage a standalone Biofilter
  database.

