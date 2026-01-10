# Installation & Setup

Biofilter 4 is designed to be easy to install and flexible to deploy across
different environments, ranging from local workstations to shared HPC systems
and production servers.

At a minimum, Biofilter 4 requires a supported Python environment and access
to either a shared knowledge database (*Knowledge as a Service*, KaaS) or a
local database backend.

---

## Requirements

- **Python:** 3.10 or newer  
- **Operating systems:**  
  - Linux  
  - macOS  
  - Windows (via WSL recommended)

### Database Backend (optional)

- **SQLite** (default, no additional setup required)
- **PostgreSQL** (recommended for production deployments)

---

## Installing Biofilter 4

Biofilter 4 can be installed directly from PyPI using `pip`:

```bash
pip install biofilter
````

For environments that require user-level installation (e.g. shared HPC
systems):

```bash
pip install --user biofilter
```

If you are working inside a virtual environment (recommended):

```bash
python -m venv biofilter-env
source biofilter-env/bin/activate
pip install biofilter
```

---

## Verifying the Installation (CLI)

Biofilter 4 provides a command-line interface (CLI) that serves as the primary
entry point for configuring projects, managing databases, running ETL
pipelines, and executing reports.

After installation, running the `biofilter` command without arguments displays
the available options and commands:

```bash
biofilter
```

Example output:

```text
Usage: biofilter [OPTIONS] COMMAND [ARGS]...
  Biofilter 4 CLI - Omics Knowledge Platform
  📚 Docs: https://xxxxxxxx

Options:
  --db-uri TEXT  Database URI (or set in .biofilter.toml)
  -V, --version  Show the version and exit.
  --help         Show this message and exit.

Commands:
  config     Configuration inspection and helpers.
  conflicts  Curation conflicts import/export helpers.
  etl        Run and manage ETL pipelines.
  index      Index management (drop/create/rebuild).
  project    Project-level operations (setup, migration, metadata).
  report     Run and manage reports.

Active DB: <not set> (use --db-uri or .biofilter.toml)
```

This output confirms that the Biofilter CLI is available and highlights the
main functional areas of the platform.

---

## Version Check

You can verify the installed Biofilter version at any time using:

```bash
biofilter --version
```

Example output:

```text
biofilter 4.0.0
DB: <not set> (use --db-uri or .biofilter.toml)
```

The version command also reports whether an active database connection is
configured.

---

## Database Configuration and Installation Notes

At installation time:

* No database is configured by default.
* No databases are automatically created or downloaded.

Biofilter 4 requires an explicit database connection to be defined before it
can be used.

A database URI can be provided either:

* via the `--db-uri` command-line option, or
* through a project-level configuration file (`.biofilter.toml`).

This design keeps Biofilter 4 lightweight at install time while supporting
flexible deployment models. Users may either:

* connect to an existing shared central knowledge database (*Knowledge as a
  Service*, KaaS), or
* create and manage a local Biofilter database for custom or isolated
  workflows.

Configuring a database connection—whether to a central KaaS deployment or to a
local database—is covered in the next sections of the User Guide.

---

## Optional Dependencies

Depending on your deployment and usage, additional dependencies may be useful:

* PostgreSQL client libraries (for connecting to a remote database)
* Scientific Python packages (e.g. `pandas`, `numpy`) for downstream analysis
  and notebooks

These are not required for basic installation but may be helpful for advanced
workflows.

---

## Next Steps

Once Biofilter 4 is installed, users can proceed to:

* **Connecting to a Knowledge Base** – to use a centrally managed Biofilter
  database (KaaS), or
* **Creating a Local Database** – to initialize and manage a standalone
  Biofilter database.



