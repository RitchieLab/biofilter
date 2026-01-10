# Configuration & Logging

Biofilter 4 provides a structured configuration system and a transparent logging
mechanism to ensure reproducibility, operational visibility, and ease of use
across local, HPC, and cloud environments.

- **Configuration** defines how Biofilter runs.
- **Logs and ETL metadata** explain what happened and when.

Together, they form the operational backbone of the platform.

---

## Configuration Overview

Biofilter 4 uses a project-level configuration file named `.biofilter.toml` to
store default runtime settings.

This file allows Biofilter to operate consistently across commands without
repeatedly specifying CLI flags or environment variables.

Configuration may include:

- database connection settings,
- ETL execution parameters,
- logging behavior,
- performance-related options.

Using a configuration file is the recommended approach for:

- long-running projects,
- shared or collaborative environments,
- reproducible workflows,
- cloud and HPC deployments.

---

## The `.biofilter.toml` File

A minimal configuration file may look like:

```toml
[database]
db_uri = "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter"
echo_sql = false         # optional, useful for debugging
auto_create = false      # create DB if it does not exist

[etl]
data_root = "./biofilter_data"
allow_parallel = true
max_workers = 8

[reports]
default_output_format = "dataframe"  # csv | parquet | dataframe
warn_on_empty = true

[logging]
level = "INFO"            # DEBUG | INFO | WARNING | ERROR | CRITICAL
log_to_file = true
log_file = "./biofilter.log"
````

Once defined, these settings are automatically applied to all Biofilter commands
unless explicitly overridden.

---

## Managing Configuration via CLI

Biofilter provides CLI helpers to inspect and manage configuration values without
manually editing files.

### Initialize a Configuration File

```bash
biofilter config init
```

Creates a default `.biofilter.toml` file in the current project directory.

### Inspect Active Configuration

```bash
biofilter config show
```

Displays the resolved configuration, including values loaded from
`.biofilter.toml` and any runtime overrides.

### Set and Get Configuration Values

```bash
biofilter config set database.db_uri sqlite:///biofilter.db
biofilter config get database.db_uri

biofilter config set etl.max_workers 16
biofilter config get etl.max_workers

biofilter config set logging.level DEBUG
biofilter config get logging.level
```

Changes are written directly to `.biofilter.toml` and affect subsequent
executions.

---

## CLI Overrides vs Configuration File

Command-line options always take precedence over configuration file values.

Example:

```bash
biofilter --db-uri sqlite:///temp.db report run ...
```

This override applies only to that command and does **not** modify
`.biofilter.toml`.

This design allows:

* stable defaults via configuration files,
* temporary overrides for testing or experimentation.

---

## Logging System

Biofilter generates structured logs during execution to provide visibility into:

* ETL execution steps,
* warnings and errors,
* performance-related events,
* configuration resolution.

By default, logs are written to:

```text
biofilter.log
```

in the execution directory.

The logging level can be adjusted via configuration:

```toml
[logging]
level = "DEBUG"
```

Supported levels include: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`.

---

## Logs vs ETL Metadata

Logs and ETL metadata serve complementary but distinct roles in Biofilter 4.

### Execution Logs

Logs are written to a log file (e.g. `biofilter.log`) and provide a
high-granularity, chronological view of what happens during execution.

They are primarily intended for:

* detailed debugging,
* inspecting internal execution steps,
* tracking warnings and recoverable issues,
* identifying data conflicts or schema inconsistencies,
* diagnosing unexpected errors or performance bottlenecks.

Logs are **ephemeral** by nature. They reflect what happened during a specific
execution and may be rotated, archived, or discarded depending on the
environment.

---

### ETL Packages (Persistent Metadata)

ETL Packages are persistent records stored in the database that represent each
ETL execution as a first-class object.

An ETL Package captures high-level, authoritative metadata, including:

* source system and data source,
* execution timestamps,
* execution status (success, failed, partial),
* number of records processed or added,
* references to incremental or full loads,
* a structured JSON field capturing final errors, warnings, or summary issues.

Unlike logs, ETL Packages are **durable and queryable**, forming the official
audit trail of how and when knowledge entered the system.

---

## How Logs and ETL Packages Work Together

Logs and ETL Packages are designed to work together:

* **Logs** explain *how* something happened, step by step.
* **ETL Packages** record *what* happened, at a summarized and authoritative
  level.

In practice:

* users rely on ETL reports (e.g. `etl_status`, `etl_packages`) for monitoring,
  auditing, and reproducibility;
* logs are consulted when deeper investigation or debugging is required.

This separation allows Biofilter 4 to provide both operational transparency and
long-term provenance tracking without overloading the database with low-level
execution noise.

---

## Monitoring ETL Through Reports

Biofilter exposes ETL execution state through dedicated reports.

### ETL Status

```python
df = bf.report.run(
    "etl_status",
    source_system="ncbi",
)
```

Provides a summary view of current and recent ETL activity.

### ETL Package History

```python
df = bf.report.run(
    "etl_packages",
    source_system="ncbi",
)
```

Lists individual ETL packages with timestamps, execution status, and provenance.

These reports allow users to:

* verify whether data is up to date,
* identify failed or partial loads,
* correlate knowledge updates with downstream analyses.

---

## Summary

Configuration and logging in Biofilter 4 are first-class features designed to
support reproducibility, operational clarity, and scalable deployment.

By combining project-level configuration, structured logs, and persistent ETL
metadata, Biofilter provides a robust foundation for both exploratory research
and production-grade knowledge management.

