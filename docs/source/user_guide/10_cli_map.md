# Biofilter 4 CLI — Command Map

This document provides a complete overview of all Biofilter 4 CLI commands,
their options, and how they map to internal components.

---

## Global

### `biofilter`
Shows help and indicates the active database (resolved via `--db-uri` or `.biofilter.toml`).

```bash
biofilter
````

### Global options

* `--db-uri TEXT`
  Define the database URI globally (can be overridden per command).
* `--debug`
  Enable debug logging (if supported by the command).
* `--version`, `-V`
  Print version and resolved DB and exit.

Examples:

```bash
biofilter --version
biofilter --db-uri sqlite:///./dev.db etl update --data-source dbsnp_sample
```

---

## `config` — Manage `.biofilter.toml`

### `biofilter config init`

Create a `.biofilter.toml` template.

```bash
biofilter config init --path .
biofilter config init --path . --force
biofilter config init --db-uri sqlite:///./biofilter.db
biofilter config init --data-root ./biofilter_data
```

### `biofilter config get`

Retrieve a configuration value using `SECTION.KEY`.

```bash
biofilter config get database.db_uri
biofilter config get etl.data_root
```

### `biofilter config set`

Set a configuration value using `SECTION.KEY VALUE`
(type is inferred automatically).

```bash
biofilter config set database.db_uri sqlite:///./biofilter.db
biofilter config set etl.max_workers 12
biofilter config set etl.allow_parallel true
biofilter config set logging.level DEBUG
```

---

## `project` — Project & database lifecycle

### `biofilter project create`

Create a new database and bootstrap schema.

```bash
biofilter project create --db-uri sqlite:///./biofilter.db
biofilter project create --db-uri sqlite:///./biofilter.db --overwrite
```

### `biofilter project migrate`

Run database migrations.

```bash
biofilter --db-uri sqlite:///./biofilter.db project migrate
# or if db_uri is defined in .biofilter.toml
biofilter project migrate
```

---

## `etl` — Run & manage ETL pipelines

### `biofilter etl update`

Run Extract → Transform → Load for selected sources.

```bash
biofilter etl update --source-system NCBI
biofilter etl update --data-source dbsnp_sample
```

Run specific steps:

```bash
biofilter etl update --data-source dbsnp_sample --run-step extract
biofilter etl update --data-source dbsnp_sample --run-step transform --run-step load
```

Force steps:

```bash
biofilter etl update --data-source dbsnp_sample --force-step extract
biofilter etl update --data-source dbsnp_sample --force-step load
```

With global DB URI:

```bash
biofilter --db-uri sqlite:///./biofilter.db etl update --data-source dbsnp_sample
```

---

### `biofilter etl restart`

Restart ETL execution and optionally remove files.

```bash
biofilter etl restart --data-source dbsnp_sample
biofilter etl restart --source-system NCBI
biofilter etl restart --data-source dbsnp_sample --delete-files
```

---

### `biofilter etl update-conflicts`

Run ETL using conflict-resolution CSV logic.

```bash
biofilter etl update-conflicts --source-system NCBI
```

---

## `index` — Index management

### `biofilter index rebuild`

Drop and/or create indexes.

```bash
# rebuild all index groups
biofilter index rebuild

# rebuild specific groups
biofilter index rebuild --group gene --group variant
biofilter index rebuild --group entity --group protein
```

Drop-only:

```bash
biofilter index rebuild --drop-only
```

Tuning flags:

```bash
biofilter index rebuild --no-write-mode
biofilter index rebuild --no-read-mode
biofilter index rebuild --no-drop-first
```

---

## `conflicts` — Curation conflict helpers

### `biofilter conflicts export-excel`

Export conflicts to Excel.

```bash
biofilter conflicts export-excel
biofilter conflicts export-excel --output curation_conflicts.xlsx
```

### `biofilter conflicts import-excel`

Import conflicts from Excel.

```bash
biofilter conflicts import-excel
biofilter conflicts import-excel --input curation_conflicts_template.xlsx
```

---

## `report` — Run and manage reports

### `biofilter report list`

List available reports.

```bash
biofilter report list
```

---

### `biofilter report explain`

Show report explanation.

```bash
biofilter report explain --name GeneToSnpReport
# or by module name
biofilter report explain --name report_gene_to_snp
```

---

### `biofilter report example-input`

Show example input for a report.

```bash
biofilter report example-input --name GeneToSnpReport
```

---

### `biofilter report available-columns`

Show available output columns.

```bash
biofilter report available-columns --name GeneToSnpReport
```

---

### `biofilter report run`

Run a report.

```bash
biofilter report run --name GeneToSnpReport
```

Export to CSV:

```bash
biofilter report run --name GeneToSnpReport --as-csv --output out.csv
```

---

## Command → Component Routing (Architecture Map)

| CLI Group   | Biofilter Component | Internal Responsibility       |
| ----------- | ------------------- | ----------------------------- |
| `project`   | `bf.db`             | Database creation & migration |
| `etl`       | `bf.etl`            | ETL execution & control       |
| `index`     | `bf.etl`            | Index creation & tuning       |
| `conflicts` | `bf.conflicts`      | Conflict import/export        |
| `report`    | `bf.reports`        | Report discovery & execution  |
| `config`    | CLI-only            | `.biofilter.toml` management  |

---

## Mental Model

* **Biofilter** is a *facade*
* **Components** own behavior (`ETLComponent`, `ReportComponent`, etc.)
* **CLI** only:

  * resolves DB
  * parses arguments
  * delegates execution

No business logic lives in the CLI.

---

