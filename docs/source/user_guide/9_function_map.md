# Biofilter 4 – API Map

This document provides a high-level and detailed map of the **public and internal APIs**
of Biofilter 4, reflecting the current **Core + Components + Modules** architecture.

Its goals are:
- Provide a single mental model of the system
- Serve as a developer reference
- Act as a basis for CLI help and documentation
- Reduce friction when adding new features

---

## 1. High-Level Architecture

```

Biofilter (Facade)
│
├── DBComponent        → Database
├── SettingsComponent  → SettingsManager → SystemConfig
├── ETLComponent       → ETLManager → DTPs
├── ConflictsComponent → ConflictManager
└── ReportComponent    → ReportManager → ReportBase subclasses

````

**Design rules**
- `Biofilter` exposes the public API
- Components validate, open sessions, and orchestrate
- Managers implement logic
- Modules (DTPs, Reports) contain domain-specific behavior
- Sessions are **never stored globally**
- Each operation opens and closes its own session safely

---

## 2. Public API – Biofilter Facade

### 2.1 Database (`bf.db`)

```python
bf.db.connect(new_uri: Optional[str] = None) -> Database
bf.db.create(db_uri: Optional[str] = None, overwrite: bool = False) -> bool
bf.db.migrate() -> bool
bf.db.get_session() -> contextmanager[Session]
````

Responsibilities:

* Database lifecycle
* Connection management
* Schema creation and migration

---

### 2.2 Settings (`bf.settings`)

```python
bf.settings.get(key: str, default=None)
bf.settings.require(key: str)
bf.settings.set(key: str, value)
bf.settings.refresh(key: Optional[str] = None)
```

Backed by:

* `SystemConfig` table
* `SettingsManager`

---

### 2.3 ETL (`bf.etl`)

```python
bf.etl.update(
    source_system: Optional[list[str]] = None,
    data_sources: Optional[list[str]] = None,
    run_steps: Optional[list[str]] = None,
    force_steps: Optional[list[str]] = None,
    use_conflict_csv: bool = False,
)

bf.etl.restart_etl(
    data_source: Optional[list[str]] = None,
    source_system: Optional[list[str]] = None,
    delete_files: bool = False,
)

bf.etl.rebuild_indexes(
    groups: Optional[list[str]] = None,
    drop_only: bool = False,
    drop_first: bool = True,
    set_write_mode: bool = True,
    set_read_mode: bool = True,
)
```

Notes:

* One session per datasource execution
* One `ETLPackage` per phase (extract / transform / load)
* Hash-based cache control
* Safe for SQLite and PostgreSQL

---

### 2.4 Conflicts (`bf.conflicts`)

```python
bf.conflicts.export_to_excel(output_path: str = "curation_conflicts.xlsx")
bf.conflicts.import_from_excel(input_path: str = "curation_conflicts_template.xlsx")
```

Future planned:

```python
bf.conflicts.resolve_all()
bf.conflicts.process_resolved_conflicts()
```

---

### 2.5 Reports (`bf.reports`)

```python
bf.reports.list(verbose: bool = True)
bf.reports.run(identifier: str, **kwargs)
bf.reports.run_example(identifier: str, **kwargs)

bf.reports.explain(identifier: str, print_output: bool = True)
bf.reports.example_input(identifier: str, print_output: bool = True)
bf.reports.available_columns(identifier: str, print_output: bool = True)

bf.reports.refresh()
bf.reports.get_report_class(identifier: str)
```

Identifiers accepted:

* Module name (`report_gene_to_snp`)
* Friendly name (`GeneToSnp`)
* Class name (`GeneToSnpReport`)

---

## 3. Internal API – Components and Managers

### 3.1 DBComponent

* Owns the **single active Database instance**
* Ensures core tables and mappings are consistent
* Exposes `get_session()` factory

Backed by:

* `modules.db.database.Database`

---

### 3.2 SettingsComponent

* Thin wrapper over `SettingsManager`
* Lazy initialization
* Cache-aware

Backed by:

* `SettingsManager`
* `SystemConfig`

---

### 3.3 ETLComponent

* Delegates to `ETLManager`
* Validates DB availability
* Does not contain ETL logic

Backed by:

* `ETLManager`
* `ETLPackage`, `ETLDataSource`, `ETLSourceSystem`
* DTP modules (`modules.etl.dtps.*`)

---

### 3.4 ReportComponent

* Facade over `ReportManager`
* Manages session lifecycle
* Print vs return handled here (not in manager)

Backed by:

* `ReportManager`
* `ReportBase` subclasses

---

### 3.5 ReportManager

Responsibilities:

* Discover report modules
* Cache report classes
* Resolve identifiers
* Run reports with session safety
* No printing, no global state

---

## 4. Extension Points

### Add a new ETL source

* Create new DTP in `modules.etl.dtps`
* Register `ETLDataSource` in DB
* No change required in `Biofilter` or `ETLComponent`

### Add a new Report

* Create `report_xxx.py` in `modules.report.reports`
* Subclass `ReportBase`
* Implement:

  * `run()`
  * `explain()`
  * `example_input()`
  * `available_columns()`

---

## 5. Common Execution Flows

### ETL Full Pipeline

```python
bf = Biofilter("sqlite:///biofilter.db")
bf.db.connect()

bf.etl.update(
    data_sources=["dbsnp"],
    run_steps=["extract", "transform", "load"]
)
```

### Run a Report

```python
bf = Biofilter("postgresql://...")
bf.db.connect()

bf.reports.run("GeneToSnp", input_data=[...])
```

---

## 6. Guiding Principles

* Facade exposes behavior, not implementation
* Components orchestrate, managers execute
* No component stores open sessions
* Reports and ETL are safe for PostgreSQL (no idle transactions)
* Architecture favors clarity over cleverness

---
