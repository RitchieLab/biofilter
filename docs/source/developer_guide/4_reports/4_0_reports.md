# Reports

## Report architecture overview

At a high level, the report system consists of:

- **ReportManager**  
  Central orchestrator responsible for discovering, registering, and executing
  reports.

- **BaseReport**  
  Abstract base class that defines the contract all reports must follow.

- **Individual report modules**  
  Self-contained Python files implementing specific biological queries.

Reports are designed to:

- abstract database complexity from users,
- enforce standardized output schemas,
- be reusable across projects and teams,
- work identically via API and (future) CLI.

---

## Where reports live

All reports must live inside the `report` package:

```text
biofilter/
└── report/
    ├── report_manager.py
    └── reports/
        ├── base_report.py
        ├── report_template.py
        ├── report_gene_to_snp.py
        ├── report_etl_status.py
        ├── report_etl_packages.py
        └── ...
````

### Key rules

* Each report lives in its **own file**.
* Files must be placed under `biofilter/report/reports/`.
* Reports outside this folder are **not discovered automatically**.

---

## Naming and discovery conventions

Biofilter relies on simple conventions for report discovery.

### File naming

* Must start with `report_`

Example:

```text
report_gene_to_snp.py
```

### Report identifier

Each report defines a unique `name` attribute:

```python
name = "gene_to_snp"
```

This is the string users pass to:

```python
bf.report.run("gene_to_snp", ...)
```

### Class naming

* Conventionally `<Something>Report`
* Not strictly enforced

Discovery is based on the `name` attribute, not the class name.

Following these conventions ensures the `ReportManager` can automatically
register the report and expose it via:

```python
bf.report.list()
```

---

## The BaseReport contract

All reports must inherit from `BaseReport`.

At minimum, a report must:

* implement a `run()` method,
* execute queries using `self.session` (SQLAlchemy session),
* return a `pandas.DataFrame`.

The report instance automatically receives:

* a database session,
* validated parameters passed via `bf.report.run(...)`,
* logging utilities.

A report must **never**:

* open its own database connection,
* manage transactions manually,
* return raw ORM objects.

---

## Minimal report template

New reports should start from the provided template:

```text
report_template.py
```

A minimal report looks like this:

```python
import pandas as pd
from biofilter.report.reports.base_report import ReportBase

class MyReport(ReportBase):
    name = "my_report"
    description = "Short description of the biological question answered."

    def run(self) -> pd.DataFrame:
        query = (
            self.session.query(
                # ORM columns here
            )
            # .join(...)
            # .filter(...)
        )

        rows = query.all()

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows)
```

This is sufficient for a functional report.

---

## Parameters and validation

Reports are parameter-driven. Parameters are passed via:

```python
bf.report.run("my_report", param1=..., param2=...)
```

Inside the report, parameters are accessed via:

```python
value = self.params.get("param_name", default)
```

### Best practices

* Always define **safe defaults**.
* Validate types and ranges early.
* Fail gracefully with empty DataFrames and log warnings when inputs are invalid.
* Never raise raw database exceptions to the user.

---

## Output columns and stability

Reports should expose **stable, human-readable output columns**.

Recommended pattern:

* Internally use ORM column labels.
* Map internal labels to user-facing display names.
* Expose available columns via:

```python
bf.report.available_columns("report_name")
```

This enables:

* compact outputs,
* standardized tables across projects,
* selective column retrieval via `output_columns=[...]`.

Stable column naming is **critical** for downstream reproducibility.

---

## ORM-first query design

Reports should be implemented using:

* SQLAlchemy ORM, or
* SQLAlchemy Core expressions.

Benefits:

* schema awareness,
* portability across SQLite and PostgreSQL,
* safer refactoring as the schema evolves.

Raw SQL should be avoided in reports unless strictly necessary for performance or
legacy compatibility.

---

## Performance considerations

Reports are expected to scale from:

* small interactive notebook usage,
* to large production databases.

When applicable, reports should:

* leverage indexed columns,
* minimize cross-joins,
* adapt execution strategy based on input size.

If a report uses multiple execution paths (e.g. small-input vs large-input
strategy), this behavior must be documented in the report’s explanation.

---

## Self-describing reports

Well-designed reports are self-documenting.

Recommended optional methods:

```python
def example_input(self):
    ...

def explain(self):
    ...
```

### `example_input()`

Returns a minimal input payload for quick testing.

### `explain()`

Describes:

* the biological question answered,
* main join logic,
* assumptions and limitations,
* performance characteristics.

These methods power:

```python
bf.report.run_example("report_name")
bf.report.explain("report_name")
```

They significantly improve discoverability and usability.

---

## ReportManager responsibilities

The `ReportManager` is responsible for:

* discovering all report modules,
* instantiating reports with the correct DB session,
* routing execution calls,
* exposing high-level helpers (`list`, `run`, `explain`, etc.).

Report authors do **not** interact directly with the manager; they only implement
compliant report classes.

---

## Developer checklist

Before committing a new report, verify:

* File lives in `biofilter/report/reports/`
* Filename starts with `report_`
* Report inherits from `BaseReport`
* `name` is unique and stable
* `run()` returns a DataFrame
* Parameters have defaults and validation
* Output columns are documented
* Works on both populated and empty databases
* No direct DB connection management

---

## Summary

Reports are the core mechanism for delivering curated, reusable biological
queries in Biofilter 4. From a developer perspective, writing a report means
encapsulating validated ORM logic into a discoverable, parameter-driven module
with stable outputs.

By following the conventions described in this chapter, developers can rapidly
extend Biofilter with new reports while preserving consistency, performance, and
long-term maintainability.
