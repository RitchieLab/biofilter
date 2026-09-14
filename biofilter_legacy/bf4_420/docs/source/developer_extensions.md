# Developer Extensions

This page explains how to extend Biofilter with new ETL data packages (DTPs) and new reports.

## Add a New DTP

### 1. Create the DTP module

Create a file under:

- `biofilter/modules/etl/dtps/dtp_<your_name>.py`

The class must be named `DTP` and expose:

- `extract(raw_dir) -> (ok: bool, message: str, file_hash: str|None)`
- `transform(raw_dir, processed_dir) -> (ok: bool, message: str)`
- `load(processed_dir) -> (ok: bool, message: str)`

`ETLManager` imports the module from `etl_datasources.dtp_script`, then runs these methods in sequence.

### 2. Follow the base contract

Use `DTPBase` (`biofilter/modules/etl/mixins/base_dtp.py`) and initialize common fields:

- `self.dtp_name`
- `self.dtp_version`
- `self.compatible_schema_min`
- `self.compatible_schema_max`

In each step, keep compatibility checks and clear status messages:

- call `self.check_compatibility()`
- return explicit success/failure tuples

### 3. Use standard file layout

Use the canonical staging paths:

- raw: `<download_path>/<source_system>/<data_source>/...`
- processed: `<processed_path>/<source_system>/<data_source>/...`

Typical pattern:

- extract downloads raw files
- transform creates `*.parquet` in processed folder (`master_data.parquet`, etc.)
- load reads processed files and writes to DB

### 4. Register the datasource

Ensure a row exists in `etl_datasources` with:

- `name`
- `source_system_id`
- `dtp_script` (must match module name, e.g. `dtp_kegg`)
- active/config metadata

Usually this comes from seed files used by `biofilter db upgrade`.

### 5. Validate end-to-end

Recommended checks:

```bash
biofilter etl update --data-source <your_data_source> --run-step extract --run-step transform --run-step load
biofilter etl status --data-source <your_data_source>
biofilter report run --report-name etl_packages --param data_sources=<your_data_source>
```

For resumable batch behavior:

```bash
biofilter etl update-all --data-source <your_data_source>
```

### 6. Add explain markdown for the DTP

Create:

- `biofilter/modules/etl/dtps_explain/dtp_<your_name>.md`

You can then inspect it from CLI:

```bash
biofilter etl explain --dtp-script dtp_<your_name>
```

Or by registered data source name:

```bash
biofilter etl explain --data-source <your_data_source>
```

## Add a New Report

### 1. Create the report module

Create:

- `biofilter/modules/report/reports/report_<your_name>.py`

Rules enforced by `ReportManager`:

- module name must start with `report_`
- module must define exactly one subclass of `ReportBase`

### 2. Implement the class contract

At minimum:

- `name` (friendly identifier used by CLI/API)
- `description`
- `run(self) -> pandas.DataFrame`

Recommended:

- `example_input()` for `--params-template`
- `available_columns()` for discoverability

In `run`, prefer helpers from `ReportBase`:

- `self.param("key", required=True)` for validated params
- `self.resolve_input_list(...)` for `input_data` list/file support
- case-insensitive filters (`_filter_ci`, `_where_in_ci`) when useful

### 3. Add explain markdown

Create:

- `biofilter/modules/report/reports_explain/report_<your_name>.md`

`biofilter report explain --report-name <name>` will prefer this markdown guide.

### 4. Validate report behavior

```bash
biofilter report list --verbose
biofilter report explain --report-name <your_report_name>
biofilter report run --report-name <your_report_name> --params-template
biofilter report run --report-name <your_report_name> [options]
```

## CLI Parameter Model for Reports

`report run` uses a dynamic parameter contract:

- inputs: `--input`, `--input-file`, `--input-column`
- options: `--param KEY=VALUE`, `--params-json`, `--params-file`

Guideline:

- use input flags for record lists (`input_data`)
- use `--param` for behavioral options (`relationship_scope`, filters, toggles)

This keeps new reports extensible without changing CLI support code.

## Suggested Development Checklist

1. Implement module (DTP/report) using naming conventions.
2. Add/validate datasource registration (for DTP).
3. Run CLI smoke tests.
4. Add or update explain markdown.
5. Add unit/integration tests for edge cases and failures.
