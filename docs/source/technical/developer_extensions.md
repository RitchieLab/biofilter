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

Usually this comes from the seed files applied by `biofilter db create-db`
and re-applied by `biofilter db upgrade`.

### 5. Validate end-to-end

Recommended checks:

```bash
biofilter etl update --data-source <your_data_source> --run-step extract --run-step transform --run-step load
biofilter etl status --data-source <your_data_source>
biofilter report run --report-name platform_etl_packages
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

A report is four artifacts, and they ship together. Writing only the first
one produces something that runs but that nobody can find, understand or
trust.

| Artifact | Path |
|---|---|
| The report | `biofilter/modules/report/reports/report_<name>.py` |
| Its guide | `biofilter/modules/report/reports_explain/report_<name>.md` |
| A worked notebook | `notebooks/templates/reports__<name>.ipynb` (copy `reports__TEMPLATE.ipynb`) |
| Tests | `tests/unit/report/test_report_<name>.py`, against the fixture bundle in `conftest.py` |

Reports are discovered by being in the package. There is nothing to
register and no CLI change to make.

### 1. The contract

```python
import pyarrow as pa
from biofilter.modules.report.reports.base_report import ReportBase


class ExpandSomethingReport(ReportBase):
    name = "expand_something"
    description = "One sentence, shown by `report list`."

    #: Bundle tables this cannot work without. Checked before run().
    requires = ("entities", "entity_aliases", "gene_masters")

    #: Tables it uses when present and does without when absent. Their
    #: absence is recorded in the result's coverage, not raised.
    optional = ("variant_rsid",)

    def run(self) -> pa.Table:
        ...
```

`run()` returns a `pyarrow.Table`; the manager wraps it with provenance.
Returning a `ReportResult` directly is allowed when the report needs to
attach artifacts of its own.

Exactly one `ReportBase` subclass may be **defined** in the module. Bases
imported from the shared modules do not count — a module whose name does
not start with `report_` (`_annotation.py`, `_resolution.py`,
`_variants.py`, `_cohort.py`) is shared SQL, not a report, and is not
discovered.

### 2. Write SQL, not loops

A report writes SQL and returns a table. It does not assemble rows in
Python, and it does not interpolate input into a query.

| Helper | Does |
|---|---|
| `self.sql(query, params)` | Run on this report's cursor, return Arrow. `params` are bound by DuckDB, never formatted in. |
| `self.stream(query, params)` | The same query as a `RecordBatchReader`, for results too large to hold. |
| `self.register_input(values, ...)` | Register the user's input list as a relation to **join against**. |
| `self.register(name, table)` | Expose any Arrow table to SQL, on this cursor only. |
| `self.param("key", required=True)` | Read and validate a parameter. |
| `self.resolve_input_list(...)` | Accept `input_data` as a list or a file. |
| `self.note_provenance(...)` | Record a decision the parameters do not show. |
| `self.add_artifact(...)` | Attach a file the report produced beside the result. |

`register_input` is the important one. Joining against a registered
relation is both what keeps injection out and what turns a ten-thousand
value filter into a hash join instead of a literal list:

```python
def run(self) -> pa.Table:
    genes = self.resolve_input_list()
    self.register_input(genes, name="in_genes")

    return self.sql("""
        SELECT g.symbol, count(*) AS n
        FROM in_genes i
        JOIN entity_aliases a ON upper(a.alias) = upper(i.value)
        JOIN gene_masters  g ON g.entity_id = a.entity_id
        GROUP BY 1 ORDER BY 2 DESC
    """)
```

Each execution gets its own cursor, so temp tables and registered
relations do not leak between reports running in the same process.

### 3. Declare what you need, so absence is visible

`requires` fails before the query runs, naming what is missing, instead of
failing somewhere inside the third join.

`optional` is the subtler one. A table the report can do without still
leaves holes when it is absent — columns come back null, and a null
because the source was never built is indistinguishable from a null
answer. Declaring it puts the absence in the result's `coverage`, which is
the only place a reader can tell the two apart. Declare every table you
read; the split is about whether its absence is fatal, not about whether
it matters.

### 4. Add the guide and the notebook

`biofilter/modules/report/reports_explain/report_<name>.md` is what
`report explain --report-name <name>` prints. Cover the parameters, the
columns, and how to read the result — including what an empty result means.

The notebook is a worked example against a real bundle. Copy
`notebooks/templates/reports__TEMPLATE.ipynb`.

### 5. Validate

```bash
biofilter --bundle <path> report list
biofilter --bundle <path> report explain --report-name <name>
biofilter --bundle <path> report run --report-name <name> --params-template
biofilter --bundle <path> report run --report-name <name> --input EXAMPLE
poetry run pytest tests/unit/report/test_report_<name>.py
```

## CLI Parameter Model for Reports

`report run` uses a dynamic parameter contract:

- inputs: `--input`, `--input-file`, `--input-column`
- options: `--param KEY=VALUE`, `--params-json`, `--params-file`

Guideline:

- use input flags for record lists (`input_data`)
- use `--param` for behavioral options (`relationship_scope`, filters, toggles)

This keeps new reports extensible without changing CLI support code.

## Checklist

**A DTP:** module with `extract`/`transform`/`load` → explain markdown →
data source registered in the seed → `etl update --data-source <name>`
end to end.

**A report:** module → explain markdown → notebook → tests →
`report list`, `report explain` and `report run` all behave.
