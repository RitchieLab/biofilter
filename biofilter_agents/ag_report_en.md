# AG Report - Report Operations in Biofilter (CLI/API/Explain Guides)

Detailed guide for working with the Biofilter report layer.

Covers:
- report discovery and introspection
- report execution via CLI and API
- dynamic parameter passing (`--input`, `--param`, JSON/YAML)
- explain guide architecture (`reports_explain`)
- authoring pattern for new reports
- LLM assistant playbook

---

## 1) Goal

This guide helps you run and maintain reports in a way that scales as new reports are added, without changing CLI support code for each report.

Key design principles:
- report logic lives in `modules/report/reports/report_*.py`
- report explain/tutorial content lives in `modules/report/reports_explain/report_*.md`
- CLI is generic and dynamic (`report run` with generic parameter injection)

---

## 2) Report Architecture

Each report is composed of:

1. Python report module:
- path: `biofilter/modules/report/reports/report_<something>.py`
- typically defines:
  - `name`
  - `description`
  - `run()`
  - `available_columns()`
  - `example_input()`
  - optional `explain()` fallback

2. Explain/Tutorial markdown:
- path: `biofilter/modules/report/reports_explain/report_<something>.md`
- used by `biofilter report explain`

Explain resolution behavior:
- first tries `reports_explain/report_<module>.md`
- then tries legacy paths (if present)
- if no guide exists, falls back to report class `explain()`

This gives you dynamic explain docs per report while keeping backwards compatibility.

---

## 3) Discover and Inspect Reports (CLI)

List reports:

```bash
biofilter report list
biofilter report list --verbose
```

Show explain/tutorial:

```bash
biofilter report explain --report-name etl_status
```

Show expected example input from report class:

```bash
biofilter report example-input --report-name entity_relationship_model
```

Show available output columns:

```bash
biofilter report available-columns --report-name etl_packages
```

Refresh report cache:

```bash
biofilter report refresh
```

---

## 4) Run Reports (CLI)

Basic run:

```bash
biofilter report run --report-name etl_status
```

Export CSV:

```bash
biofilter report run --report-name etl_packages --output ./etl_packages.csv
```

Show params template (from `example_input()`):

```bash
biofilter report run --report-name entity_relationship_model --params-template
```

Pass direct inputs:

```bash
biofilter report run --report-name entity_filter --input BRCA1 --input TP53
```

Pass input file:

```bash
biofilter report run --report-name entity_filter --input-file ./entities.txt
biofilter report run --report-name entity_filter --input-file ./entities.csv --input-column symbol
```

Pass generic parameters:

```bash
biofilter report run \
  --report-name entity_relationship_model \
  --input TP53 --input BRCA1 \
  --param relationship_scope=input_to_any \
  --param deduplicate_pairs=true
```

Pass parameter files:

```bash
biofilter report run --report-name entity_relationship_model --params-file ./params.yaml
biofilter report run --report-name entity_relationship_model --params-json '{"relationship_scope":"input_to_any"}'
```

Large value from file in a single param:

```bash
biofilter report run \
  --report-name entity_relationship_model \
  --input TP53 \
  --param relationship_types=@./relationship_types.txt
```

Note:
- `--report-name` is the canonical option (`--name` is still accepted as alias).

---

## 5) Inputs vs Params (Important Rule)

Use:
- `--input` / `--input-file` for report inputs (`input_data`)
- `--param` for report options (scope, filters, toggles, limits, etc.)

Avoid mixing input channels:
- if `--input`/`--input-file` is provided, do not pass `input_data`, `items`, or `input_path` through `--param`/JSON/YAML.
- CLI enforces this and returns a friendly error to prevent ambiguous execution.

---

## 6) Parameter Parsing Behavior

`--param KEY=VALUE` coercion rules:
- `true` / `false` -> boolean
- `null` / `none` -> `None`
- JSON/py-literal values are parsed when possible:
  - lists: `["a","b"]`
  - dicts: `{"k":"v"}`
  - numbers: `123`, `4.5`
- `@path` loads value from file
- `@@something` escapes a literal `@something`

`--params-file` supports:
- `.json`
- `.yml`
- `.yaml`

If JSON/YAML root is not a dict, it is mapped to `{"input_data": <value>}`.

---

## 7) Run Reports via API (Notebook/Python)

Setup:

```python
from biofilter import Biofilter

bf = Biofilter(db_uri="sqlite:///biofilter_dev.db", debug_mode=False)
```

Examples:

```python
df_status = bf.report.run("etl_status", only_active=False)

df_rel = bf.report.run(
    "entity_relationship_model",
    input_data=["TP53", "BRCA1", "NOT_FOUND_ENTITY"],
    relationship_scope="input_to_any",
)

```

Introspection in API:

```python
print(bf.report.explain("etl_status"))
print(bf.report.example_input("entity_relationship_model"))
print(bf.report.available_columns("etl_packages"))
```

---

## 8) Built-in Reports (Current)

- `etl_status`
- `etl_packages`
- `entity_filter`
- `entity_relationship_model`
- `variant_gene_location_model`
- `db_pg_table_stats` (Postgres only)
- `db_pg_index_stats` (Postgres only)
- `qry_template`

Always use `biofilter report list --verbose` to confirm what is available in your runtime.

---

## 9) Authoring New Reports (Recommended Pattern)

For a new report `my_report`:

1. Create Python module:
- `biofilter/modules/report/reports/report_my_report.py`

2. Define:
- `name = "my_report"`
- `description`
- `run()`
- `available_columns()`
- `example_input()`

3. Create explain guide:
- `biofilter/modules/report/reports_explain/report_my_report.md`

4. Add tests:
- unit tests for report behavior
- optional integration tests via CLI/API

5. Validate:

```bash
biofilter report list --verbose
biofilter report explain --report-name my_report
biofilter report run --report-name my_report --params-template
```

Result:
- new reports become self-documented and executable without changing CLI support code.

---

## 10) Troubleshooting

If report is not found:
- run `biofilter report list`
- check exact report name
- use friendly suggestions from CLI output

If explain does not show markdown:
- verify file exists at `reports_explain/report_<module>.md`
- ensure filename matches report module pattern

If parameter parsing fails:
- test with `--params-template` first
- use `--params-json` or `--params-file` for complex objects
- quote JSON properly in shell

If Postgres-only reports fail:
- confirm DB is PostgreSQL for `db_pg_table_stats` and `db_pg_index_stats`

---

## 11) LLM Assistant Playbook

When an assistant runs reports:

1. Discover:
- `report list --verbose`

2. Understand:
- `report explain --report-name <report>`
- `report run --report-name <report> --params-template`

3. Execute:
- start with minimal command
- add `--input` / `--param` progressively
- export with `--output` when needed

4. Diagnose:
- prefer `etl_packages` for ETL-level audit
- prefer `etl_status` for quick consolidated health

This flow keeps report operations deterministic, explainable, and easy to automate.
