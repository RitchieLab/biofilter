# AG ETL - Update and Operations (CLI/API/Reports)

Detailed guide to run and monitor ETL in Biofilter, covering:
- CLI usage
- API usage (Python/Notebook)
- support reports
- recommended flow for long and resumable runs
- playbook for LLM assistants

---

## 1) Goal

This guide explains how to:
- update one or more DataSources manually (`etl update`)
- update many DataSources sequentially with resume support (`etl update-all`)
- monitor status and audit history (`etl status`, `etl_status`, `etl_packages`)
- restart or rollback when needed (`etl restart`, `etl rollback`)

---

## 2) Quick Concepts

- **DataSource**: ETL source unit (for example: `hgnc`, `dbsnp_chr1`, `gnomad_chr22`).
- **ETL pipeline**: `extract -> transform -> load`.
- **ETLPackage**: execution record for ETL stages.
- **Resume behavior**: in `update-all`, DataSources already successful are skipped.

---

## 3) Prerequisites

- DB configured (`--db-uri` or `.biofilter.toml`).
- ETL paths configured (`[etl].data_root`) when needed.
- Python environment ready (`poetry run ...` is recommended during development).

Validation example:

```bash
poetry run biofilter etl --help
```

---

## 4) ETL Commands (CLI)

### 4.1 `biofilter etl update`

Manual, explicit update for a selected subset.

Common usage:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl update --data-source hgnc
```

Specific steps:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl update \
  --data-source dbsnp_chr22 \
  --run-step extract --run-step transform --run-step load
```

Force a step:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl update \
  --data-source hgnc \
  --force-step extract
```

Important:
- If no `--source-system` and no `--data-source` is passed, command aborts by design.

---

### 4.2 `biofilter etl update-all`

Sequential update for multiple DataSources, with resume-friendly behavior.

Basic (all active):

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl update-all
```

Filter by source system:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl update-all \
  --source-system NCBI
```

Drop raw/processed files after each successful load:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl update-all \
  --drop-files
```

Stop on first failure:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl update-all \
  --stop-on-error
```

Current behavior:
- resolves DataSources in deterministic order (`data_source_id` ascending)
- checks latest `load` status per DataSource
- skips DataSources already in success state
- runs `extract -> transform -> load` for pending ones
- with `--drop-files`, deletes `raw/processed` only after successful load
- prints a final summary: `selected`, `skipped`, `processed`, `succeeded`, `failed`

---

### 4.3 `biofilter etl status`

Quick operational view by DataSource: success/fail + latest execution time.

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl status
```

With filter:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl status \
  --source-system NCBI --only-active
```

---

### 4.4 `biofilter etl restart`

Rollback DataSource data and rerun full ETL pipeline.

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl restart \
  --data-source gnomad_chr22
```

With file cleanup:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl restart \
  --data-source gnomad_chr22 \
  --delete-files
```

---

### 4.5 `biofilter etl rollback`

Rollback without rerunning ETL.

By package:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl rollback --package-id 123
```

By DataSource:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db etl rollback \
  --data-source gnomad_chr22 \
  --delete-files
```

---

## 5) Operational Support Reports

### 5.1 `etl_status` (DataSource-level consolidated view)

- consolidated ETL state per DataSource
- includes DataSources with no packages yet

CLI:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db report run --name etl_status
```

API:

```python
df_status = bf.report.run("etl_status", only_active=False)
```

Useful columns:
- `source_system`
- `data_source`
- `extract_status`
- `transform_status`
- `load_status`
- `pipeline_ok`
- `latest_error`

---

### 5.2 `etl_packages` (detailed audit)

- raw package history
- best for debugging failures and timing

CLI:

```bash
poetry run biofilter --db-uri sqlite:///biofilter_dev.db report run --name etl_packages
```

API:

```python
df_pkg = bf.report.run("etl_packages", only_active=False)
```

Useful columns:
- `package_id`
- `operation_type`
- `status`
- `extract_status`, `transform_status`, `load_status`
- `extract_minutes`, `transform_minutes`, `load_minutes`

---

## 6) API Usage (Python/Notebook)

### 6.1 Setup

```python
from biofilter import Biofilter
import pandas as pd

bf = Biofilter(db_uri="sqlite:///biofilter_dev.db", debug_mode=False)
bf.db.connect()
```

---

### 6.2 Run a targeted update

```python
bf.etl.update(
    data_sources=["hgnc"],
    run_steps=["extract", "transform", "load"],
    force_steps=[],
)
```

---

### 6.3 Run resumable update-all

```python
summary = bf.etl.update_all(
    source_system=None,
    data_sources=None,
    drop_files_on_success=False,
    only_active=True,
    stop_on_error=False,
)
print(summary)
```

Example output:

```python
{
    "selected": 120,
    "skipped": 95,
    "processed": 25,
    "succeeded": 24,
    "failed": 1,
}
```

---

### 6.4 Monitoring in Notebook

```python
df_status = bf.report.run("etl_status", only_active=False)
display(
    df_status[
        ["source_system", "data_source", "extract_status", "transform_status", "load_status", "pipeline_ok", "latest_error"]
    ].sort_values(["source_system", "data_source"])
)
```

```python
df_pkg = bf.report.run("etl_packages", only_active=False)
display(
    df_pkg[
        ["package_id", "created_at", "source_system", "data_source", "operation_type", "status", "load_status"]
    ].sort_values(["package_id"], ascending=False).head(50)
)
```

---

## 7) Recommended Operational Flow

1. Check current state with `etl status` + `etl_status` report.
2. Run `etl update-all` (first cycles usually with `--keep-files`).
3. Investigate failures in `etl_packages`.
4. Fix source/input/runtime issues.
5. Run `etl update-all` again (resume skips already completed DataSources).
6. After stability, consider `--drop-files` to reduce disk usage.

---

## 8) Quick Troubleshooting

- **Error: "No source_system or data_sources provided. Aborting."**
  - expected for `etl update`; provide explicit target.
  - use `etl update-all` for batch runs.

- **DataSource does not progress in `update-all`**
  - check latest load package in `etl_packages`.
  - check `latest_error` in `etl_status`.

- **Intermittent processing failure**
  - rerun `update-all`; flow is resumable.
  - use `--stop-on-error` only when you want early interruption.

- **Low disk space**
  - run with `--drop-files` after validating stable loads.

---

## 9) LLM Assistant Playbook

### 9.1 Pre-run checklist

- confirm target `db_uri`
- confirm run mode (`update` vs `update-all`)
- confirm file policy (`--drop-files` vs `--keep-files`)
- log executed command and timestamp

### 9.2 Recommended strategy

1. Run `etl status`.
2. If broad backlog exists, run `etl update-all --only-active`.
3. After run, collect:
   - `report run --name etl_status`
   - `report run --name etl_packages`
4. Deliver summary with:
   - processed/succeeded/failed/skipped
   - failed DataSources
   - next recommended action

### 9.3 Safety rules

- do not run rollback automatically without explicit confirmation
- avoid `--drop-files` by default on sensitive environments
- prefer `update-all` for controlled resumable operation
- always report failures with context (`data_source`, stage, error)

### 9.4 Suggested base prompt

```text
You are operating Biofilter ETL.
1) Run `biofilter etl status` and summarize pending items.
2) Run `biofilter etl update-all --only-active`.
3) At the end, run reports `etl_status` and `etl_packages`.
4) Provide a summary: succeeded/failed/skipped, failed data_sources, recommendations.
Do not execute rollback without confirmation.
```

---

## 10) Internal References

- CLI command map: `biofilter/api/cli/ag_01_commands.md`
- ETL CLI group: `biofilter/api/cli/groups/etl.py`
- ETL manager: `biofilter/modules/etl/etl_manager.py`
- Reports:
  - `biofilter/modules/report/reports/report_etl_status.py`
  - `biofilter/modules/report/reports/report_etl_packages.py`

---

## 11) Document Status

- file: `biofilter_agents/ag_etl_en.md`
- scope: ETL operations (CLI/API/Reports)
- intended future use: source material for official docs
