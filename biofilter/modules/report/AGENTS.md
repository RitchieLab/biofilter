# AGENTS - Report Module

This file defines local guidelines for evolving `biofilter/modules/report`.

## Objective
- Keep reports discoverable, reliable, and consistent across API and CLI.
- Preserve stable report identifiers (`ReportBase.name`) used by users and tests.

## Report Contract
- One file per report under `biofilter/modules/report/reports/`, named `report_*.py`.
- Exactly one `ReportBase` subclass per module.
- Every report must define:
  - `name`
  - `description`
  - `run()`
- Strongly recommended:
  - `explain()`
  - `available_columns()`
  - `example_input()`

## When Adding or Changing a Report
1. Update or add the tutorial file `ag_XX_report_<name>.md`.
2. Validate report discovery via `ReportManager.index()`.
3. Add or update tests:
   - Unit tests for parser/logic branches.
   - Integration tests for expected query behavior (SQLite/Postgres as needed).
4. If behavior is user-visible, update CLI/API parity tests when applicable.

## Design Rules
- Keep output schema stable and explicit.
- Validate inputs early and return actionable errors.
- Prefer chunked/bulk DB queries for large ID lists.
- Avoid DB-dialect-specific SQL unless report is explicitly PostgreSQL-only.
- For PostgreSQL-only reports, fail fast with a clear message on non-Postgres DBs.

## References
- Report manager: `biofilter/modules/report/report_manager.py`
- Base class: `biofilter/modules/report/reports/base_report.py`
- Tutorials: `biofilter/modules/report/ag_*.md`
