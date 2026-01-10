# Report API

Reports are the primary mechanism for exposing curated, reusable queries in
Biofilter 4. From a developer perspective, a report is a packaged ORM query that
encapsulates validated logic, enforces consistent outputs, and returns
analysis-ready results as a `pandas.DataFrame`.

This chapter explains how reports are structured, discovered, and implemented,
and provides practical guidance for writing new reports that integrate cleanly
with the Biofilter ecosystem.

```{toctree}
:maxdepth: 1
:caption: Reports

4_0_reports
4_1_write_report
