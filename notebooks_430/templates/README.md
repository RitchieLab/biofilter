# Report notebooks (4.3.0)

One notebook per report, next to the report itself in spirit: the code
lives in `biofilter/modules/report/reports/report_<name>.py`, the
reference in `reports_explain/report_<name>.md`, and the worked example
here as `reports__<name>.ipynb`.

The reference answers *what are the parameters*. The notebook answers
*what does this look like when I run it*, which is the question a new
user actually has.

| notebook | for |
| --- | --- |
| `reports__101.ipynb` | start here — how reports work against a bundle |
| `reports__TEMPLATE.ipynb` | copy this when migrating or writing a report |
| `reports__<name>.ipynb` | one per report |

## Running them

Every notebook starts with a bundle path to edit:

```python
BUNDLE = "/path/to/biofilter_data/bundles/20260914"
```

A bundle is a directory — point at the directory, not at its `tables/`.

## Writing one

Copy `reports__TEMPLATE.ipynb` and keep its section numbering; people
move between these notebooks and the sections should mean the same thing
in each. Two things earn their place in every one:

**An input that fails to resolve.** How a report reports absence is part
of what a reader needs to know, and it is the part no reference table
conveys.

**Where null differs from zero.** Null usually means "not computed" or
"not applicable"; zero means "computed, and none". Reports that count
things across a bundle built for a subset of chromosomes will return
honest zeros that are easy to misread as biology.

## 4.2.0

The previous set is frozen under
`biofilter_legacy/bf4_420/notebooks/Templates/`. It covers the reports
that still run on the legacy layer, against a PostgreSQL connection.
Those notebooks are replaced, not edited, as each report is migrated.
