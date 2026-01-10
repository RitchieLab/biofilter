# Deep dive: writing a Report (Gene → SNP)

This section walks through how to implement a Biofilter report using the
existing **Gene → SNP** report as a reference. Reports are Python classes that
encapsulate reusable, reproducible logic and return an analysis-ready
`pandas.DataFrame`.

Reports are discovered and executed by the `ReportManager`, which dynamically
loads modules from the `biofilter.report.reports` package. Reports **must** live
in that package, and the module filename **must start with** `report_`
(e.g. `report_gene_to_snp.py`). Each module must expose **exactly one**
`ReportBase` subclass.

---

## Report discovery and execution flow

At runtime, `ReportManager`:

1. Scans `biofilter.report.reports` for modules whose names start with `report_`.
2. Imports each report module and finds its single `ReportBase` subclass.  
   - If none (or more than one) are found, an error is raised.
3. Allows users to run reports by either:
   - the module name (e.g. `"report_gene_to_snp"`), or
   - the report friendly name defined by `ReportBase.name`
     (e.g. `"gene_to_snp"`).
4. Executes `run()` and performs a `rollback` in a `finally` block to avoid
   leaving *idle in transaction* states in PostgreSQL sessions.

---

## The ReportBase contract (what every report must implement)

All reports inherit from `ReportBase`. This base class provides:

- `self.session` – SQLAlchemy session (for ORM queries)
- `self.logger` – logger (defaults to `Logger(name=self.name)`)
- `self.params` – all parameters passed when the report is instantiated
- `param()` helper for required/default parameters
- input parsing helpers such as `resolve_input_list()` (list- or file-based)

### Required elements

Every report should define:

- `name` – user-facing identifier (used in `bf.report.run(...)`)
- `description` – short one-line summary for discovery/listing
- `run()` – must return a `pandas.DataFrame`

### Recommended additions

- `explain()` – describes logic, assumptions, and performance
- `example_input()` – provides a minimal runnable example

---

## Minimal template (recommended starting point)

The project includes a starter template that reinforces core rules: use
`self.session`, use `self.logger`, and always return a DataFrame.

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
        )

        rows = query.all()
        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows)
````

---

## Case study: GeneToSNPReport (what it does)

The **GeneToSNPReport** resolves a list of gene identifiers
(symbol, HGNC, Entrez, Ensembl, synonyms) and returns SNPs overlapping each
gene’s genomic region.

It uses:

* `EntityAlias` to resolve inputs to canonical gene entities
* `EntityLocation` to get gene intervals (currently build 38)
* `VariantSNP` to fetch SNPs overlapping those intervals via `position_38`
* Optional provenance enrichment via `ETLDataSource` and `ETLSourceSystem`

It also includes:

* A fixed **display column** contract
* `available_columns()` for discoverability
* A detailed `explain()` block (parameters + performance strategy)
* `example_input()` for quick testing

---

## Input handling pattern (lists and text files)

Inside `run()`, input is read from `input_data` and normalized using
`resolve_input_list()`:

* If a Python list is passed, it is used directly
* If a path to a `.txt` file is passed, one item per line is loaded
* If a named list is passed, it tries `./input_lists/{name}.txt`

In `GeneToSNPReport`, inputs are normalized to lowercase to support:

* case-insensitive matching
* deterministic tie-breaking using input order

---

## Parameter validation pattern (small but important)

`GeneToSNPReport` validates common user parameters early:

* `window_bp` must be `int >= 0` (defaults to `1000` on invalid input)
* `assembly` is output-only (`"37"`, `"38"`, or `None`)
* `output_columns` must match display names
  → unknown columns trigger an error and return an empty DataFrame
* `strategy` controls performance mode:

  * `"auto"`
  * `"per_gene"`
  * `"per_chr"` (with `per_gene_threshold`)

**Guideline:** validate fast, fail clearly, and keep output predictable.

---

## Query design: 3-phase pattern (resolve → enrich → fetch)

The Gene → SNP report follows a reusable query architecture.

### Phase 1 — Resolve domain entities from free-form input

* Resolves user inputs using `EntityAlias`
* Selects canonical representation per `entity_id`
* Pulls the primary symbol via aliased `EntityAlias` joins
* Deduplicates to one row per gene entity
* Marks duplicates with a user-visible note

---

### Phase 2 — Fetch gene intervals (EntityLocation, build 38)

* Collects locations for resolved gene entities
* Restricts to build 38
* Builds windowed ranges (`w_start`, `w_end`) based on `window_bp`

If no locations exist, the report returns a **partial output** with a warning
note instead of failing.

---

### Phase 3 — Fetch SNPs overlapping ranges (strategy-dependent)

Two execution strategies are supported (documented in `explain()`):

#### `per_gene`

* One indexed query per gene interval
* Best for small input sizes

```sql
WHERE chromosome = ?
  AND position_38 BETWEEN w_start AND w_end
```

#### `per_chr`

* One range query per chromosome
* SNPs are matched to gene windows in pandas
* Best for larger input sizes

Both strategies enrich results with provenance via joins to
`ETLDataSource` and `ETLSourceSystem`.

---

## Output contract: display columns and filtering

A strong design pattern in this report:

1. Build a DataFrame with **internal column names**
2. Rename to **stable display names**
3. Apply optional:

   * column filtering (`output_columns`)
   * assembly-based column hiding (37 vs 38)
4. Keep a `Note` column for user-facing explanations

This makes the report:

* human-friendly,
* machine-friendly,
* stable for downstream workflows.

---

## Recommended conventions for new reports

When implementing your own report:

* File name: `report_<topic>.py` in `biofilter.report.reports`
* Exactly **one** report class per file
* Define:

  * `name`
  * `description`
  * `run()` → returns `pd.DataFrame`
* Strongly recommended:

  * `explain()`
  * `example_input()`
  * `available_columns()` (if output filtering is supported)

---

## Quick run example (user perspective)

A well-written report documents how it is intended to be run.
This is a great practice to replicate inside `explain()`.

```python
df = bf.report.run(
    "gene_to_snp",
    input_data=["TP53", "HGNC:11998"],
    window_bp=5000,
    assembly=None,
    strategy="auto",
    output_columns=[
        "HGNC Symbol",
        "Variant ID",
        "SNP Pos (Build 38)",
        "Note",
    ],
)
```

---

## Takeaway

The Gene → SNP report demonstrates how to combine:

* flexible input resolution,
* entity-centric joins,
* strategy-aware performance tuning,
* stable output contracts,

into a reusable, production-grade Biofilter report.

When writing new reports, treat this pattern as the reference implementation
for balancing correctness, performance, and long-term maintainability.
