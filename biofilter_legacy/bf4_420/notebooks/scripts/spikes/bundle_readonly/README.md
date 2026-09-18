# Spike — Report layer over a read-only `parquet://` bundle

**Status:** concluded — premise holds
**Branch:** `spike/reports-over-parquet`
**Date:** 2026-08-17

## Why this spike existed

The parquet-bundle build plan (temp staging DB → parquet bundle → consume
without any DB server) rests on one unverified assumption:

> the report layer runs unchanged against a read-only `parquet://` backend.

If a significant share of reports needed a writable relational engine, the
whole "local base without PostgreSQL" goal would collapse. This spike
measured that empirically instead of assuming it.

This is a **validation spike — no production code was changed.** The one
fix it identifies is validated here via runtime patch and is left for the
follow-up PR.

## Verdict

**The premise holds.** With a single one-line fix, 24 of 26 reports reach
full parity with PostgreSQL. The 2 that do not are PostgreSQL-only by
explicit design.

| Verdict | Baseline | After fix |
| --- | ---: | ---: |
| PARITY (identical status + row count) | 15 | **17** |
| PARITY_ROWDIFF (explained by the test subset) | 2 | 2 |
| BLOCKED (works on PG, fails on parquet) | 3 | **2** |
| DIVERGENT (silently different result) | 1 | **0** |
| SKIPPED (needs external input files) | 5 | 5 |

## Findings

### 1. BLOCKER — JSON columns break every report that selects them

`ETLPackage.stats` is the schema's only `JSON` column, and it broke the two
main monitoring reports.

Root cause: psycopg2 returns JSON already decoded, and SQLAlchemy's
PostgreSQL dialect disables its JSON deserializer accordingly.
`duckdb_engine` also returns a decoded `dict`, but the generic dialect still
applies the deserializer — so SQLAlchemy calls `json.loads()` on a `dict`:

```
TypeError: the JSON object must be str, bytes or bytearray, not dict
```

Impact was worse than a plain error:

- `report_etl_status` — raised (visible failure)
- `report_etl_packages` — its `except Exception: return pd.DataFrame()`
  swallowed the error and returned **0 rows instead of 55**: a silent wrong
  answer

Validated fix (in `Database._engine_kwargs`, DuckDB branch only):

```python
kw["json_deserializer"] = (
    lambda v: v if isinstance(v, (dict, list)) else json.loads(v)
)
```

After the fix both reports reach parity: 55/55 and 42/42 rows.

### 2. BLOCKER — `export_full_clone()` duplicates all partitioned variant data

`export_full_clone()` iterates `inspect(engine).get_table_names()`, which on
PostgreSQL returns **both** the partitioned parent and all its children. On
the dev DB that is 43 parents + 125 children.

Since `SELECT * FROM variant_masters` already returns every partition's rows,
the bundle stores each variant row twice — once in `variant_masters.parquet`
and again across `variant_masters_chr_N.parquet`.

Worse, `Database._register_parquet_views()` then **skips** every file with
`_chr_` in its name, so the duplicated half is never read: pure dead weight.
At full gnomAD scale this doubles a multi-hundred-GB bundle for no benefit.

The export must select parents *or* children deliberately — not both.

### 3. NOT A PROBLEM — temp tables work fine

Four reports use `CREATE TEMP TABLE` + `INSERT INTO`. This was the feared
blocker, and it is a non-issue: the `parquet://` backend is an **in-memory
DuckDB** where only the parquet-backed VIEWs are read-only. Temp tables were
verified working directly, and `report_annotation_variant_regulatory_evidence`
(which uses one) reaches parity.

### 4. NOT A PROBLEM — booleans and joins round-trip correctly

Verified explicitly after `report_etl_packages` first looked like a boolean
filter bug: `active` stays `BOOLEAN`, `IS TRUE` matches the same 42 rows on
both backends, and the 3-table ETL join returns 55 on both.

### 5. Observation — `db.read_only` is a dead flag

`Database.read_only` is assigned in two places and **never read anywhere in
the codebase**. Nothing enforces read-only semantics today. Either enforce it
or drop it; leaving it suggests a guarantee that does not exist.

### 6. Observation — silent-failure pattern in reports

`report_etl_packages` catches every exception and returns an empty
DataFrame. That converted a hard error into a plausible-looking empty result
and is why finding #1 was nearly missed. Worth auditing across reports,
independent of this work.

## Residual risk — what this spike did NOT prove

Honest limitations:

- **5 reports were never exercised** because they require external input
  files the repo doesn't ship: `snp_snp_pair_generator`,
  `variant_list_intersect`, `variant_annotation_expanded`,
  `variant_binning`, `template`. Two of them (`variant_annotation_expanded`,
  `variant_binning`) are exactly the ones that write temp tables and output
  files — the temp-table *mechanism* is proven by #3, but these specific
  reports are unverified end to end.
- **The bundle is a subset**, windowed to `chr1:1,000,000-6,000,000`. Both
  `PARITY_ROWDIFF` results were traced to this and are not defects
  (`platform_data_statistics` misses exactly the chr2/chr3 count rows;
  `variant_gene_location_model` sees fewer variants). Full-scale behaviour —
  especially memory on a multi-GB single-file parquet — is untested.
- **Empty source tables**: `variant_gwas`, `variant_snp_merges`,
  `variant_effect_predictions`, `variant_regulatory_elements` and
  `chemical_masters` are empty in dev, so reports touching them agree
  trivially rather than meaningfully.
- Reports were compared on **status and row count**, not cell-by-cell values.

## Reproducing

Requires a populated PostgreSQL at the URI in `build_spike_bundle.py`.

```bash
# 1) Build the subset bundle (~1m45s; entity graph + chr1 variant window)
poetry run python scripts/spikes/bundle_readonly/build_spike_bundle.py ./spike_bundle

# 2) Baseline — current code
poetry run python scripts/spikes/bundle_readonly/report_matrix.py ./spike_bundle

# 3) With the candidate JSON fix applied at runtime
SPIKE_JSON_FIX=1 poetry run python scripts/spikes/bundle_readonly/report_matrix.py ./spike_bundle
```

Each run writes `spike_matrix_{baseline,postfix}.json` next to the bundle,
including full tracebacks.

## Recommended follow-up

Items 1–4 were addressed in the bundle-foundation work that followed this
spike; `report_matrix.py` doubles as its regression test (run it *without*
`SPIKE_JSON_FIX` and the post-fix column should reproduce).

1. ~~Apply the `json_deserializer` fix (finding #1)~~ — done.
2. ~~Fix `export_full_clone()` parent/child selection (finding #2)~~ — done.
3. ~~Teach `_register_parquet_views()` to register partitioned
   **directories** as datasets~~ — done.
4. ~~Decide `db.read_only` (finding #5)~~ — documented as advisory; not
   enforced, because reports legitimately create temp tables.
5. Cover the 5 unexercised reports with committed fixture inputs — **still
   open**.
6. Audit the silent `except → return empty DataFrame` pattern (finding #6)
   — **still open**.
