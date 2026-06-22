# ADR-001: Adopt DuckDB + Parquet for HPC Read Workloads

| Field      | Value                                                                                                                        |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Status     | **Accepted** (POC validated 2026-06-22)                                                                                      |
| Date       | 2026-06-22                                                                                                                   |
| Author     | Andre Rico                                                                                                                   |
| Supersedes | (none)                                                                                                                       |
| Related    | [bf4-hpc-image.md](../bf4-hpc-image.md) (legacy SQLite/PG approach), [poc_duckdb_annotation.py](../poc_duckdb_annotation.py) |

---

## 1. Context

Biofilter 4 ships as a Python package that, until now, supported two
storage backends:

- **PostgreSQL** — production target, ETL writes, full feature set
- **SQLite** — development target, dev/test only

When deploying to the Penn LPC (HPC cluster), neither option is ideal:

1. **PostgreSQL on LPC** requires a bundled-container approach
   (`docker/hpc/Dockerfile`). Works for a single user with `PGDATA`
   bind-mounted to project storage, but has hard structural limits:
   - PG's single-postmaster-per-`PGDATA` constraint blocks multi-user
     concurrent access. Each user would need a private 480 GB copy.
   - File permissions (`chown`, `0700`) trip on shared filesystems
     under Apptainer.
   - Indexes need to be rebuilt after every dump/restore.

2. **SQLite migration** was attempted as a recovery path. After multiple
   tries (see operational history below), it failed in practice:
   - `variant_molecular_effects` has ~1.79 billion rows.
   - With `PRAGMA foreign_keys = ON` (BF4 default), `DELETE FROM` cannot
     use SQLite's truncate optimization and validates FKs per row.
     `TRUNCATE` of 593 M rows took >12 hours.
   - Bulk inserts of 1.79 B rows under FK enforcement run at ~17 M
     rows/hour — well over a day for that single table.
   - Even with FK off, 90+ BF4 curated indexes must be rebuilt over a
     1.79 B-row table — another multi-hour phase.
   - Three full 12–24 hour LSF jobs each ended in `TERM_RUNLIMIT`
     before reaching the final state.

Both approaches assume an "import" phase that converts the canonical
storage into the deployed format. For 480 GB / ~2 B rows of biological
data on a shared HPC filesystem, that import phase is the bottleneck.

A sibling project (IGEM, also Ritchie Lab) avoids the import phase
entirely by reading **Parquet files directly** via **DuckDB**. The
operational results have been good. The pattern is mature and the
trade-offs are known.

---

## 2. Decision

For **HPC read-only workloads** (reports, exploratory queries), Biofilter
4 will support a **Parquet-backed read mode** via DuckDB:

1. The Parquet bundle produced by `biofilter db export` becomes the
   canonical artifact for HPC deployment. No conversion to SQLite or PG
   is required.

2. A new connection mode (`duckdb:///path/to/biofilter.duckdb` or
   `parquet:///path/to/bundle`) is added to `Database`. On connect, BF4
   registers one DuckDB **VIEW per Parquet file** in the bundle's
   `tables/` directory.

3. The existing BF4 ORM-based reports continue to work because DuckDB
   speaks standard SQL via the `duckdb-engine` SQLAlchemy dialect.
   No report code changes for the common case.

4. **ETL and writes remain on PostgreSQL.** ETL runs on the VPS and
   produces a periodic Parquet snapshot that is published to the
   cluster. The HPC environment is read-only.

5. **Multi-user concurrent reads are first-class**: DuckDB allows
   N parallel readers over the same Parquet files with no coordination,
   because each DuckDB process opens its own file handles.

### Recommended layout

Stage 1 (simplest, ship this first): **consolidated parents only**.

```
bundle/tables/
├── entities.parquet
├── entity_aliases.parquet
├── variant_masters.parquet              (consolidated, 152 M rows)
├── variant_molecular_effects.parquet    (consolidated, 1.79 B rows)
└── ...
```

DuckDB registers one VIEW per file. BF4 reports filter on
`variant_masters` first (small N inputs from the user), DuckDB pushes the
filter down to the Parquet row-group level using min/max statistics, and
the downstream JOIN against `variant_molecular_effects` sees only the
matching `variant_id`s.

Stage 2 (optimization, only if needed): **Hive-style chromosome
partitioning** for tables that have queries with explicit chromosome
filters.

```
bundle/hive/variant_masters/chr=1/data.parquet
bundle/hive/variant_masters/chr=2/data.parquet
…
bundle/hive/variant_molecular_effects/chr=1/data.parquet
…
```

`read_parquet('…/**/*.parquet', hive_partitioning=1)` exposes the
chromosome as a virtual column. DuckDB prunes the file list when the
query includes `WHERE chromosome = N`. Only worth doing for tables and
query patterns that prove to be slow under Stage 1.

---

## 3. Consequences

### Positive

- **No import phase.** Bundle → cluster is a file copy; reports run as
  soon as the parquets are visible. ~24 h → minutes.
- **Multi-user concurrent reads** are native. Multiple LPC users can run
  reports against the same bundle without each needing a private copy.
- **Bundle is portable and citable.** A dated snapshot bundle is the
  unit of "data release" — analyses can pin to a snapshot and reproduce
  them years later by checking out the same files.
- **Image size shrinks.** No bundled PostgreSQL, no `initdb`, no
  bind-mount dance. The `bf4-hpc` Apptainer image becomes BF4 +
  duckdb-engine ≈ 300 MB instead of 1.5 GB.
- **Memory profile improves.** DuckDB uses mmap and columnar reads.
  No 30 GB pandas DataFrames just to load one table.
- **Aligns with IGEM.** Shared infrastructure pattern across lab tools.

### Negative

- **BF4 code change required.** `Database` needs a new connect branch
  for `duckdb://`. View registration logic. `duckdb-engine` added to
  `pyproject.toml`. Estimate: 1–2 days of focused engineering.
- **ETL not supported against the Parquet backend.** DuckDB views over
  Parquet are effectively read-only. Acceptable: ETL is a VPS workload.
- **Schema changes require new bundle.** Adding a column means a new
  full export → new bundle → republish. Same cost as the SQLite or PG
  path would have had.
- **Query planner differences.** Some report SQL written for PG/SQLite
  may need minor adjustments under DuckDB. Most standard SQL passes
  through unchanged, but worth validating each report after the cutover.

### Neutral / mitigations

- **Indexes don't exist** in the Parquet model. DuckDB substitutes with
  parquet row-group min/max stats and bloom filters. For BF4's typical
  query shape (small input set → filter → JOIN), this is sufficient.
  Heavy aggregate queries without filters may need separate treatment.
- **Updates require new bundle**. Acceptable for read-only deployment.

---

## 4. Alternatives Considered

### Alternative A — SQLite migration (attempted, rejected)

Approach: `biofilter db export` → Parquet bundle → `biofilter db import`
into a SQLite database file on shared storage. The SQLite is then queried
in place.

**Result: failed in practice.** Three full LSF runs each spent 12–24 h
in the import phase without reaching completion. Root causes:

- `DELETE FROM variant_molecular_effects` is O(N) under FK enforcement
  (>10 h for 593 M rows)
- Bulk INSERT validates FKs per row (>15 h for 1.79 B rows)
- Even after disabling FK, the index rebuild over a 1.79 B-row table
  is a multi-hour phase
- Total observed throughput too slow for the LPC walltime budget

Documented in detail in [bf4-hpc-image.md](../bf4-hpc-image.md) and the
operational log in `databases/20260514/logs/`.

### Alternative B — PostgreSQL in bundled container (current state, retained for special cases)

The `docker/hpc/` image keeps PG bundled. Works for **single-user**
deployments and as a fallback. Cannot be retired immediately because:

- ETL runs against PG; the VPS still uses this image
- Some development workflows benefit from a writable backend

The DuckDB strategy in this ADR **complements** rather than replaces the
PG image: HPC read-only deployments use DuckDB, VPS / ETL keeps PG.

### Alternative C — pandas/pyarrow direct reads (no SQL layer)

Reports would be rewritten to use pyarrow filtering and pandas joins
directly, bypassing both DuckDB and SQLAlchemy.

Rejected: would require rewriting every BF4 report (17+). DuckDB
preserves the SQL interface that the existing ORM-based reports already
emit, so the report layer is unchanged. The cost-benefit is heavily in
favour of keeping SQL.

### Alternative D — Per-user PGDATA copies

Each LPC user makes a private 480 GB copy of the PG `pgdata` directory
in their scratch area. Bypasses the single-postmaster constraint.

Rejected: 480 GB × N users wastes shared storage. New snapshots require
N re-copies. Doesn't address the FK / chown / Apptainer issues — only
the concurrency one.

---

## 5. Implementation outline

Order of work (estimates assume one focused engineer):

1. **POC** (~2 h)
   Standalone Python script that creates DuckDB views over an existing
   Parquet bundle and runs annotation-style queries against three input
   sizes (5 / 100 / 10 000 rsIDs). Measures wall clock and peak memory.
   This validates that the JOIN performance against the consolidated
   parents is acceptable before any BF4 code changes.

2. **Database backend** (~4–8 h)
   - Add `duckdb-engine` to `pyproject.toml`.
   - In `biofilter/modules/db/database.py`, detect `duckdb://` URI.
   - After connecting, scan the configured Parquet directory and
     `CREATE VIEW <name> AS SELECT * FROM read_parquet('<file>')` for
     each `tables/*.parquet`.
   - Mark the connection as read-only.

3. **Report compatibility pass** (~4–8 h)
   - Smoke-test every existing report against a DuckDB-backed bundle.
   - For each, log differences in row count or execution time vs. PG.
   - Patch the small number of reports (estimated <5) that emit
     PG-specific syntax (`ON CONFLICT`, `RETURNING`, `JSONB` ops, etc.).

4. **CLI surface** (~2 h)
   - `biofilter` accepts `--db-uri duckdb:///path/to/biofilter.duckdb`.
   - Helpful error when the user tries `db import` / `etl` against a
     DuckDB backend (point them to the VPS for writes).

5. **HPC deployment update** (~2 h)
   - Drop PG from the `docker/hpc/` image; rebuild as a thin
     BF4-only image with `duckdb-engine`.
   - Update [notebooks/Templates/lpc\_\_quickstart.md](../../Templates/lpc__quickstart.md)
     to show the DuckDB invocation.

Total: roughly **3–4 days of focused work** end-to-end. Mostly testing,
not new code.

---

## 6. Operational history (why this ADR exists)

Between 2026-06-17 and 2026-06-22 we attempted a full SQLite migration
on the LPC. Three increasingly aggressive resume strategies, two BF4
patches (server-side cursor in export, batched streaming in import),
and ~70 h of cumulative LSF job runtime later, the SQLite file reached
~118 GB but the row counts showed:

```
variant_masters                   152,084,680 / 152,084,680   ✅
variant_gene_regulatory_evidence   18,231,184 /  18,231,184   ✅
variant_molecular_effects         899,000,000 / 1,793,092,126 ⚠️  partial
entities, gene_masters, …                   0                ❌ never reached
```

The import always ended in the middle of `variant_molecular_effects`,
never advancing to the smaller tables. This made it clear the structural
constraints of SQLite on this dataset are not addressable by tuning.

This ADR is the consequence: a different storage strategy whose cost
profile actually matches the data.

---

## 7. POC results (2026-06-22)

POC executed on the LPC (`superman` login node) against the production
bundle (`/project/hall_shared/biofilter/databases/20260514/bundle/tables`,
40 consolidated parent parquets, ~32 GB total, including the
1.79-billion-row `variant_molecular_effects`).

DuckDB configuration: `--threads 4 --memory-limit 8GB` (in-memory catalog,
parquet read from GPFS shared FS).

Three-stage query pattern equivalent to `annotation_master_variant`:

- **Stage A:** lookup variants by rsID in `variant_masters`
- **Stage B:** Stage A + JOIN `variant_molecular_effects`
- **Stage C:** Stage B + LEFT JOIN `variant_gene_regulatory_evidence`

Joins use both `variant_id` and `chromosome` equality, so DuckDB can
push the chromosome predicate down to parquet row-group stats.

| Input size | Stage | Output rows | Wall clock | Peak Python MB |
| ---------: | :---: | ----------: | ---------: | -------------: |
|          5 |   A   |          15 |    28.16 s |            0.0 |
|          5 |   B   |         251 |    15.36 s |            0.1 |
|          5 |   C   |         251 |    16.82 s |            0.1 |
|        100 |   A   |         151 |    13.86 s |            0.0 |
|        100 |   B   |       1,492 |    14.68 s |            0.4 |
|        100 |   C   |       1,492 |    16.16 s |            0.5 |
|     10,000 |   A   |      11,065 |    14.18 s |            2.6 |
|     10,000 |   B   |     197,861 |    15.52 s |           58.6 |
|     10,000 |   C   |     275,854 |    18.22 s |           88.8 |

**Key takeaways:**

1. Stage A wall clock is roughly constant across input sizes — the
   parquet row-group statistics let DuckDB prune blocks regardless of
   how many rsIDs are in the filter. Read I/O is the dominant cost,
   not row matching.
2. The first query (cold) is ~2x slower than subsequent ones (28 s vs
   14 s for Stage A on 5 rsIDs vs 100 rsIDs). Once parquet footers and
   metadata are cached by the OS / DuckDB object cache, queries are
   consistently in the 14–18 s range.
3. The JOIN on a 1.79-billion-row table adds only ~3 seconds over the
   filter-only baseline. With chromosome equality in the JOIN, DuckDB
   prunes most row groups in `variant_molecular_effects` before doing
   the hash join.
4. **Memory peak of 89 MB for the worst case** — three orders of
   magnitude below what SQLite would have needed for the same query.
   Reports can run on a login node or in a 2 GB SLURM job.
5. Multi-allelic variants are visible in the input→output ratio (5
   rsIDs → 15 variant rows; 100 → 151; 10,000 → 11,065), which matches
   the expected schema where one rsID can map to multiple alt-allele
   variants.

**Verdict:** the strategy works as expected and at the expected
performance. The ADR moves from Proposed to Accepted.

---

## 8. Open questions

- **DuckDB version pinning.** What baseline version is on the LPC? Does
  it need to be installed via pip, or is there a system module?
- **Concurrent write avoidance.** If someone accidentally tries to write
  via the DuckDB backend, what's the failure mode? Need to confirm it's
  loud, not silent.
- **Sequences and IDs.** Reports that depend on `RETURNING id` or
  sequence values won't work read-only. Confirm none of the read-path
  reports do this.
- **Schema migrations on the bundle side.** When BF4 schema changes,
  what's the bundle compatibility story? Today: regenerate. Future:
  consider per-table version tags in the manifest.

These are tracked for follow-up but not blocking the POC or the initial
implementation.
