# The Read Path

Reports do not connect to a database. They open a directory.

This page is the mechanism between `Bundle.open()` and a result row: how a
bundle becomes queryable, what is checked on the way, and what the read
layer deliberately cannot do. For the practical setup — pointing Biofilter
at a bundle and running something — see
[Pointing Biofilter at a bundle](../getting_started/reading_a_bundle.md).

## Opening a bundle

```python
from biofilter.modules.report.bundle import Bundle

bundle = Bundle.open("/path/to/bundles/20260914")

bundle.bundle_id                      # which build this is
bundle.tables["variant_masters"].rows # what it declares
bundle.has("variant_gtex")            # whether a source made it in
```

Six things happen, in this order:

1. **Read `manifest.json`.** If it is not there, this is not a bundle and
   the error says so — including the most common cause, which is pointing at
   the `tables/` subdirectory instead of the bundle root.
2. **Check the manifest version.** This release reads `manifest_version: 2`.
   A bundle declaring anything else is refused rather than guessed at.
3. **Resolve logical tables** from the manifest entries.
4. **Verify the files** — every one the manifest declares is present, at the
   size it recorded.
5. **`duckdb.connect()`** — in the same process. No server, no port, no
   daemon.
6. **Register one view per logical table.**

What is *not* created is as much the point: no `Engine`, no `sessionmaker`,
no connection pool, no `Session`, no ORM. The read layer has none of that
machinery because it needs none of it.

Most code reaches this through the facade rather than directly:

```python
from biofilter import Biofilter

bf = Biofilter(bundle="/path/to/bundles/20260914")
result = bf.report.run("annotate_gene", input_data=["TP53"])
```

## The manifest is the catalogue

Views are built from what the manifest declares, not from what is on disk.

Each manifest entry names a file. A single-file table names only itself; a
partitioned table declares one entry per file, each carrying a `table` field
naming the parent it belongs to. Entries are grouped by that field, and each
group becomes one view:

```sql
CREATE OR REPLACE VIEW variant_masters AS
SELECT * FROM read_parquet([ ...the files the manifest names... ],
                           union_by_name = true);
```

So `variant_masters` is one queryable name whether it is one file or
twenty-five, and no filename convention has to be interpreted to work that
out.

This matters more than it looks. Inferring table membership from filenames
is what once let an empty parent file shadow 177 million rows of real data —
the same defect, independently, in two code paths. Reading the manifest
removes that whole class of error rather than patching its symptoms.

## Verification is tiered

Hashing 21 GB on every `report run` is not an option, so the checks are
split by cost:

| Tier | Checks | Cost | When |
|---|---|---|---|
| Open | Every declared file is present, at the declared size | ~100 `stat()` calls | Every `Bundle.open()`, by default |
| `db verify` | Adds SHA-256 re-computation | Reads every byte | On demand |
| `db verify --schema` | Adds column-level comparison against the models | A metadata read | Gating, CI |

```bash
biofilter db verify --in ./bundles/20260914
biofilter db verify --in ./bundles/20260914 --schema
```

Two notes on the middle tier. `--no-hashes` skips the re-computation. And
the check only runs where the manifest actually recorded a digest — bundles
produced by `bundle build` currently record size but not SHA-256, so for
those the hash tier is a no-op and `verify` is checking presence and size.

The `--schema` tier exists because opening a bundle only *warns* when a
table is missing columns this build expects. That is deliberate: a bundle
built from a subset of sources legitimately has fewer tables, and refusing
to open it would make it useless for the sources it does have. `--schema`
turns the warning into an error, for when you need a gate.

## One cursor per report

Each report execution gets `bundle.cursor()` — its own connection over the
same database.

Views are catalog objects, so every cursor sees them. Temp tables and
registered relations are connection-scoped, so they do not leak between
cursors. That is what gives one report execution a private scratch space,
and it is why two reports running in the same process cannot collide on a
temp table name.

Report input arrives the same way: registered as a relation on the cursor
and joined, never interpolated into the SQL. That is both what keeps
injection out and what turns a ten-thousand-value filter into a hash join
instead of a literal list.

## Tuning

```python
bundle = Bundle.open(path, threads=6, memory_limit="12GB")
```

`memory_limit` is a declared ceiling, and DuckDB respects it by spilling
rather than by failing. The read path also sets
`preserve_insertion_order = false`: results are assembled by joins and
ordered explicitly when order matters, so letting DuckDB drop that guarantee
is a straight memory saving on large scans.

## What to expect

A production-scale question, measured on a 21 GB bundle with 114 declared
tables (macOS, 6 threads, `memory_limit = 12GB`):

> Which of a cohort's 711,836 variants map to a protein-coding gene?
> Against `variant_masters` (177,520,333 rows) and
> `variant_molecular_effects` (2,238,929,441 rows).

| Step | Time |
|---|---:|
| Parse input | 0.2 s |
| Coding genes | 0.0 s |
| Match `variant_masters` (177 M) | 2.5 s |
| Join `variant_molecular_effects` (2.2 B) | 8.9 s |
| Coding filter and aggregate | 0.1 s |
| **Total** | **11.9 s** |

Peak resident memory was 1.87 GB against the declared 12 GB ceiling. Queries
stream from disk with column pruning and predicate pushdown, so memory
tracks the shape of the result rather than the size of the table.

Shared network storage costs roughly an order of magnitude in wall clock
against local NVMe and still lands inside interactive range.

## Writing is not possible

Not "blocked" — absent. The read layer opens parquet and has no write path
to disrespect, so there is no flag to set and no mode to get wrong.

Refreshing data means producing a **new** bundle; see
[Building Bundles](building_bundles.md). Deployments that want the guarantee
visible on disk as well typically make the bundle directory non-writable
(`chmod -R a-w`).

## Querying a bundle without Biofilter

A bundle is parquet files and a JSON catalogue, so anything that reads
parquet can read it:

```sql
-- single-file table
SELECT * FROM read_parquet('/path/to/bundle/tables/gene_masters.parquet')
LIMIT 5;

-- partitioned table: pass the files together
SELECT chromosome, count(*)
FROM read_parquet('/path/to/bundle/tables/variant_masters/*.parquet',
                  union_by_name = true)
GROUP BY 1 ORDER BY 1;
```

One caution: read `manifest.json` to learn which files make up a table
rather than trusting a glob. The manifest is the only authority on that, and
the bugs that come from guessing are silent ones.

## Two parquet readers, and which is which

There are two in the codebase, and they are not interchangeable:

| | `Bundle` | `Database` with a `parquet://` URI |
|---|---|---|
| Where | `modules/report/bundle.py` | `modules/db/database.py` |
| Builds views from | `manifest.json` | A directory scan |
| Stack | DuckDB directly | SQLAlchemy over an in-memory DuckDB |
| Used by | Every report | `db verify --schema`, `bf.db` helpers |

Reports never touch the second one. `Biofilter(bundle=...)` stores the path
as a `parquet://` URI internally, but the report component resolves it back
to a directory and calls `Bundle.open()` on it.

## Troubleshooting

**`No manifest.json in <path>`**
The path is not a bundle root. Point at the directory that holds
`manifest.json`, not at its `tables/` subdirectory.

**`<name> declares manifest_version N; this Biofilter reads [2]`**
The bundle was written by a newer Biofilter. Update the package — the bundle
is fine and needs no migration.

**`<name> does not match its manifest`**
A declared file is missing or the wrong size. The message names the files.
An incomplete copy is the usual cause; re-sync the directory.

**`<name> does not carry: <tables>`**
The bundle was built without the sources that report needs. Run
`platform_data_statistics` to see what it does have.

**A column is all null**
Check `result.provenance["coverage"]`. It records which optional tables the
bundle did not have — a column that is null because a source was never built
looks exactly like one that is null because the answer is null.

## See also

- [Pointing Biofilter at a bundle](../getting_started/reading_a_bundle.md) — practical setup
- [Building Bundles](building_bundles.md) — where bundles come from
- [Database Operations](database.md) — `verify`, `export`, `import`
- [Configuration](configuration.md) — how the bundle path is resolved
