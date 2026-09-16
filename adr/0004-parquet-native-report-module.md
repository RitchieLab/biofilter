# ADR-004: A Parquet-Native Report Module (4.3.0)

| Field      | Value                                                                                                                                                                              |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Status     | **Proposed** (measurements taken 2026-09-14 against the 20260910 bundle)                                                                                                            |
| Date       | 2026-09-14                                                                                                                                                                          |
| Author     | Andre Rico                                                                                                                                                                          |
| Supersedes | [ADR-001](0001-duckdb-parquet-strategy.md) §2 item 3 (ORM-based reports as the read path)                                                                                            |
| Related    | [ADR-002](0002-cohort-coding-gene-overlap-report.md) D2 (set-based execution), [ADR-003](0003-parquet-native-build-pipeline.md) §2.5–2.6 (bundle-scoped IDs, provenance), §2.9 (partition layout) |

---

## 1. Context

ADR-003 made the bundle the product: immutable, versioned, parquet. The
build pipeline was rewritten for it. The report layer was not — it still
carries the shape it had when PostgreSQL was the canonical store, and it
reaches the bundle through a compatibility bridge rather than natively.

### 1.1 What the layer looks like today

25 reports, 16,443 lines in `modules/report/reports/`, plus 5,692 lines
of superseded copies in `reports_bkp/`.

The dominant shape is fan-out then merge in Python: a report issues one
ORM query per facet, materialises each with `.all()`, and joins the
results with dictionaries and loops before building a DataFrame.

| pattern                       | occurrences |
| ----------------------------- | ----------- |
| `session.query` / `.execute`  | 160         |
| `.all()`                      | 142         |
| `pd.DataFrame(...)`           | 115         |

Per report, the fan-out reaches 16 separate queries
(`gene_to_variant_filtering`), 15 (`variant_single_gene_annotation`), 14
(`annotation_variant_regulatory_evidence`). Against a relational server
with a warm buffer pool this was reasonable. Against a bundle it means N
independent passes over the same parquet files, with the join executed
in CPython — the slowest engine in the stack — instead of in DuckDB, the
fastest.

§5 measures what that costs.

### 1.2 The bridge works, and that is the problem

`parquet://` registers one DuckDB view per relation, so ORM queries
resolve to `read_parquet()` unchanged. That was the right call for the
migration: it made the bundle readable without touching 25 reports.

But the bridge is transparent in both directions — it also hides the
engine from the report author, who cannot tell a pruning-friendly
predicate from one that scans 6 GB. Measured on `variant_rsid`
(712,358,067 rows, 24 files, 6.0 GB) in the 20260910 bundle:

| predicate                                              | time    |
| ------------------------------------------------------ | ------- |
| `chromosome = 21 AND position BETWEEN ...`             | 97 ms   |
| `position BETWEEN ...` (no chromosome)                 | 551 ms  |
| `CAST(chromosome AS VARCHAR) = '21' AND position ...`  | 535 ms  |

The third row is the finding: wrapping the partition column in an
expression costs 5.5x on a narrow range, and the gap widens with scan
width. The layer does exactly this today —
`func.lower(vm.c.rsid).in_(rsids_norm)` in
`report_variant_gene_location_model.py:477` scans the `rsid` column of
every file, every time.

Nothing in the ORM discourages that, and nothing in review catches it.

### 1.3 Relations are resolved by guessing at the directory

View discovery keys directories and same-named files identically
(`database.py:295-306`). `sorted()` orders `variant_masters` before
`variant_masters.parquet`, so the file overwrites the directory:

| view                        | resolves to              | rows served | rows available   |
| --------------------------- | ------------------------ | ----------- | ---------------- |
| `variant_masters`           | `.parquet` stub (3.7 KB) | **0**       | 177,520,333      |
| `variant_molecular_effects` | `.parquet` stub (6 KB)   | **0**       | 2,238,929,441    |
| `variant_gwas`              | `.parquet` stub (4 KB)   | **0**       | 1,208,545        |
| `variant_rsid`              | directory (no sibling)   | 712,358,067 | 712,358,067      |

Every variant report returns empty against the current bundle, without
error.

This is not a one-off slip. The ADSP analysis
(`biofilter_legacy/bf4_420/notebooks/Andre/adsp/step_01/`) resolves
bundle tables with its own helper, written independently, and it makes
the same choice in the same direction — `flat.exists()` first, the
partitioned directory only as a fallback
(`bf4_map_coding_genes.py:77-85`). It worked on the 4.2.0 bundle, where
the flat file was the real one. Against 20260910 it would read the
stubs.

Two independently written code paths, one ambiguity, the same wrong
answer. The defect is not the ordering; it is that both are
reconstructing by inspection something the bundle already states.

**The manifest already carries the mapping.** It declares 114 entries,
22.33 GB against 21 GB on disk, and each partitioned child names its
logical table:

```json
{"name": "variant_masters",      "rows": 0,        "file": "tables/variant_masters.parquet"}
{"name": "variant_masters_chr1", "table": "variant_masters", "branch": "variant",
 "rows": 14090551, "file": "tables/variant_masters/variant_masters_chr1.parquet"}
```

Nothing has to be inferred. §2.3 is the consequence.

A second instance of the same class: `connect()` re-normalises an
already-normalised URI and clears `_parquet_dir`, so calling it twice
silently drops every view. The example in `CLAUDE.md` does exactly that.

### 1.4 The models and the data have diverged

The stub is not merely an empty `variant_masters`. It is a **different
table**:

| | stub `variant_masters.parquet` | data `variant_masters/` |
| --- | --- | --- |
| rows | 0 | 177,520,333 |
| columns | 23 | 25 |
| position | `position_start`, `position_end` | `position` |
| identity | `variant_id` | `variant_key` |
| frequencies | `af`, `ac`, `an` | `af_joint`, `ac_exomes`, `af_genomes`, … |
| predictors | `cadd_phred`, `revel_max`, `sift_max` | (absent) |

**Four columns in common out of 25.**

The stub matches the ORM model — `model_variants.py:109-170` declares
`position_start`, `position_end`, `variant_id`. The data matches what
the ETL actually writes — `dtp_variant_gnomad_joint.py:323-326` declares
an Arrow schema with `position` and `variant_key`. The stub is generated
from the model via the SQLite staging branch; the real rows come from
the variant branch, written parquet-native by the DTPs. The two branches
drifted and nothing compared them.

Three consequences:

- **12 of the 25 reports** reference `position_start` or `VariantMaster`
  and are written against a schema the bundle does not carry. This is
  not a performance question; they cannot run.
- **`db verify --schema` gives a false pass.** Run against 20260910 it
  reports `drift entries: 0`. It compares the models against the stub,
  which matches the models perfectly. The check exists, runs, passes,
  and is looking at the wrong file.
- **The variant tables are denormalised.**
  `variant_molecular_effects` carries `symbol`, `gene`, `biotype` and
  `consequence` as text, not as foreign keys. The lookup joins the
  reports perform (`biotype_id`, `consequence_id`) have nothing to join
  against. This is consistent with ADR-003 §1.3 — variants are
  schema-isolated by design — but it means the reports' whole approach
  to variants does not translate.

**The predictors are not merely relocated — they are absent.** The stub
declares `cadd_phred`, `revel_max`, `sift_max`, `polyphen_max`,
`spliceai_ds_max` and `pangolin_largest_ds`. None appear in
`variant_molecular_effects`, `variant_effect_predictions` is empty and
still carries the old schema, and `dtp_variant_gnomad_vep.json` does not
reference them. The DTPs that would produce them write to
`variant_alphamissense` and `variant_gtex`
(`dtp_variant_alphamissense.py:28`, `dtp_variant_eqtl_gtex.py:52`), and
neither table exists in the bundle — although `bundle_plan.json`
includes both sources. `build_record.json` shows why: on the 2026-09-12
rebuild both transformed as `not-applicable`, so nothing was produced
and nothing was moved. `gwas`, which transformed for real, is present
with 1,208,545 rows.

Ten of the 114 declared tables are empty: `chemical_masters`,
`variant_biotypes`, `variant_effect_predictions`,
`variant_gene_regulatory_evidence`, `variant_impacts`,
`variant_regulatory_elements`, `variant_gwas_snp`, and the three parent
stubs. `report_annotation_variant_regulatory_evidence` (918 lines) has
nothing to read.

That is a build-pipeline problem, not a report one, and it belongs to
ADR-003. It is recorded here because it defines the ground the migration
stands on.

The variant reports have to be rewritten whatever this ADR decides. That
moves the migration from an optimisation to a repair.

### 1.5 The built bundle deviates from ADR-003 §2.9

§2.9 specified hive-partitioned directories only
(`<table>/chromosome=N/`), parents dropped. The 20260910 bundle emits
flat per-chromosome files (`variant_masters/variant_masters_chr21.parquet`)
**and** keeps the parents as stubs. So `hive_partitioning = true` is a
no-op — `chromosome` survives only because it is a real column in the
files, and pruning happens through row-group statistics rather than
partition elimination.

This works, and §1.2 shows the statistics do prune. It is recorded here
because §1.3 and §1.4 are both downstream of the parent stubs existing
at all.

### 1.6 Provenance stops at the export boundary

ADR-003 §2.6 requires bundle identity to travel with results.
`ReportManager._stamp_bundle()` implements half of it, attaching
`bundle_id` to `DataFrame.attrs` — and its own docstring records the
gap: the attribute does not survive CSV export. The CLI exports with
`df.to_csv(output, index=False)` (`groups/report.py:508`), so every
result written to a file today loses its provenance.

ADR-003 left two questions open: how `bundle_id` should be derived, and
which stamping mechanism to use. This ADR closes both.

### 1.7 The target is already demonstrated

ADR-002 D2 built one report set-based over the bundle and measured it:

| approach                                | 712k inputs |
| --------------------------------------- | ----------- |
| per-input queries (current report shape) | not viable  |
| set-based over PostgreSQL                | ~2.5 min    |
| set-based over the parquet bundle        | **4.6 s**   |

The ADSP analysis is the same argument at production scale, on the LPC,
for a real cohort. Both were written report-by-report, outside the
report layer, by authors who knew to avoid it. This ADR makes that
shape the property of a module rather than of an author's discipline.

§5 reproduces the ADSP question on the 20260910 bundle.

---

## 2. Decision

### 2.1 Reports read parquet only

The report layer drops SQLite and PostgreSQL support. A report runs
against a bundle or it does not run.

This does **not** remove the ORM from the project. The models stay as
the schema contract: the ETL writes through them, `bundle build` stages
the core in SQLite through them (ADR-003 §2.3), and `db verify --schema`
compares a bundle against them. The boundary is directional — **models
are the write and validation path; DuckDB is the read path** — and this
ADR moves the report layer to the correct side of it.

§1.4 shows the write side has its own problem. Fixing it is a
precondition (§6), not part of this decision — but the direction is
settled: **the models follow the data.** The 4.3.0 variant schema is
intentional, and it follows one rule — **one table per source, no
cross-source joins, with the fields each source contributes declared in
that DTP's JSON config** (`dtps/config/dtp_*.json`). A report that wants
AlphaMissense reads the AlphaMissense table; it does not reconstruct it
by joining a shared predictions table.

### 2.2 A new module beside the old one; the legacy one is renamed and frozen

`biofilter/modules/report/` is renamed to
`biofilter/modules/report_legacy/` and frozen: bug fixes only, no new
reports, no new features. A new `biofilter/modules/report/` is created
with the contract below.

The rename happens **first**, not last. Naming the new module
provisionally would mean every migrated report, test and guide written
during the migration carries an import path that changes later. Renaming
the legacy module once, up front, means new code is born at its final
path and the migration ends with a deletion rather than a rename.

Migration is report by report. A report is migrated when someone needs
it; `report_legacy` shrinks and is deleted when empty.

**Amended 2026-09-15: the frozen module is reference, not a fallback.**
The original plan kept both live, with the CLI resolving native-first
and falling through. That was abandoned once the first report moved,
for a reason the migration made plain: the frozen reports are not a
working fallback. They were written against a relational database and
the 4.2.x variant schema, and several cannot run at all — they select
columns like `variant_masters.variant_id` that 4.3.0 bundles do not
carry. Presenting them in `report list` as things a user could run was
a promise the code could not keep.

So nothing imports `report_legacy`. Its package `__init__` no longer
exports anything, no manager instantiates it, and no URI reaches it.
What survives is the reason to keep the files: rewriting a report is
easier with the original in front of you. `report list` reports how many
are still waiting, counted from the directory listing rather than by
importing anything.

The cost is stated plainly: **24 reports are unavailable until they are
rewritten.** That is the price of the schema change in ADR-003, not of
this decision — it only stops hiding it.

`reports_bkp/` is deleted outright. It is superseded code, and git
retains it.

### 2.3 Bundles are opened from the manifest, not connected

A bundle is not a database and the new module does not connect to one.
It opens a directory:

```
Bundle.open(path)  →  validate (cheap tier, below)
                      read manifest.json
                      duckdb.connect()
                      register one view per logical table, from the manifest
    .con           →  the DuckDB connection
    .manifest / .bundle_id
    .cursor()      →  one per report execution
```

Views are built from the manifest's table entries, grouped by their
`table` field. There is no directory scan, no filename convention to
interpret, and no ordering to depend on — which removes §1.3 as a class
rather than patching its symptom.

**What this deletes.** No `Engine`, no `sessionmaker`, no `StaticPool`,
no `bootstrap_models`, no `Session`. The `_tolerant_json_deserializer`
goes too — it exists only because SQLAlchemy's generic dialect decodes
JSON a second time over `duckdb-engine`, and without SQLAlchemy in the
path that class of problem does not arise.

**The Session is already being bypassed.** Reports reach past it to the
raw connection 14 times (`session.connection()`, `session.bind`,
`session.get_bind`) — `pd.read_sql(sql, self.session.bind)` in the
`pg_*` reports, `conn = self.session.connection()` in
`annotate_variant.py:293`. The remaining 8 uses are
`session.flush()`, a unit-of-work call in a read-only report, needed
only because three reports INSERT into temp tables.

**Temp tables get real isolation.** Those three reports use fixed names
(`_bf_vre_ranges`, `_bf_gene_ranges`). Over `parquet://`,
`_engine_kwargs` forces `StaticPool` — "must share a single connection
across sessions", or the views vanish — and a DuckDB TEMP TABLE is
connection-scoped. So "one session per report" is today one connection
for everybody, and two reports in one process collide on the same name.
`con.cursor()` gives a thread-local connection over the same database:
the same per-execution shape, with the isolation that does not currently
exist.

`report_variant_annotation_expanded.py:40` states why the temp tables
are there: *"Uses a PostgreSQL TEMPORARY TABLE to accumulate results in
batches"* — a workaround for memory pressure. §5 shows DuckDB bounds
memory by declaration, so the workaround loses its reason to exist.

**Read-only stops being advisory.** ADR-003 notes the `read_only` flag
is "advisory, not enforced". A `Bundle` that can only open parquet has
no write path to disrespect.

**Verification is tiered**, because hashing 21 GB on every `report run`
is not an option:

| tier | checks | cost |
| --- | --- | --- |
| on open, always | manifest parses, `manifest_version` known, declared files present and the right size | 114 `stat()` calls — the size is free in the same syscall as presence |
| `db verify` | the same, explicitly, plus schema drift | milliseconds |
| `db verify --hashes` | full SHA-256 re-read | minutes; for after a transfer |

Note what "integrity" means today: **0 of the 114 tables carry a
`sha256`**, because the build runs with `checksums=False`. Presence and
size is a real check for the likely failure — a truncated copy to the
LPC — but it is not corruption detection, and `--no-hashes` is currently
inert.

**Enabling checksums is explicitly deferred.** The bundle already
identifies itself well enough for this work: `manifest.json` carries
`bundle_id`, and `build_record.json` carries the per-step provenance
behind it — DTP script, DTP version, source URL, extract hash and
timings. Identity and provenance are answered; integrity can wait until
something needs it.

### 2.4 Native DuckDB SQL, not the ORM query builder

Reports are written as DuckDB SQL. The reason is not ORM call overhead —
it is that the ORM cannot express what makes this workload fast:

- **Range joins.** Overlapping a gene interval with variant positions is
  the central Biofilter operation, and DuckDB has a dedicated operator
  for it (IEJoin), plus `ASOF JOIN` for nearest-position semantics.
  Through the ORM, `variant_gene_location_model` does the only thing
  available to it: iterates gene by gene, one query per region, with a
  hand-rolled "does this chromosome have variants?" cache
  (`report_variant_gene_location_model.py:493-508`). Natively this is one
  join, planned by the engine.
- **Arrow output.** `.all()` → `Row` → `dict` → `pd.DataFrame(records)`
  is the current path. §5 measures the alternative.
- **Set aggregation.** `QUALIFY`, `PIVOT`, `list_aggregate`, `UNNEST`
  collapse the Python grouping loops that dominate the annotation
  reports.

A secondary benefit, and not a small one for this lab: a report becomes
readable SQL, reviewable by a bioinformatician who does not read
SQLAlchemy.

The cost is losing the models as a compile-time check on column names.
Mitigation in §2.10 — and §1.4 shows that check was not working anyway.

### 2.5 Input is a registered relation, never an interpolated literal

Input lists are registered as an Arrow table and joined. They are never
interpolated into SQL text, and never expanded into an `IN (...)` list
of literals.

This is one decision serving two purposes. It removes SQL injection as a
category — the risk native SQL would otherwise introduce — and it turns
the `IN (10,000 literals)` pattern into a hash join, which is what makes
the `__ALL__` modes viable. ADR-002 D2 reached the same conclusion for
one report; here it is a rule.

### 2.6 A report returns a result object, not a DataFrame

```
ReportResult(
    table,        # Arrow table — the result
    provenance,   # bundle_id, report, params, biofilter version, timestamp
    artifacts=[], # additional outputs; empty for every report at first
)
```

`artifacts` is specified now and used later. The need already exists —
`report_variant_binning.py:1098` writes its own sidecar JSON with no
contract for it — and adding the field now costs nothing, while adding
it later means a second pass over every migrated report.

Rejected-row logs, inconsistency reports and multi-file outputs are
**not** designed here. Only the return type capable of carrying them is.

Returning Arrow rather than a DataFrame also keeps the streaming option
open: DuckDB's `.arrow()` yields a `RecordBatchReader`, so a caller that
does not need the whole result in memory does not have to materialise
it. The current path cannot offer that at all.

### 2.7 Results stay CSV and DataFrame; provenance rides alongside

The result of a report is what a person opens, so the output format
follows the reader, not the storage decision:

- **CLI:** CSV by default. It is what the lab opens, and nothing about
  parquet storage obliges a parquet result.
- **Notebook / Python API:** a DataFrame, as today.
- **Parquet:** available for a result large enough to warrant it.

Because CSV has nowhere honest to put metadata, **provenance is written
as a sidecar** `<output>.provenance.json` on every CLI export — the
primary mechanism, not a fallback. It carries `bundle_id`, the report
name, the parameters, the Biofilter version and the timestamp, which is
enough to find the matching `build_record.json` later. For a DataFrame,
`result.attrs` already carries `bundle_id` and keeps doing so; when the
output is parquet, provenance also goes into its key-value metadata.

`ReportResult.artifacts` (§2.6) — error logs, rejected rows,
inconsistency reports — are JSON or parquet, since nobody reads those in
a spreadsheet.

This closes ADR-003's open question on the stamping mechanism. An added
column was rejected: it changes every report's schema to carry one
constant.

### 2.8 `bundle_id` is build-derived, and is labelled as such

`bundle_id` is a SHA-256 over each table's `(name, rows, bytes)`
(`builder.py:651-659`), with per-file checksums disabled. It answers
"which build produced this result" and does not attempt to answer "has
this data been altered" — parquet rewriting is not byte-reproducible, so
identical data can yield a different id.

That is the right trade for an identifier, and this ADR keeps it,
closing ADR-003's open question. It is recorded explicitly so the
guarantee is not overread later.

### 2.9 The CLI surface is preserved, then improved

Unchanged, because they are correct: `--input` / `--input-file` for
input data, `--param KEY=VALUE` for options, `--bundle` for the bundle
path, `report list`, `report explain`, automatic discovery with no
support-code changes.

`report list` shows the reports that run, then how many are still
waiting to be rewritten — the migration's progress bar, and it reaches
zero when `report_legacy/reports/` empties.

Metadata commands open nothing. `report list`, `explain`,
`example-input` and `available-columns` answer from the installed
package, so a dead URI in `.biofilter.toml` no longer stops someone
asking what reports exist. `report run` needs a bundle and says so in
those words, rather than the generic "DB not set" left over from when a
database was an option.

Two additions the native path makes cheap:

- **`--explain-plan`** prints the DuckDB plan and the files the query
  will touch, without running it. On a 21 GB bundle, knowing a report is
  about to scan every chromosome is worth more than the report.
- **Provenance on export**, per §2.7.

**A bundle this Biofilter does not understand is an error, not a
degraded mode.** If the manifest declares a `manifest_version` or schema
newer than this install supports, `Bundle.open()` fails and says to
update Biofilter — which is a package upgrade, not a data migration. No
partial reads, no guessing at an unfamiliar layout.

### 2.10 Tests move to a fixture bundle

Report tests run against `sqlite:///:memory:` today
(`test_report_variant_gene_location_model.py:162`), across 9 test
files. Parquet-only makes that impossible, and the replacement is
better: a small committed fixture bundle — a few hundred rows per table,
with 2–3 partitioned chromosomes so pruning is exercised, and a parent
stub present so §1.3 stays fixed.

The fixture bundle also carries the column-name check that §2.4 gives
up: a report referencing a column the bundle does not have fails in
tests rather than in production. §1.4 is what happens without it.

### 2.11 `report_legacy` ends when the last report leaves

No version deadline. The module is deleted when it is empty, and
`report list` keeps the remaining count visible (§2.9) so the debt does
not go quiet.

**Done, 2026-09-16.** `biofilter/modules/report_legacy/` is deleted, and
with it `tests/unit/report_legacy/` — whose own conftest said it would
go the same day — plus `ReportComponent.pending_migration()` and the
`report list` footer that counted what was left. A progress bar at zero
is not information, and keeping it invites someone to add a row.

The deletion is safe because git holds the module: every retired report
is one `git show` away, which is what "reference material" was ever
worth (§2.2).

It also settles a question three other files were waiting on.
`db/base.py`, `db/models/model_variants.py` and `bundle/builder.py` each
keep retired model classes alive with a comment saying they exist *only*
because `report_legacy` imports them. Nothing imports them now, so that
comment is false and `RetiredBase` can go. Not done here: those files
belong to the bundle-build work in flight, and the class deletion is
theirs to make.

### 2.12 Reports fall into six classes, and the class predicts the work

Migrating seven reports made the shape of the remaining nineteen legible.
The classes are not bureaucracy — they answer two practical questions:
what does one output row mean, and where will the rewrite actually pay.

| class | one row is | reports |
| --- | --- | --- |
| **resolution** | a candidate match for one input name | `resolve_entity` |
| **annotation** | one input, enriched | the `annotate_*` family, `variant_single_gene_annotation`, `variant_annotation_expanded`, `annotation_variant_regulatory_evidence` |
| **expansion** | a link — an input, and something reached from it | `entity_neighborhood_summary`, `entity_relationship_model`, `variant_gene_location_model`, `gene_to_variant_filtering` |
| **pair generation** | a candidate pair to test downstream | `snp_snp_model`, `snp_snp_pair_generator`, `variant_modeling` |
| **aggregation** | a bin, or the result of a set operation | `variant_binning`, `variant_list_intersect` |
| **platform** | a fact about the bundle, not about biology | `etl_status`, `etl_packages`, `platform_data_statistics`, `db_pg_*` |

**"Pair generation", not "modeling".** Three reports carry `model` or
`modeling` in their names and none of them fits a model. They produce
candidate pairs — variant x variant, gene x gene — for statistical or ML
testing that happens elsewhere. The word is inherited from Biofilter 2
and 3, where "SNP-SNP model" meant "the candidate set to test", and it is
recorded here so nobody corrects it in the wrong direction. Whether the
reports themselves get renamed is §2.14.

**`entity_filter` became `resolve_entity`.** It filtered nothing. A
filtering report reduces a set; this one expands — one input becomes
every entity answering to it, so ambiguity surfaces as extra rows. Done
now rather than later, while seven reports exist and no lab script
depends on the name.

### 2.13 The speed argument does not apply uniformly

§5 measured 113x on materialisation and §1.1 blamed the fan-out. Both
hold — for reports that fan out. Migrating `resolve_entity` showed where
they do not.

Legacy against native, same bundle, same inputs:

| mode | 10 inputs | 100 | 500 |
| --- | --- | --- | --- |
| `exact` legacy | 0.08s | 0.04s | 0.04s |
| `exact` native | 0.09s | 0.05s | 0.09s |
| `fuzzy` legacy | 2.63s | 15.38s | 72.64s |
| `fuzzy` native | 0.16s | 1.29s | 6.31s |

In `exact` and `like` there is **no gain**, and at 500 inputs the
relational version is marginally ahead — the native one pays a fixed cost
to register the input relation. `like` returned the same 226 rows in the
same 0.08s from both.

The reason is worth stating plainly: the relational `entity_filter` was
already a single SQL query, executed by DuckDB through the `parquet://`
bridge. It had no fan-out and did no Python-side assembly, so there was
nothing for the migration to recover. The annotation reports were slow
because they issued nine to sixteen queries and merged the results in
dictionaries; this one issued one.

`fuzzy` is 12-16x because that mode *did* work in Python — it pulled all
912,316 aliases in and scored them there. The row counts differ, 6
against 79, because the scorers differ; it is not identical work.

**So the class predicts the payoff.** Annotation and pair generation fan
out, and will gain. Resolution and the platform reports mostly will not —
they are migrated for the contract, the provenance and the tests, not for
speed. Expansion and aggregation have to be measured one at a time.

Claiming a speedup a report does not have would make the next measurement
less believable, which is why this section exists.

### 2.14 Not decided here

- **Migration order.** Reports are migrated on request, one at a time;
  `annotate_gene` was first (§6).

  **Settled 2026-09-15 for two of them: `db_pg_index_stats` and
  `db_pg_table_stats` were deleted rather than migrated.** They read
  `pg_stat_user_indexes` and `pg_stat_user_tables` — PostgreSQL's own
  catalogue, which a bundle does not have and could not have. The
  question they answered, "how big is this and how is it laid out", is
  now the `storage` section of `platform_data_statistics`, read from the
  manifest. Retiring a report is as legitimate an outcome as migrating
  one, and this is the first case where it was the right one.

  **Settled 2026-09-15 for two more: `gene_to_variant_filtering` and
  `variant_annotation_expanded` were replaced by the single
  `expand_gene_to_variant`.** The first mapped genes to variants by
  positional overlap; the second was the same query with no filters and
  a gene list scraped out of another report's CSV by column name. The
  replacement keeps positional overlap, adds an optional window, and adds
  a second mechanism — VEP's own gene assignment — which the caller must
  choose between explicitly, because the two return different variants
  and nothing in the rows reveals which ran. Chaining is deliberately
  not carried over: a user who needs one report's output as another's
  input writes the file and reads the column, which is a thing they can
  look at.

  **Settled 2026-09-15 for four more: `variant_gene_location_model`,
  `variant_single_gene_annotation`, `snp_snp_model` and `variant_modeling`
  became the single `pair_variants`.** They were one pipeline written in
  three eras, each re-implementing the stages before it — and each
  carrying its own copy of the chromosome and rsID parsers, which had
  already drifted. Stage 1 places the input on genes; stage 2 connects
  those genes through a shared entity; stage 3 pairs the variants.
  `variant_gene_location_model` stopped after stage 1,
  `variant_single_gene_annotation` after stage 2, and the only difference
  between the remaining two was whether both sides of a pair had to come
  from the input — now the `membership` parameter. The pipeline's own
  vocabulary settles §2.12's open question: these reports generate
  candidate pairs, so the class is **pair generation** and the report is
  named for it.

  Two findings shaped the result rather than being footnotes to it. The
  bundle carries 38,092 Gene Ontology entities and **zero** GO
  relationships, so the legacy `group_entity_type='GO'` returned nothing
  and read as a finding; a group type the bundle cannot use is now
  refused by name. And group size is not a performance knob: a pathway
  naming 2,615 genes links its members while saying nothing about any of
  them. `max_group_size` defaults to 300, which keeps 98% of pathways
  while cutting the gene pairs they generate from 37.1 M to 6.9 M.

  **Settled 2026-09-15: `annotation_master_chemical` was deleted rather
  than migrated.** Not because a bundle could not hold chemicals — unlike
  the `db_pg_*` pair, this one is about data that is simply not there
  yet. The `Chemicals` entity group exists in the seeded taxonomy and
  holds **zero** entities; there is no `chemical_masters` table in the
  bundle; and the `chebi` source has no ETL package at all, meaning it
  has never run. Nine of the fourteen seeded groups are empty the same
  way: the taxonomy was seeded ahead of the data.

  Rewriting a report against a table that does not exist would mean
  guessing at its shape, which is the mistake §1.3 records and §2.1
  forbids. When a bundle carries chemicals, the report gets written
  against that bundle.

  **Deferred, not decided: `variant_binning`.** It bins a cohort's rare
  variants into gene, gene-group, locus-type or pathway bins, so it needs
  both the user's VCF and the bundle's gene ranges. The current bundle
  carries chromosome 22 only, and binning a whole-genome VCF against it
  would not fail — it would return bins covering one chromosome's worth
  of the cohort and say nothing about the rest. That is the failure mode
  this ADR exists to stop, so the report waits for a bundle with every
  chromosome rather than shipping with a caveat.

  **Settled 2026-09-16, and the migration is finished.** The last three
  legacy reports are gone. `snp_snp_pair_generator` was retired into
  `pair_variants`: `seed_vs_all` is `membership='either'`, `cross_gene`
  is guaranteed because every pair is across two distinct genes, and
  `all_vs_all` was dropped — pairing without biology is
  `itertools.combinations`, n²/2 rows carrying no information, and the
  legacy code itself pushed callers away from it.

  `variant_list_intersect` and `variant_binning` became
  `aggregate_cohort_variants`, because the first is the second stopped
  early: read the cohort's file, match it against the bundle, and only
  then aggregate into bins. `output_grain` chooses where to stop. This
  also tightens §2.12's definition of the aggregation class, which read
  "a bin, or the result of a set operation" and covered two unrelated
  row meanings; they are now stages of one pipeline.

  **The binning rewrite happened before the data to validate it exists,
  deliberately.** The bundle carries chromosome 22 only, so every real
  cohort file hits the case that matters most: variants on chromosomes
  the bundle cannot place. Writing that guard now means it is exercised
  by real data; once a full bundle lands the situation becomes rare and
  hard to test. What cannot be settled yet is performance at whole-genome
  scale, and §2.13 applies — no claim is made about it until a full
  bundle and a real VCF exist to measure. The shape is chosen for that
  day: DuckDB has no VCF reader, so cyvcf2 parses and numpy does the
  per-variant genotype arithmetic, while every join against the bundle
  and the final aggregation are SQL.

  Two arithmetic facts became guards while building it. A cohort of N
  samples cannot observe a minor allele frequency below 1/(2N), so a
  `maf_cutoff` under that keeps only variants nobody carries and empties
  every bin — correctly, and invisibly. And the legacy report's
  `gene_window_size=500000` was a spatial-index bucket size, not a
  biological window; carrying it over as one would have placed every
  variant in every gene within half a megabase. In DuckDB the index
  disappears into a range join and the parameter with it.
- **How `model_variants.py` is realigned with what the ETL writes**
  (§1.4). The direction is settled in §2.1; the table-by-table work
  belongs to the model and build layers.
- **Why `alphamissense` and `gtex_v10_eqtl` produced nothing** (§1.4).
  An ADR-003 matter, and a precondition for any report that needs
  predictors or eQTL evidence.
- **Stopping the parent stubs** and the partition layout of ADR-003
  §2.9. Both are ADR-003 matters; §6 step 2 works either way.
- **Whether the pair-generation reports get renamed** when they are
  migrated (§2.12). `snp_snp_model` and `variant_modeling` produce
  candidate pairs, not models. It is the same decision `resolve_entity`
  already went through, and the same argument applies: cheaper now than
  after the lab has scripted them.

---

## 3. Consequences

### Positive

- Reports are planned as whole queries by DuckDB instead of assembled
  row by row in CPython. §5 measures 113x on the materialisation step
  alone, at one fifth of the memory.
- Memory becomes a declared ceiling (`SET memory_limit`) instead of
  whatever Python allocates, and DuckDB spills rather than dying.
- Range joins over variant positions become a first-class operation
  rather than a per-gene loop.
- Relations resolve from the manifest, so §1.3 cannot recur.
- Provenance survives export, closing the gap in ADR-003 §2.6.
- Reports become reviewable by people who read SQL and not SQLAlchemy.
- Migration is incremental and reversible per report.
- ~5,700 lines of superseded code leave the tree immediately.

### Negative

- **24 reports are unavailable** until they are rewritten. Most could
  not have run against a 4.3.0 bundle anyway (§1.4), but the ones that
  could are gone too, and that is a real loss for anyone using them.
- Losing the ORM means losing static column names. §2.10 replaces it
  with a test, which catches the same errors later in the cycle.
- The `parquet://` ORM bridge is no longer needed by reports, but
  `db verify --schema` still uses it, so it stays.
- Rebuilding 9 test files onto a fixture bundle is mechanical work with
  no visible output.
- A report written in SQL is harder to compose than one written in
  Python. Shared logic needs a deliberate answer (CTE library, SQL
  templates) rather than falling out of inheritance.
- Opening from the manifest makes the manifest load-bearing. A bundle
  with a stale or hand-edited manifest fails to open rather than
  degrading — which is the intent, but it is a new failure mode.

### Neutral / mitigations

- **SQL injection**, introduced by native SQL, is removed by §2.5 —
  which was already required for performance.
- **`report_legacy` becoming permanent** is the realistic failure mode
  of any parallel-module migration. Mitigation: `report list` prints the
  outstanding count every time it runs, so the debt is visible in normal
  use rather than by reading the tree.
- **Dialect portability** is lost by design, per §2.1.
- **The speed gain is not uniform.** It concentrates in variant-scale
  reports. `etl_status` and `etl_packages` become manifest reads and
  gain nothing measurable, because they were never slow.

---

## 4. Alternatives Considered

### Alternative A — Optimise the reports in place, keep the ORM

Fix the predicates, collapse the fan-out, keep SQLAlchemy. Cheapest, and
it would recover much of the performance. Rejected because it does not
change what the layer encourages: the next report written under the same
contract reintroduces the same patterns, and the ORM still cannot
express a range join. §1.4 also means the variant reports need rewriting
regardless, which removes most of the cost saving.

### Alternative B — A subpackage inside the existing module

`modules/report/reports_native/`, discovered by the same manager.
Smaller diff, no rename. Rejected because the two contracts genuinely
differ — session plus DataFrame versus DuckDB connection plus
`ReportResult` — so one manager would carry two code paths with no
forcing function to ever remove one.

### Alternative C — Name the new module provisionally, rename at the end

Rejected in §2.2: it makes every file written during the migration carry
a path that changes later, which is strictly more churn than renaming
the frozen module once.

### Alternative D — Rewrite all 25 reports at once

A clean cut with no coexistence and no routing. Rejected: 16,443 lines
rewritten before anything ships, with no incremental validation. The
reports also differ enormously in value — some are load-bearing, some
have never been run twice.

### Alternative E — pandas/pyarrow directly, no SQL layer

Rejected for the same reason ADR-001 Alternative C rejected it: it
discards the query planner, and range joins and set aggregation would be
hand-written against 2.2 billion rows.

### Alternative F — Keep the Database/session machinery, swap only the query style

Write native SQL but keep `Engine`, `Session` and the `parquet://`
bridge underneath. Rejected: the Session contributes nothing to a
read-only analytical query, reports already bypass it 14 times to reach
the raw connection (§2.3), and keeping `StaticPool` keeps the temp-table
collision. The machinery is not neutral overhead; it is actively in the
way.

---

## 5. POC results (2026-09-14)

Measured on the 20260910 bundle (21 GB, 114 declared tables), macOS, 6
threads, `memory_limit = 12GB`. Peak memory is `maximum resident set
size` from `/usr/bin/time -l`.

### 5.1 A production-scale question, end to end

The ADSP step_01 question — which of a cohort's variants map to a
protein-coding gene — rewritten against the 4.3.0 schema. 711,836 input
variants against `variant_masters` (177,520,333 rows) and
`variant_molecular_effects` (2,238,929,441 rows).

```
parse input                         0.2s
coding genes                        0.0s
match variant_masters (177M)        2.5s
join molecular_effects (2.2B)       8.9s
coding filter + aggregate           0.1s
─────────────────────────────────────────
TOTAL                              11.9s     peak RSS: 1.87 GB
```

| | |
| --- | ---: |
| inputs (autosomal) | 711,836 |
| matched in `variant_masters` | 711,651 |
| VEP annotation rows | 8,163,820 |
| variants with a coding gene | 356,082 |

The ADSP run on the 4.2.0 bundle reported 355,710 for the same cohort —
0.1% apart. The rewritten query is a simplification (it matches on
`symbol` only, without the Ensembl-id fallback or the `locus_type`
filter), so the agreement is a sanity check, not an equivalence proof.

Peak memory was 1.87 GB against a declared ceiling of 12 GB.

### 5.2 Materialisation, isolated

The same query, the same 69,973,856 rows (`variant_molecular_effects`,
chromosomes 21–22, 8 columns), changing **only** how the result is
handed back:

| | time | peak RSS |
| --- | ---: | ---: |
| `fetchall()` → dicts → `pd.DataFrame` — the current shape | **442.0 s** | **20.98 GB** |
| `fetch_arrow_table()` | **3.9 s** | **4.04 GB** |

**113x in time, 5.2x in memory**, for the materialisation step alone —
before the fan-out of §1.1 or the predicate shape of §1.2 is addressed.
The difference is 70 million Python dictionaries against columnar
buffers.

The 20.98 GB also explains the batch-accumulation temp tables of §2.3:
they are not fastidiousness, they are what keeps the process alive.

A third tier exists that the current path cannot reach: `.arrow()`
without `read_all()` returns a streaming `RecordBatchReader`, so a
caller can consume batches without materialising the 4 GB at all.

### 5.3 What is *not* claimed

- The 113x is the materialisation step, not a report end to end. A
  variant-scale report should gain more, since the fan-out also
  disappears; `etl_status` gains nothing, because it was never slow.
- Opening from the manifest (§2.3) contributes **no** measurable speed.
  It trades 114 `stat()` calls for one JSON read. Its value is
  correctness: §1.3 and §1.4.
- No head-to-head against the current reports was possible. They query
  `variant_masters.position_start`, which the bundle does not carry
  (§1.4), so they cannot run against this data at all.

---

## 6. Implementation outline

1. **Realign `model_variants.py` with what the ETL writes** (§1.4), and
   make `db verify --schema` compare against the partitioned data rather
   than the stub. Nothing below is trustworthy until the models, the
   data and the check agree.
2. Register views from the manifest (§2.3), and fix the
   double-`connect()` reset. This retires §1.3 for `report_legacy` too,
   which is what lets unmigrated reports keep working.
3. Delete `reports_bkp/`.
4. Rename `modules/report/` → `modules/report_legacy/`; update imports,
   the CLI and tests. No behaviour change.
5. Create `modules/report/` with `Bundle`, `ReportBase`,
   `ReportManager`, `ReportResult`, input registration (§2.5) and
   provenance export (§2.7).
6. Build the fixture bundle (§2.10).
7. Pilot A — `annotate_gene` (687 lines, 9 queries). It reads
   eight populated core tables plus the partitioned `variant_masters`,
   so it exercises the fan-out collapse, input registration, aliases,
   groups, locations and relationships in one report. Every table it
   needs has rows (§1.4).
8. Pilot B — `variant_gene_location_model` (946 lines). The range-join
   case, and a report §1.4 has already broken; this is what proves §2.4.
9. Pilot C — `etl_status` and `etl_packages`. Both read provenance the
   manifest and `build_record.json` already carry, so they become
   manifest readers with no query at all.
10. Review the contract against what the three pilots taught before
    migrating anything else.
11. CLI routing and `report list` engine markers (§2.9).

---

## 7. Open questions

- ~~**Composition.**~~ **Answered 2026-09-15 by the annotation family.**
  `annotate_gene` was written alone; migrating disease, GO,
  pathway and protein showed its shape was the shape of all of them.
  `reports/_annotation.py` holds what repeats — the resolution step, and
  functions returning CTE text for relationships, aliases, cross-
  references and provenance. Each report still writes its own query.

  Two things kept it from becoming a framework. It is **SQL text and one
  method**, not a class hierarchy that owns execution; and a report
  overrides by simply not calling a helper, as `annotate_protein`
  does when it rewrites `resolved` to follow an isoform to its canonical
  entity. Whether this holds for the variant reports, whose shape is
  genuinely different, is still open.
- **Where `ReportResult.artifacts` files are written**, and whether the
  CLI or the report owns the naming.
- **What `db verify` should assert about schema.** It is the right home
  — it already exists, needs no database, and is where an operator
  looks. Two changes are needed and their shape is open: compare the
  models against the partitioned data rather than the parent stub
  (§1.4), and add a check that each DTP's declared Arrow schema matches
  the model it feeds, so §1.4 becomes self-defending.
- **Whether `db verify` should flag empty declared tables.** Ten are
  empty today and the command reports the bundle as valid. Some
  emptiness is legitimate (`chemical_masters` was never loaded); some is
  a silent build failure (§1.4).
