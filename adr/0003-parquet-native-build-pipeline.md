# ADR-003: Parquet-Native Build Pipeline and Immutable Bundles (4.3.0)

| Field      | Value                                                                                                                                                     |
| ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Status     | **Proposed** (measurements taken 2026-09-08 against the 4.2.0 bundle)                                                                                     |
| Date       | 2026-09-08                                                                                                                                                 |
| Author     | Andre Rico                                                                                                                                                 |
| Supersedes | (none) — extends [ADR-001](0001-duckdb-parquet-strategy.md)                                                                                                |
| Related    | [ADR-001](0001-duckdb-parquet-strategy.md) (parquet/DuckDB read mode), [ADR-002](0002-cohort-coding-gene-overlap-report.md) (cohort report over parquet)  |

---

## 1. Context

ADR-001 made Parquet + DuckDB the **read** path for HPC workloads, while
PostgreSQL remained the canonical **write** path: the ETL wrote to PG,
and `db export` produced a bundle from it. The bundle was a derived
artifact of a relational database that had to exist first.

That remaining PostgreSQL dependency is now the main source of
operational cost, and the numbers are decisive.

### 1.1 Measured cost of the relational write path

Taken from the live `biofilter_dev` (chromosomes 1–3 only) and from the
full-genome 4.2.0 bundle at `bf_files/manifest.json` (built 2026-08-27,
chromosomes 1–25):

| `variant_molecular_effects` | rows          | size    | bytes/row |
| --------------------------- | ------------- | ------- | --------- |
| PostgreSQL (chr 1–3)        | 437,905,822   | 104 GB  | 255       |
| Parquet (chr 1–25)          | 1,793,092,126 | 6.65 GB | 4.0       |

A **64x** difference in per-row footprint. Extrapolating the PostgreSQL
rate to the full genome puts that single table at roughly **426 GB**.
The same data is 6.65 GB in Parquet.

### 1.2 The relational core is small

Splitting the bundle's 40 logical tables:

| group                          | tables | rows          | parquet   |
| ------------------------------ | ------ | ------------- | --------- |
| Relational core (non-variant)  | 29     | 6,975,385     | **105 MB** |
| Variants                       | 11     | 1,966,172,066 | 15.6 GB   |

The entire non-variant model — entities, aliases, relationships, genes,
proteins, pathways, diseases, GO, chemicals — is 105 MB. The largest
single table is `entity_relationships` at 4.06 M rows. This is
laptop-scale data being hosted on a database server sized for the 2
billion variant rows sitting next to it.

### 1.3 Variants are already schema-isolated

`variant_masters` carries no `entity_id` and no foreign keys — the model
is explicit about it (`# Provenance (no FK)`,
`model_variants.py:135`). `variant_molecular_effects` states
`No physical FK constraints (ETL-enforced integrity)` and links to genes
through raw `gene_id` / `gene_symbol` strings emitted by VEP, never
through an entity surrogate.

The variant DTP's only coupling to the database is
`_load_dimension_cache` over three small seed dimensions —
`variant_consequences`, `variant_impacts`, `variant_biotypes`
(`dtp_variant_gnomad.py:1976`). It reads nothing from genes, proteins or
entities.

### 1.4 Half of the load step is already redundant

`dtp_variant_gnomad.py` already writes Parquet during `transform`
(`_write_parquet_part`, line 556). The `load` step then **re-reads that
Parquet** and `COPY`s it into PostgreSQL stage tables (line 1092), from
which `db export` later writes Parquet again. The canonical variant data
is produced in the target format and then round-tripped through a
database for no downstream benefit.

### 1.5 Bundle identity is unreliable today

The 4.2.0 bundle's in-band metadata disagrees with its own manifest:

| field in `biofilter_metadata` | value           | manifest says |
| ----------------------------- | --------------- | ------------- |
| `schema_version`              | 4.1.0           | 4.2.0         |
| `etl_version`                 | 4.1.0           | 4.2.0         |
| `build_hash`                  | `None`          | —             |
| `created_at`                  | 2026-03-17      | 2026-08-27    |

The row was written when the source database was first created in March
and never updated by a build. A consumer reading version information
from the tables — the normal path — gets the wrong answer.

`DatabaseManager._bundle_manifest()` (`database.py:191`) can read the
correct values, but it is the only occurrence of that symbol in the
codebase: nothing consumes it. `ReportManager.run` returns the result
object unmodified (`report_manager.py:275`), so no report output records
which bundle produced it.

### 1.6 Internal IDs already differ between environments

`entities.id` is an autoincrement assigned by `get_or_create_entity`
(`entity_query_mixin.py:12`) in ETL execution order. Comparing the same
symbols in `biofilter_dev` and in the 4.2.0 bundle:

| symbol | bundle | dev   | dev's ID resolved **in the bundle** |
| ------ | ------ | ----- | ----------------------------------- |
| APOE   | 11448  | 11450 | APOF                                |
| BRCA1  | 22849  | 22854 | BRCC3P1                             |
| TP53   | 37296  | 37311 | TP53TG3B                            |
| CFTR   | 42679  | 42699 | CH25H                               |

The drift is small, so an ID taken from one environment still resolves
in the other — to a different, biologically adjacent gene. No query
fails; the answer is silently wrong. This behaviour exists today and is
noted in `CLAUDE.md` ("never export from it over the bundle").

---

## 2. Decision

### 2.1 Bundles are immutable, versioned releases

A bundle is built once and never updated in place. Refreshing data
produces a **new** bundle (`biofilter/<YYYYMM>/`), and consumers pin to
the version they ran against. There is no incremental update path, no
"is this row already loaded?" reconciliation, and no migration of an
existing bundle to a newer shape.

This is the decision the rest of the ADR follows from.

### 2.2 The build pipeline

```
                    ┌──────────────────────────────────────┐
   core DTPs  ─────▶│  throwaway SQLite (staging, ~105 MB) │──┐
   (29 tables)      │  get_or_create_*, cross-DTP lookups  │  │
                    └──────────────────────────────────────┘  │
                                                              ├──▶ parquet
                    ┌──────────────────────────────────────┐  │    tables/
   variant DTPs ───▶│  parquet parts, written directly     │──┘      +
   (11 tables)      │  no relational hop                   │      manifest.json
                    └──────────────────────────────────────┘
```

1. **Core branch.** The 29 non-variant tables are built in a throwaway
   SQLite database. These DTPs need row-at-a-time `get_or_create_*` with
   read-back, and depend on each other's output (HGNC before Ensembl,
   entities before relationships). A transactional row store is the
   right engine for that phase.
2. **Variant branch.** The 11 variant tables are written straight to
   Parquet, in parallel with the core branch. Justified by §1.3: the
   only shared state is three seed dimensions.
3. **Dump.** The SQLite core is written out to Parquet.
4. **Manifest.** `manifest.json` is written last, covering both branches.
5. **Archive.** The SQLite file is retained as a build artifact
   (~105 MB), outside the published bundle. It is the only thing that
   can reproduce or diff a build after the fact.

### 2.3 SQLite is for staging the core only — never for variants

ADR-001 §4 rejected SQLite after three failed LSF jobs. That rejection
stands and is not contradicted here: it was measured against
`variant_molecular_effects` at 1.79 B rows, with FK enforcement and 90+
indexes. This ADR puts **6.98 M rows and 105 MB** in SQLite and keeps
every variant row out of it.

The boundary is load-bearing. If a variant table ever gains an
`entity_id` foreign key, the parallel branch can no longer be built
independently — the core branch would still be assigning those IDs when
the variant branch needs them. **No variant table may reference an
entity surrogate.** Cross-domain links use natural keys
(`gene_symbol`, `HGNC:xxxx`).

### 2.4 Why not DuckDB for staging too

DuckDB is already a dependency, and `COPY ... TO '*.parquet'` would
remove the dump step. It is still the wrong engine for the core branch:
the ETL is built on `get_or_create_*`, which is single-row insert
followed by read-back — the operation a columnar engine is worst at.
SQLite stages; DuckDB reads.

### 2.5 Internal IDs are scoped to a bundle

`entities.id`, `variant_masters.variant_id` and every other surrogate
are **internal row identifiers valid only within their bundle**. They
carry no meaning across versions, and no attempt is made to stabilise
them.

Deterministic (hash-derived) IDs were considered and rejected: with
immutable bundles, reproducibility comes from pinning the version, which
is a stronger guarantee. A stable ID would keep the same key pointing at
data whose content changed upstream — indistinguishable from a correct
result. Pinning covers both identity and content drift.

### 2.6 Bundle identity travels with the results

Because §2.5 makes internal IDs meaningless outside their bundle, bundle
identity must be inseparable from anything that carries one:

- `biofilter_metadata` is written **by the build**, with a real
  `build_hash`, replacing the stale March row described in §1.5.
- A `bundle_id` derived from the build (a hash over the manifest) is
  exposed through the Python API and the CLI.
- Report output carrying entity or variant IDs is stamped with that
  `bundle_id`.

This replaces the ID-stability guarantee with a **detectability**
guarantee: an orphan ID can be recognised as one. That is the honest
scope — it does not attempt to make the mistake impossible.

### 2.7 Drop `schema_version` and `etl_version`

Per-DTP data provenance (which gnomAD release, which HGNC dump) is
sufficient and is already tracked in `etl_data_sources` /
`etl_packages`. The global version fields are removed rather than fixed:
§1.5 shows a field nobody writes and everybody may read is worse than no
field.

Structural compatibility — "can this BF4 build read this bundle?" — is
**discovered, not declared**. Parquet is self-describing, so the reader
introspects columns when opening a bundle and fails loudly at connection
time rather than mid-report inside a DuckDB binder error.

### 2.8 Remove Alembic

With staging created fresh from the models on every build and no
persistent database anywhere in the pipeline, there is nothing to
migrate. `create_all` replaces the migration chain.

Removed: `alembic.ini`, `biofilter/alembic/`, the `db migrate` and
`db upgrade` CLI commands, and the `alembic_version` table.

This also answers ADR-001 §8's open question on bundle-side schema
migrations: there is no migration story, because bundles are never
migrated. A schema change produces a new bundle.

### 2.9 Stop duplicating partition children

The 4.2.0 bundle sets `partition_children_included: true`, writing each
variant table both as a parent and as 25 `_chr_N` children — 31 GB on
disk for 15.6 GB of data. 4.3.0 emits hive-partitioned directories only
(`<table>/chromosome=N/`), which the `parquet://` reader already
registers as a single view with the partition key as a real column.

### 2.10 Not decided here

**DTP slimming.** Removing the "already inserted?" checks now that every
write is an insert is a direct consequence of §2.1, but the DTP-by-DTP
review is deferred.

**`model_curation.py` is a misnomer, not dead code.** It defines one
table, `omic_status` (6 rows: active, deactive, merged, deleted,
conflict, not_defined). It is written by `gene_query_mixin.py:152`, is
indexed on `gene_masters` (`base_dtp_turning.py:124`), and HGNC, MONDO,
NCBI and ChEBI depend on its `active`/`deactive` values. It stays;
renaming the file to `model_status.py` is suggested. The other
"curation" references in the codebase are ClinGen source data, which is
unaffected.

---

## 3. Consequences

### Positive

- No PostgreSQL anywhere in the pipeline. The production read path
  (ADR-001) and the build path converge on one format.
- The variant `load` step disappears — Parquet is no longer round-tripped
  through a database (§1.4).
- The build becomes portable: 105 MB of staging plus streaming Parquet
  writes runs on a laptop, not a database server.
- Alembic, `db migrate`, `db upgrade` and the `alembic_version` table are
  gone.
- Published bundle size drops from 31 GB to ~15.6 GB (§2.9).
- Bundle identity becomes trustworthy and travels with results.

### Negative

- **Rebuild is all-or-nothing.** Correcting one DTP means rebuilding the
  bundle. Acceptable at 105 MB of core, but the variant branch is hours
  of work, so per-branch resume matters in the implementation.
- **IDs are explicitly not portable across bundles.** This is now a
  documented property rather than an accident, but it must be stated in
  user-facing docs, not just here.
- **Storage grows monotonically.** Immutable versioned bundles mean
  ~15.6 GB per release retained. A retention policy is needed.
- **The parallel-branch invariant is unenforced.** §2.3 depends on no
  variant table referencing an entity surrogate; nothing currently
  prevents someone from adding one.

### Neutral / mitigations

- The 4.2.0 bundle at `bf_files/` is a ready-made acceptance test: same
  table set, known row counts, per-file sha256 in the manifest. The new
  pipeline can be validated table-by-table against it without waiting
  for a full ETL run.
- `variant_impacts` (325 rows) and `variant_biotypes` (56 rows) have no
  seed JSON — they are created as new values are discovered, so their
  IDs depend on input order. They need seeding for reproducible output.
- Dropping the `_chr_N` children changes bundle layout; the
  `parquet://` reader already handles hive partitioning, so this is a
  build-side change only.

---

## 4. Alternatives Considered

### Alternative A — Keep PostgreSQL as the canonical store

Rejected on §1.1: ~426 GB for one table, against 6.65 GB in Parquet, to
serve a read path that no longer uses PostgreSQL.

### Alternative B — DuckDB as the staging engine

Rejected on §2.4: the core ETL is row-at-a-time `get_or_create_*`.

### Alternative C — Deterministic (hash-derived) entity IDs

Rejected on §2.5. Reconsider only if a cross-bundle join becomes a real
requirement.

### Alternative D — SQLite for everything, variants included

Rejected: measured and failed in ADR-001 §4.

### Alternative E — Delete the staging SQLite after the build

Rejected. At 105 MB it is the cheapest reproducibility and
release-diffing artifact available; discarding it makes builds
irreproducible to save storage that is not scarce.

---

## 5. Implementation outline

1. Seed `variant_impacts` and `variant_biotypes` (§3, mitigations) —
   prerequisite for reproducible output.
2. Split the ETL orchestrator into a core branch and a variant branch
   with independent resume.
3. Core branch: target a throwaway SQLite built via `create_all`.
4. Variant branch: drop the `load` step; promote the existing
   `_write_parquet_part` output to final.
5. Dump the SQLite core to Parquet; emit hive-partitioned variant
   directories with no duplicated children.
6. Write `biofilter_metadata` from the build; compute `bundle_id`;
   emit the manifest.
7. Expose `bundle_id` through the API/CLI and stamp report output.
8. Introspect columns when opening a bundle; fail loudly on mismatch.
9. Remove Alembic and its CLI commands.
10. Validate the result against `bf_files/` table by table.

---

## 6. Open questions

- **`bundle_id` derivation.** Hash over the manifest is the obvious
  choice, but the manifest contains `created_at`, so two identical
  builds would produce different IDs. Decide whether `bundle_id` should
  be content-derived (reproducible) or build-derived (unique per run).
- **Stamping mechanism for reports.** DataFrame attribute, an added
  column, or export-header only. An attribute is invisible in CSV; a
  column changes report schemas.
- **Enforcing the §2.3 invariant.** A model-level test asserting no
  variant table declares an entity FK would make the parallel-branch
  assumption self-defending.
- **Retention policy.** How many historical bundles are kept, and where.
- **Failure semantics of the parallel branches.** If the variant branch
  fails after the core branch succeeded, is a partial bundle published,
  or is the whole build discarded?
