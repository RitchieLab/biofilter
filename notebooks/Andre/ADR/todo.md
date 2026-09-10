# TODO — ADR-003 implementation (4.3.0)

Working checklist for [ADR-003](0003-parquet-native-build-pipeline.md).
Decisions live in the ADR; this file tracks execution state only.

**Branch:** `release/4.3.0`
**Acceptance target:** the 4.2.0 bundle at `../../../bf_files/` — same table
set, known row counts, per-file sha256 in its manifest.
**Last updated:** 2026-09-10, after the first full build: 22 sources, 96
tables, 2.2 GB, valid.

Legend: `[ ]` todo · `[~]` in progress · `[x]` done · `[!]` blocked

---

## Ground truth (measured 2026-09-08, do not re-derive)

The core/variant split **already exists in the data** — no new metadata
is needed. `etl_data_sources.data_type` partitions the 43 active sources:

| branch      | sources | data_type values                                                        |
| ----------- | ------- | ----------------------------------------------------------------------- |
| **core**    | 16      | Gene, Protein, Pathway, Disease, Disease Relationships, GO, Chemical, Relationships |
| **variant** | 27      | Variant (`alphamissense`, `gnomad_chr1`..`chry` ×24, `gtex_v10_brain_eqtl`, `gwas`) |

Implementation style of the variant DTPs, which drives the effort:

| DTP                         | lines | parquet writes | `self.session` uses |
| --------------------------- | ----- | -------------- | ------------------- |
| `dtp_variant_gnomad`        | 2069  | 4              | 3                   |
| `dtp_variant_alphamissense` | 783   | 1              | 1                   |
| `dtp_variant_eqtl_gtex`     | 851   | 1              | 1                   |
| `dtp_variant_ncbi`          | 635   | 1              | 1                   |
| **`dtp_gwas`**              | 648   | 1              | **16**              |

### `dtp_gwas` — resolved 2026-09-09

Flagged here as the hard case because of its 16 `self.session` uses. Most
of them turned out not to be id resolution: `DELETE FROM` to clear a
prior load (gone — bundles are immutable), a bulk `insert()`, and the
rebuild of the `variant_gwas_snp` helper table.

Rewritten parquet-native. The helper table is absorbed by exploding
multi-SNP associations inline (336,159 rows carry a secondary SNP), the
same treatment VEP consequences get. It stays a **single unpartitioned
file**: its rows are associations (study x trait x SNP), not variants, so
there is no 1:1 with `variant_masters`, and 16.4% carry no position to
partition on.

Two recoveries fell out of it: `platform` (10,186 distinct values) was
commented out of the column selection while the model declared it, so the
bundle holds it as all-null; and the 255-character truncation of every
text field — a relational column limit — was cutting
`initial_sample_size` at 255 where the source has 745. `cnv` stays
dropped: populated on every source row but with a single distinct value.

---

## Phase 0 — Prerequisites

- [x] ~~Seed `variant_impacts` and `variant_biotypes`~~ — **dropped, not
      needed.** Measured on real chr21 data (24.3 M rows): normalised
      ints cost 7.33 MB, denormalised strings 7.46 MB — **1.8%**.
      Dictionary encoding makes the FK indirection pointless in parquet,
      while costing three seed tables, three load-time lookups,
      discovery-order-dependent IDs and a join on every query. The new
      DTPs carry the strings inline. (The current schema already declared
      `consequence_raw` alongside `consequence_id` and left it 100% NULL.)
      Side effect: `biotype_id` is typed `DOUBLE` today — a nullable int
      that went through pandas — costing 4.52 MB vs 0.18 MB for
      `impact_id`. Denormalising removes it.
- [x] ~~Decide `bundle_id` derivation~~ — content-derived. Since a
      bundle cannot be rebuilt once its sources move on, the id's job is
      to identify and verify the artifact, not to label a run, and a
      content hash can be recomputed from the finished bundle.
- [x] ~~Decide the `dtp_gwas` question~~ — resolved above; rewritten
      parquet-native as a single unpartitioned file.

### New gnomAD DTPs — design settled 2026-09-08

Two data sources per chromosome, each producing exactly one parquet, with
no dependency between them (they join downstream on the natural key
`chrom:pos:ref:alt`, verified unique — 2,025,847 rows / 2,025,847 keys on
chr21, zero orphan effects):

| data source          | release | files | output                          |
| -------------------- | ------- | ----- | ------------------------------- |
| `gnomad_joint_chr<N>` | 4.1     | 1     | variants (frequencies)          |
| `gnomad_vep_chr<N>`   | 4.1.1   | 2     | VEP, one row per consequence    |

Why split rather than one merged file per chromosome — storage does not
decide it (merged 193.5 MB vs split 192.1 MB on chr21, **1.01x**), the
query semantics do:

```
count variants with af<0.01, chr21
  SPLIT  (reads the variants file only)      5.7 ms  ->  1,778,604
  MERGED (needs COUNT DISTINCT)             36.2 ms  ->  1,778,604
  MERGED without DISTINCT                   19.9 ms  -> 21,948,350   WRONG
```

With a 12x VEP fanout, any `count(*)` that forgets `DISTINCT` is silently
wrong by an order of magnitude. Splitting also lets the joint branch ship
a usable variants parquet before exome/genome are even downloaded, and
lets VEP logic be rebuilt without rewriting frequencies.

Measured facts backing the design:

- **VEP lives only in exome/genome.** INFO field counts: exomes 415 (has
  VEP), genomes 242 (has VEP), joint 664 (**no VEP**). The joint's 664
  fields are all frequency — AC/AN/AF/nhomalt across 102 population
  breakdowns, faf95/faf99, CTT tests, histograms — plus
  `exomes_filters` / `genomes_filters`, which say which callset a variant
  came from.
- **The VEP format is identical across exome and genome** — 46 fields,
  same order. No per-source mapping; only a precedence rule when a
  variant is in both. Field 24 is `HGNC_ID`, a stable natural key to
  genes, better than the `gene_symbol` the old DTP uses.
- **Field lists are include-lists, not exclude-lists.** 1,321 INFO fields
  across the three sources; an exclude-list would silently adopt whatever
  a future release adds.
- **Downloads are sequential.** Measured against this origin: 1 stream
  64.2 MB/s, 4 parallel range streams 66.6 MB/s — the local link
  saturates, so concurrency buys ~4%. Resume is what matters.
- **Full genome is 1.53 TB of raw downloads** (joint 816.7 GB, genomes
  526.8 GB, exomes 185.6 GB) against 348 GB free. The pipeline cannot
  retain raw files; process-and-discard per chromosome, or point
  `data_root` at external storage. **Not yet in the ADR.**
- **~6h37 of continuous download** at 64 MB/s for the full genome, which
  is why resume is a prerequisite rather than polish.

Decisions taken:

- [x] Two data sources per chromosome (48 total), seeded **inactive** —
      `etl update-all` only runs active sources, so an accidental 1.53 TB
      pull is not one command away. Activate per chromosome.
- [x] URL templates + release pins live in the DTP, not the seed. The
      URLs vary only by chromosome, so 48 seeded strings would make a
      release bump a 48-edit change. Follows the existing ClinGen
      precedent (`dtp_clingen.py:214`), which hardcodes its base URL and
      iterates endpoints.
- [x] Resumable downloader added to `DTPBase` as
      `http_download_resumable` — the existing `http_download` has no
      retry and no resume, and is left untouched for the current DTPs.
      Uses the origin's `x-goog-hash` md5 as the package hash instead of
      re-reading a 20 GB file from disk; per-file hashes are folded into
      one composite digest for the extract skip-logic.
- [x] ~~`dtp_variant_gnomad` (old) left intact~~ — removed in
      `38f754e`. Generating the first plan showed it and its 24 data
      sources were still active, so a build would have run both
      gnomAD pipelines over the same data. Its mentions in ADR-002
      and ADR-003 are left alone: they are historical records, and
      ADR-003 §1 cites measurements from it as the evidence for this
      migration.
- [x] ~~ETL dependency module~~ — **not needed.** The order lives in
      the plan instead: sources run in the order listed, and that order
      *is* the dependency declaration. No graph, no topological sort, and
      it travels into the manifest as provenance. The generated order is
      declared in `CORE_ORDER` rather than taken from the database, whose
      id order is seed insertion order and had `gene_ncbi` before `hgnc`
      — the reverse of what the gene load needs. Validated by the full
      build.

## Phase 1 — Strip the relational assumptions ✅ done 2026-09-09

Removed in `456af0c`. With no persistent database anywhere in the
pipeline there was nothing to migrate.

- [x] `alembic.ini`, `biofilter/alembic/` (env.py, script.py.mako, two
      migrations), `biofilter/modules/db/migrate.py`,
      `biofilter/utils/migrate.py` — which nothing imported — and the
      dependency itself.
- [x] `db migrate` removed; `db upgrade` became seed-only.
      `DBComponent.migrate()` gone with it.
- [x] `tests/unit/db/test_db_migrate.py` deleted. Five more tests covered
      the removed paths: the two upgrade ones were rewritten to assert
      the seed pass happens and no migration hook is left; the rest went
      with the behaviour they tested.
- [x] `create_db` already used `create_all`, so staging needed no change.
- [x] `biofilter_metadata.schema_revision` stopped claiming an Alembic
      head that no longer exists (`a06d012d7d00`) and carries the package
      version. `schema_version` / `etl_version` were not dropped — Phase 4
      rewrites the whole row at build time instead.
- [ ] Rename `model_curation.py` → `model_status.py`. Still open, and
      cosmetic: it holds `omic_status` (6 rows), is written by
      `gene_query_mixin.py:152`, and HGNC/MONDO/NCBI/ChEBI depend on its
      `active`/`deactive` values. The file name is the only thing wrong.

The `alembic_version` guards in `transfer.py` stay on purpose: legacy
databases and bundles still carry that table, and export/import read from
whatever engine they are given.


## Phase 2 — Core branch (SQLite staging) — validated 2026-09-09

Run end to end with `hgnc, gene_ncbi, ensembl, reactome, kegg_pathways,
reactome_relationships, kegg_relationships`. Counts against the 4.2.0
bundle, for the tables those seven own:

| table | staging | bundle 4.2.0 |
| ----- | ------- | ------------ |
| gene_masters | 72,660 | 72,647 |
| entity_locations | 39,306 | 38,882 |
| pathway_masters | 3,255 | 3,220 |
| genome_assemblies | 49 | 49 |

Slightly above the bundle because the sources are newer (Ensembl 116 vs
115). The tables far below it — entities, entity_aliases,
entity_relationships — belong to the nine sources not included.

**The core DTPs need no adaptation for SQLite.** Seven ran unchanged,
including both relationship DTPs and Ensembl, which carries the only
dialect-specific path in the core (`pg_insert` with `ON CONFLICT`) and
had never been exercised on its SQLite branch.

**The declared order holds.** Ensembl resolved against the genes HGNC and
NCBI created; KEGG and Reactome resolved against the same universe.

### ⚠ Time, not space, is what the core branch costs

| step | hgnc |
| ---- | ---- |
| extract | 6 s |
| transform | 1 s |
| **load** | **518 s** |

The load is 74x everything else, at roughly 85 entities/second. ADR-003
argued the core is comfortable in SQLite because it is 105 MB — true
about space, silent about time. At that rate the 4.06 M rows of
`entity_relationships` alone extrapolate to ~13 hours.

The cause is `get_or_create_*`: a SELECT then an INSERT per row through
the ORM. The variant DTPs have bulk paths (`_bulk_insert_records`, `COPY`
on Postgres); the core DTPs have none. ChEBI was dropped from this run
for exactly this reason — it was still loading 117,265 chemicals when it
was killed.

- [ ] **Batch the core load.** Probably worth more than anything else
      left in Phase 2.


- [x] Split core and variant branches — done in `BundleBuilder`, not in
      `ETLManager` as this item proposed. The ETL stayed a data-source
      executor that knows nothing about branches; the plan declares them
      and the builder passes different `run_steps`. Less invasive, and
      build policy lives in one place.
- [x] Resume — per source rather than per branch, which is finer. Proven
      against a real failure: Ensembl 404'd because the source published
      release 116 while the seed pinned 115; after fixing the URL, the
      re-run skipped six sources and executed one.
- [x] Throwaway SQLite via `create_all` (`prepare_staging`). 180 MB for
      seven sources.
- [x] ~~Dump the SQLite core to parquet~~ — `_assemble()` implemented
      in `bb7a1b1`, reusing `export_full_clone`. Variant tables land in a
      subdirectory named for the table, because the reader has no rule
      for sibling files sharing a prefix and flat names would register as
      25 separate views.
- [x] The SQLite stays under `<data-root>/staging`, outside the bundle.

## Phase 3 — Variant branch (parquet-direct) ✅ done 2026-09-09

Every variant DTP now writes its final parquet directly, keyed by the
natural key. Verified by running each end to end on real data, not by
inspection.

- [x] gnomAD joint — `variant_masters_chr<N>.parquet`. chr21: 2,490,587
      variants from 12,719,368 alleles (`AC_joint >= 5`), 96 MB. A strict
      superset of the 4.2.0 bundle's 2,025,847 with zero rows missing;
      the 464,740 extra come from reading the joint callset where the old
      pipeline read only genomes.
- [x] gnomAD VEP — `variant_molecular_effects_chr<N>.parquet`, one row
      per (variant x transcript x consequence). chr21: 30,196,789 rows
      over 2,470,113 variants (12.2x fanout), 111 MB, zero rows orphaned
      from the variants file.
- [x] gnomAD rsID — `variant_rsid_chr<N>.parquet`, 9,850,326 keys on
      chr21. See the note below: this exists because the joint callset
      publishes no rsID at all.
- [x] AlphaMissense — 25 files, 71,697,556 rows, 889 MB. The bundle holds
      783,844 (1.1%): the old load's inner join against `variant_masters`
      discarded every prediction whose variant had not been loaded.
- [x] GTEx — 23 files, 18,470,502 rows, 634 MB, tissue selection moved to
      JSON (see below).
- [x] GWAS — single file, 1,208,545 rows, 36 MB.
- [x] dbSNP/NCBI variant DTP removed entirely (26 data sources, the
      `VariantSNPMerge` model, the `variant_snp_merges` table).
- [x] Output layout: one file per chromosome named after its table
      (`variant_masters_chr21.parquet`), **not** hive directories. See
      the note below.
- [x] `load()` on all five raises `NotImplementedError` with the reason,
      keeping the old body as `_load_legacy` for comparison.

### rsID: the joint callset has none

`variant_masters` came out with `rsid` 100% null and reported success.
The cause is that the joint VCF's ID column is empty on every record —
only the exome and genome callsets carry rsIDs (24.4% and 70.8% of
records sampled).

The VEP DTP now also emits `variant_rsid_chr<N>.parquet`, captured
**before** the AC filter: only ~25% of rsIDs survive `AC >= 5`, and the
ones dropped are the rare variants that most need an identifier. Coverage
on chr21 is 98.3% of variants; a LEFT JOIN adds 126 ms for a whole
chromosome, 60 ms for an ADSP-sized list of 700,000 variants.

It is a table of its own, not a column stamped onto `variant_masters`.
Stamping would make the joint branch depend on this one having run first,
and a missing map would degrade to silent nulls — which is exactly the
failure being fixed. If the bundle should ship `variant_masters` with
rsID already attached, bundle assembly is the place: both parquets exist
there and neither DTP has to wait for the other.

`min_ac` for the map is configurable and currently `null` (every rsID,
~4.5 GB genome-wide). `3` is the safe lower bound for the joint's
`AC >= 5`, since `AC_joint` is exactly `AC_exomes + AC_genomes` (verified
on 250,000 records).

### Layout: flat per-chromosome files, not hive

ADR-003 §2.9 called for hive directories. Measured on 71.7 M rows both
ways, that was revised:

| layout | size | 1 chrom | 3 chroms | full scan | non-chrom filter |
| ------ | ---- | ------- | -------- | --------- | ---------------- |
| hive   | 888.9 MB | 10.7 ms | 7.8 ms | 22.8 ms | 40.5 ms |
| flat   | 835.2 MB | 17.7 ms | 21.1 ms | 24.0 ms | 34.4 ms |

Hive prunes a chromosome predicate faster, but the gap is milliseconds.
What decided it: one file per chromosome gives the manifest one entry
with one checksum, with none of the parent-plus-children ambiguity that
made the 4.2.0 bundle store 15.6 GB twice, and
`variant_masters_chr21.parquet` names its own table and partition where
`part-0.parquet` means nothing without its parent directory.

**The source is part of the file name** (`variant_alphamissense`,
`variant_gtex`, `variant_rsid`) because a parquet is immutable once
written: one file per source makes provenance a property of the layout
rather than something a column has to be trusted for. The consequence is
that a second missense predictor becomes its own table rather than rows
in a shared one.

### GTEx: tissue selection is configurable

The DTP had a hardcoded `BRAIN_TISSUES_V10` frozenset filtering at three
points. It now reads `config/dtp_variant_eqtl_gtex.json`, which lists all
50 tissues with the size each occupies in the tarball.

Default stays the 13 Brain tissues, so behaviour is unchanged; enabling
all 50 takes the output from ~18.5 M rows to roughly 70 M. Note the
selection controls **transform and output only** — GTEx ships all tissues
in one 2.39 GB tarball and does not expose them individually (per-tissue
URLs 404), so the download is all-or-nothing.

The data source was renamed `gtex_v10_brain_eqtl` → `gtex_v10_eqtl`.


## Build orchestration ✅ landed 2026-09-09

`bundle plan` writes the recipe; `bundle build` runs it. Verified on a
real single-source plan, not by inspection.

- [x] `bundle plan` enumerates every source with an `include` flag, its
      DTP, version and config path, split into the two branches. Refuses
      to overwrite without `--force`, since a plan may be the only record
      of how a published bundle was made.
- [x] `bundle build` creates a throwaway SQLite under
      `<data-root>/staging`, runs each included source, reclaims disk, and
      assembles only if every source succeeded.
- [x] Resume is the default; `--restart` discards the staging database.
      Tested: a clean build, a resume with everything already done (no
      download at all), a resume after the parquet was deleted (rebuilds
      it), and `--restart`.
- [x] Per-branch discard: variant keeps its parquet and drops the raw
      VCFs; core drops both once the rows are staged.
- [x] All-or-nothing assembly. A partial bundle is indistinguishable from
      a complete one to a reader.
- [x] ~~`_assemble()` is a stub~~ — implemented in `bb7a1b1`. Produces
      the bundle, the manifest, a copy of the plan, and a build record
      with DTP versions, source URLs, hashes and per-step timings.

### Sequential, and why that is not a limitation

The build runs sources one at a time. Requested as parallel; implemented
sequential because parallelism works against the constraint that
motivated the discard in the first place. Peak disk for one chromosome is
~126 GB and the full download is ~1.53 TB, so N workers means N times the
peak. Downloads gain nothing anyway — four parallel range streams
measured 66.6 MB/s against 64.2 MB/s for one, since the local link
saturates. Core and variant could overlap (core is 105 MB and does not
strain disk), but they would contend on the same SQLite ledger for no
wall-clock gain, as neither is CPU-bound while the other waits on the
network.

### `--data-root` has to be pinned into the staging database

The ETL reads `download_path` and `processed_path` from `system_config`,
not from its caller. A freshly seeded database carries the packaged
defaults, so the ETL wrote under `./biofilter_data` while the builder
reclaimed disk under `--data-root` — the discard deleted the wrong copy
and the real download was never freed. The builder now writes both
settings into the staging database before running. Worth remembering for
anything else that drives the ETL programmatically.

## Phase 4 — Bundle identity ✅ done 2026-09-09

A bundle cannot be rebuilt once its sources move on, so what it says
about itself is the only account that survives — and it was saying the
wrong thing.

- [x] `biofilter_metadata` written **by the build**, with a real
      `build_hash`. The row was seeded at database creation and never
      updated, so the 4.2.0 bundle claims schema 4.1.0 with a null
      build_hash and a `created_at` from the day its source database was
      first made. Written after export, because the id is derived from
      the finished tables.
- [x] `bundle_id` derived from content — table, rows, bytes, plus the
      plan — with `created_at` excluded so it identifies and verifies the
      artifact rather than labelling a run. `biofilter_metadata` is
      excluded from the fingerprint because it is rewritten to carry the
      id.
- [x] Exposed as `bundle_id()` and `bundle_manifest()` on Database and
      DBComponent, and as `bundle info <path>`. `_bundle_manifest`
      existed and nothing consumed it.
- [x] Report results carry `bundle_id` in `DataFrame.attrs`. **Caveat:
      that does not survive a CSV export**, which is exactly how a list
      of ids leaves and comes back months later. Stamping the export is a
      separate change and is not done.
- [x] Column introspection on connect, warning rather than refusing, with
      `db verify --schema` as the strict gate. A bundle built from a
      subset of sources legitimately carries fewer tables, so refusing
      would make it unusable for the ones it has; a table present but
      short a column is the real mismatch. It found one immediately: the
      4.2.0 bundle has no `variant_class` in
      `variant_molecular_effects`.


## First full build — 2026-09-10

15 core sources (all but ChEBI and OMIM) plus 7 variant sources (gwas,
alphamissense, gtex, and joint+vep for chr21 and chr22).

```
bundle_id 7fb687d116359701   96 tables   193 M rows   2.2 GB
  core      41 tables    5,869,160 rows     71.9 MB
  variant   55 tables  187,246,566 rows  2,202.8 MB
valid under `db verify --schema`
```

Against the 4.2.0 bundle, for the tables both carry:

| table | new | 4.2.0 |
| ----- | --- | ----- |
| entity_relationships | 4,210,591 | 4,063,429 |
| gene_masters | 72,660 | 72,647 |
| protein_masters | 20,431 | 20,431 |
| entity_locations | 39,306 | 38,882 |
| disease_masters | 36,090 | 30,613 |
| protein_pfams | 30,134 | 27,481 |

`entity_relationships` above the old bundle with one fewer source is the
clearest evidence the declared order is right: it is the largest core
table and the one most dependent on masters existing before relationships
run. `entities` and `entity_aliases` sit below 4.2.0 by roughly the size
of ChEBI, which was excluded.

**All 15 core DTPs work against SQLite unchanged**, including the four
relationship DTPs. That closes the open question from Phase 2, which had
only exercised seven.

### What it cost, and what broke

Three defects, all from one wrong assumption — that a source owns what it
produces. True for the variant branch, false for the core:

1. The plan was not authoritative in fact. Four gnomAD sources were
   skipped with "No matching active DataSources found" because the ETL
   filters on `active` and they are seeded inactive. The docs already
   claimed the plan was the authority; that was written and not
   implemented.
2. Reclaiming per source destroyed inputs. `uniprot` freed its processed
   directory and `uniprot_relationships` failed with "File not found" —
   the DTP says outright the file "is hosted in the parent dtp".
3. `_already_done` treated any core source as finished because its rows
   are in the staging database, which is false for a master whose
   relationship child still reads its parquet.

Merging each master with its relationships was considered and rejected:
relationships resolve **across** sources and need every master loaded
first, which is why the plan runs all masters before any relationships. A
merged DTP would run its relationships before the other masters existed.

Resolved instead by reclaiming the core's processed files once, after the
whole branch succeeds — dropping a name-based heuristic that was already
wrong for `kegg_pathways` → `kegg_relationships`. Verified on a clean
build of exactly that pair.

- [ ] **Batch the core load — still open, and now the top cost.**
      `uniprot` took 1,148 s to load, `hgnc` 518 s, against seconds for
      extract and transform. Pfam loaded in 5 s because it creates no
      entities: the cost tracks `get_or_create_*` calls, not table size.

## Phase 5 — Guardrails

- [x] ~~Model-level test for the entity-FK invariant~~ — added in
      `b3d7c9f`. It failed on the first run: `variant_gwas` declared real
      foreign keys to `etl_data_sources` and `etl_packages` where every
      sibling used the same columns without them. Removed — a constraint
      nothing can enforce is worse than none.
- [x] ~~Reproducibility test~~ — covered in `b3d7c9f` at the level
      that matters: the same content yields the same id whenever it was
      built, a changed row count changes it, table order does not.
      Byte-identical parquet across two runs is **not** asserted, and
      would be a stronger claim than the build currently makes.
- [x] ~~Acceptance run against `bf_files/`~~ — done in the full build
      above. Row counts compared table by table; the tables both bundles
      carry agree, and the differences are accounted for (ChEBI excluded,
      sources newer).
- [x] ~~The extract/transform skip only checks the hash~~ — fixed in
      `df9e15a`. Both steps verify their own product still exists. The
      extract check had to be careful: requiring the raw file would
      re-download 1.5 TB on every resume, since the build deletes raw on
      purpose, so it skips when the raw is present *or* a transform
      already completed for that hash.
- [x] ~~A DTP failure can leave its package stuck in `running`~~ — fixed
      in `df9e15a`. The three DTP calls mark the package failed with the
      exception before re-raising.
- [x] ~~A failed step still ends with a success line~~ — fixed in
      `b3d7c9f`. `start_process` reports whether every source completed,
      confirmed against the package ledger rather than inferred from the
      call returning, and `etl update` exits 1. Verified both ways.

## Phase 6 — Documentation — medium pass done 2026-09-09

Scoped deliberately: fix what is factually wrong, and add the one
document that makes the rest coherent. Sources were 2,002 lines with
stale content across ten files, but the problem was never the count — the
mental model changed. `database.md` opened with "create DB → ping →
migrate", a sequence describing a persistent database an operator
maintains, which no longer exists; `etl.md` never mentioned branch,
bundle or SQLite, describing the ETL as an end in itself when it is now a
step inside a build. Fixing mentions one by one would have produced
internally inconsistent docs: correct commands describing a flow that is
gone.

- [x] **New `building_bundles.md`** — the plan/build/info flow, why the
      build splits in two, the disk constraint, resume semantics, and the
      two properties a reader has to know: ids are internal to one
      bundle, and a bundle cannot be rebuilt.
- [x] `database.md` rewritten — states up front that there is no
      persistent database, keeps the commands that still apply.
- [x] `etl.md` rewritten — the two branches, why variant DTPs need
      explicit `--run-step`, the JSON field/tissue configs.
- [x] `index.md`, `cli_reference.md`, `parquet_backend.md`,
      `troubleshooting.md` — removed `db migrate`, added the bundle
      commands.
- [x] `CLAUDE.md` and `README.md` — Alembic, PostgreSQL-as-production and
      the migration-based workflow replaced with the bundle flow.
- [x] Sphinx still builds.

### Deferred to the full pass

Waiting on a build with all sixteen core sources, since several of these
need numbers that run will produce:

- [ ] `parquet_backend.md` (194 lines) — written against the old model
      where a bundle was an export. Needs to describe it as the product.
- [ ] `system_overview.md`, `schema.md` — the four-layer description
      predates the branch split.
- [ ] `report_catalog.md`, `configuration.md`, `entity_and_omics.md` —
      light drift, unreviewed.
- [ ] Retention policy for archived bundles, with real sizes.
- [ ] Load timings and full-bundle size, once measured.
- [ ] **Decide which document is the source of truth.** `CLAUDE.md`,
      `docs/source/` and `biofilter_agents/*.md` all describe the same
      system and will drift apart. Suggested: `docs/source/`, with the
      other two pointing at it rather than restating it.



- [ ] `CLAUDE.md`: architecture section still describes PostgreSQL as
      production and lists `db migrate` / `db upgrade` in the workflow.
- [ ] `README.md`: backend list.
- [ ] `docs/source/`: `database.md`, `etl.md`, `parquet_backend.md`,
      `system_overview.md`.
- [ ] State explicitly in user-facing docs that IDs are **not portable
      across bundles** (ADR §3, Negative). This is now a documented
      property, not an accident.
- [ ] Retention policy for historical bundles (~15.6 GB per release).

---

## Open decisions carried forward

- [ ] **`min_ac` for the rsID map** — `null` (every rsID, ~4.5 GB
      genome-wide) vs `3` (safe lower bound for the joint's `AC >= 5`,
      no loss, some prunable orphans). One line in the VEP config plus a
      transform re-run; the extract is cached.
- [ ] **`min_ac` for the VEP annotations** — currently `5`, which leaves
      0.8% of variants without annotation (they clear the combined
      `AC_joint` without either callset clearing it alone). `3` removes
      that loss at +34% on the intermediate parquet, with the orphans
      prunable at bundle assembly.
- [ ] **Whether `variant_masters` ships with rsID attached.** If yes,
      bundle assembly does the join — not the joint DTP, which would
      reintroduce the branch dependency.
- [ ] **Raw data retention.** The full genome is 1.53 TB of downloads
      against 348 GB free locally. Chromosomes must be processed and
      discarded, or `data_root` must point at external storage. Still not
      in the ADR.
- [x] ~~Sources pinned to `current` break reproducibility~~ — resolved
      2026-09-09, and not the way it was framed. Ensembl's URL points at
      `current_gff3` with the release number in the file name, so it
      404s whenever Ensembl publishes, and a rebuild would pull different
      data than the original run.

      The decision is that this is expected rather than a defect: when a
      source changes, you build a **new** bundle against the new data,
      and the old bundle stays a snapshot of a moment that can no longer
      be recreated. Reproducibility lives in the retained artifact, not
      in the ability to rebuild it — which is why bundles are backed up
      and versioned as the record. Simpler than pinning every source, and
      honest about what an upstream release actually means.

      Consequence for Phase 4: the manifest has to be complete enough to
      identify what a bundle contains, because it is the only account
      that will survive. And bundle retention stops being housekeeping
      and becomes the archive.

## Deferred (not 4.3.0 scope)

- **DTP slimming for the core branch.** Dropping the "already
  inserted?" checks now that every write is an insert. Done for the
  variant DTPs as part of Phase 3; the 16 core DTPs are their own pass.
- **Failure semantics across branches.** If the variant branch fails
  after the core branch succeeded, is a partial bundle published or is
  the whole build discarded? (ADR §6)
- **`rapidfuzz`** is not in `pyproject.toml` but three report tests
  expect it (`pytest.importorskip`). Unrelated to ADR-003; decide
  whether it becomes a dependency or an extra.
