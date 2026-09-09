# TODO — ADR-003 implementation (4.3.0)

Working checklist for [ADR-003](0003-parquet-native-build-pipeline.md).
Decisions live in the ADR; this file tracks execution state only.

**Branch:** `release/4.3.0`
**Acceptance target:** the 4.2.0 bundle at `../../../bf_files/` — same table
set, known row counts, per-file sha256 in its manifest.

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

### ⚠ `dtp_gwas` is the exception — decide before starting §3

Four of the five variant DTPs already produce parquet in `transform` and
touch the session only to load it. `dtp_gwas` does not: it inherits
`EntityQueryMixin` (`dtp_gwas.py:26`) and writes through the ORM —
`bulk_save_objects`, `session.execute(insert())`, raw `DELETE FROM`
(lines 523–618).

Its **output schema is clean**: `variant_gwas` has 27 columns and no
entity surrogate (`mapped_trait_id` / `parent_trait_id` are EFO/MONDO
strings; `variant_gwas_snp.variant_gwas_id` is internal to the pair). So
it does not violate the ADR §2.3 invariant — but it is a rewrite, not a
"delete the load step" like the other four.

Options: (a) rewrite it parquet-native with the rest, (b) leave it in the
core branch for 4.3.0 since its output is small (1.07 M + 0.91 M rows,
70 MB) and move it later. **Not decided.**

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
- [ ] Decide `bundle_id` derivation: content-derived (reproducible,
      identical builds collide by design) vs build-derived (unique per
      run). ADR §6. Blocks Phase 4.
- [ ] Decide the `dtp_gwas` question above. Blocks Phase 3.

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
- [x] `dtp_variant_gnomad` (old) left intact, as agreed.
- [ ] **ETL dependency module — deferred, not rejected.** There is no
      dependency model today (`start_process_all` orders by
      `data_source_id asc`; no `depends_on`, no topological sort). The
      new DTPs do not need it: the two branches are independent. The
      *core* branch does — HGNC before Ensembl, entities before
      relationships, all riding on seed insertion order today. Trigger:
      if Phase 2 hits that ordering, build it then, with a concrete case.

## Phase 1 — Strip the relational assumptions

Independent of the pipeline split; safe to do first.

- [ ] Remove Alembic. Footprint is small: `alembic.ini`,
      `biofilter/alembic/` (2 migrations), `biofilter/utils/migrate.py`,
      `biofilter/modules/db/migrate.py`, plus references in
      `create_db_mixin.py` and `transfer.py`.
- [ ] Remove `db migrate` and `db upgrade` from the CLI
      (`groups/db.py:132` and `:200`). Note `create-db` and `ping` stay.
- [ ] Delete `tests/unit/db/test_db_migrate.py`; check for other tests
      asserting on the migration commands.
- [ ] Replace the migration path with `create_all` for staging.
- [ ] Drop `schema_version` / `etl_version` from `biofilter_metadata`
      (ADR §2.7).
- [ ] Rename `model_curation.py` → `model_status.py`. It is **not** dead
      code — it holds `omic_status` (6 rows), written by
      `gene_query_mixin.py:152`, indexed via `base_dtp_turning.py:124`,
      and required by HGNC/MONDO/NCBI/ChEBI. Update the import in
      `utils/db_loader.py:19`.

## Phase 2 — Core branch (SQLite staging)

- [ ] Split the ETL orchestrator into core and variant branches, keyed
      on `data_type == 'Variant'`. `ETLManager.start_process_all`
      (`etl_manager.py:250`) is the entry point.
- [ ] Independent resume per branch — a failed variant branch must not
      force a core rebuild.
- [ ] Point the core branch at a throwaway SQLite built with
      `create_all`. Target size ~105 MB / 6.98 M rows.
- [ ] Dump the SQLite core to parquet after the branch completes.
- [ ] Retain the SQLite as a build artifact outside the published bundle
      (ADR §4, Alternative E).

## Phase 3 — Variant branch (parquet-direct)

- [ ] Promote `_write_parquet_part` output to final for `gnomad`
      (`dtp_variant_gnomad.py:556`); delete the load step that re-reads
      it and `COPY`s to PG (`:1092`, plus the stage-table helpers at
      `:1016`–`:1083`).
- [ ] Same for `alphamissense`, `gtex`, `ncbi` — one parquet write and
      one session use each, so this should be mechanical.
- [ ] Handle `gwas` per the Phase 0 decision.
- [ ] Keep the three seed dimension lookups (`variant_consequences`,
      `variant_impacts`, `variant_biotypes`) reading from seed, not from
      a live session (`dtp_variant_gnomad.py:1976`).
- [ ] Emit hive-partitioned directories (`<table>/chromosome=N/`) with
      **no** duplicated `_chr_N` children — the 4.2.0 bundle sets
      `partition_children_included: true` and pays 31 GB for 15.6 GB of
      data (ADR §2.9).

## Phase 4 — Bundle identity

- [ ] Write `biofilter_metadata` **from the build**, with a real
      `build_hash`. Today's bundle carries a March row with
      `build_hash: None` claiming version 4.1.0 (ADR §1.5).
- [ ] Compute `bundle_id` per the Phase 0 decision; emit into both the
      manifest and `biofilter_metadata`.
- [ ] Expose `bundle_id` through the Python API and CLI.
      `DatabaseManager._bundle_manifest()` (`database.py:191`) already
      reads the manifest and is currently the only occurrence of that
      symbol in the codebase — nothing consumes it. Start there.
- [ ] Stamp report output that carries entity or variant IDs.
      `ReportManager.run` returns the result unmodified
      (`report_manager.py:275`). Mechanism undecided (ADR §6): DataFrame
      attribute is invisible in CSV; a column changes report schemas.
- [ ] Introspect columns when opening a bundle and fail loudly at
      connection time rather than mid-report in a DuckDB binder error
      (ADR §2.7).

## Phase 5 — Guardrails

- [ ] Model-level test asserting **no variant table declares an entity
      FK**. The parallel-branch design depends on this invariant
      (ADR §2.3) and nothing currently enforces it.
- [ ] Reproducibility test: build twice, assert identical parquet
      content per table (ordering included).
- [ ] Acceptance run against `bf_files/`: compare row counts table by
      table, and content for the core's 29 tables.

## Phase 6 — Documentation

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

## Deferred (not 4.3.0 scope)

- **DTP slimming.** Dropping the "already inserted?" checks now that
  every write is an insert. Direct consequence of ADR §2.1, but the
  DTP-by-DTP review is its own pass.
- **Failure semantics across branches.** If the variant branch fails
  after the core branch succeeded, is a partial bundle published or is
  the whole build discarded? (ADR §6)
- **`rapidfuzz`** is not in `pyproject.toml` but three report tests
  expect it (`pytest.importorskip`). Unrelated to ADR-003; decide
  whether it becomes a dependency or an extra.
