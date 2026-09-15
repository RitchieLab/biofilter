# Bundle gaps blocking the report redesign (4.3.0)

Findings measured on 2026-09-14 against the two bundles in
`biofilter_data/bundles/`. This file is a **handoff to the bundle/ETL
work**: everything below is a build-pipeline or model issue, surfaced while
designing the new report module ([ADR-004](../../adr/0004-parquet-native-report-module.md)).

The report redesign needs one bundle that carries all of it. Neither
existing bundle does.

**Do not re-derive** — every number here was measured, and the command
shape is given where it matters.

---

## 0. The two bundles, and why neither is enough

| | `20260910` | `20260909_full` |
| --- | --- | --- |
| `bundle_id` | `e29a11604a326d2e` | `7fb687d116359701` |
| created | 2026-09-12 | 2026-09-10 |
| manifest entries | 114 | 96 |
| on disk | 21 GB | — |
| `variant_masters` | **177,520,333** (24 files) | 5,380,390 (chr21–22 only) |
| `variant_molecular_effects` | **2,238,929,441** (24) | 69,973,856 (chr21–22) |
| `variant_rsid` | **712,358,067** (24) | 20,515,717 (chr21–22) |
| `variant_gwas` | 1,208,545 | 1,208,545 |
| `variant_alphamissense` | **absent** | **71,697,556** (25 files, full genome) |
| `variant_gtex` | **absent** | **18,470,502** (23 files, full genome) |

`20260910` has the genome but no predictors. `20260909_full` has the
predictors but only two chromosomes of gnomAD.

**What the report work needs:** one bundle with full-genome gnomAD *and*
`variant_alphamissense` / `variant_gtex`.

Until then, `20260909_full` is the better development bundle — every
table is represented, and 70 M rows of `variant_molecular_effects` is
enough to validate query plans and pruning. `20260910` stays as the
scale test.

---

## 1. `alphamissense` and `gtex_v10_eqtl` produced nothing in `20260910`

Both are `include: true` in that bundle's `bundle_plan.json`. Its
`build_record.json` shows why nothing landed:

```
2026-09-09  alphamissense   transform  completed
2026-09-09  gtex_v10_eqtl   transform  completed
2026-09-12  alphamissense   transform  not-applicable     <- the build that produced this bundle
2026-09-12  gtex_v10_eqtl   transform  not-applicable
2026-09-12  gwas            transform  completed          <- this one did land (1,208,545 rows)
```

The skip logic decided there was nothing to do, so no output was
produced and `_move_variant_tables` had nothing to move. The 2026-09-09
output was not picked up either.

**Action:** make the skip path either reuse the existing transform
output or refuse to assemble a bundle whose plan includes a source that
contributed no table.

---

## 2. Ten declared tables are empty, and `db verify` calls the bundle valid

Empty in `20260910`: `chemical_masters`, `variant_biotypes`,
`variant_effect_predictions`, `variant_gene_regulatory_evidence`,
`variant_impacts`, `variant_regulatory_elements`, `variant_gwas_snp`,
plus the three parent stubs of §3.

`variant_effect_predictions`, `variant_gene_regulatory_evidence` and
`variant_regulatory_elements` are **also empty in `20260909_full`** —
they were replaced by the per-source tables (`variant_alphamissense`,
`variant_gtex`). They should leave the schema.

Some emptiness is legitimate (`chemical_masters` was never loaded); some
is a silent build failure (§1). `db verify` cannot currently tell them
apart and reports the bundle as valid either way.

**Action:** decide which of the ten are intentional, drop the rest, and
consider having `db verify` flag a declared-but-empty table.

---

## 3. Parent stubs shadow the real data

Both bundles emit, for the partitioned variant tables, **both** a
partition directory and an empty same-named parent file:

| view | resolves to | rows served | rows available |
| --- | --- | ---: | ---: |
| `variant_masters` | `variant_masters.parquet` (3.7 KB) | **0** | 177,520,333 |
| `variant_molecular_effects` | `.parquet` (6 KB) | **0** | 2,238,929,441 |
| `variant_gwas` | `.parquet` (4 KB) | **0** | 1,208,545 |
| `variant_rsid` | directory (no sibling file) | 712,358,067 | 712,358,067 |

`Database._discover_bundle_sources()` (`database.py:295-306`) keys the
directory and the file identically, and `sorted()` puts the directory
first — so the empty file overwrites it. Every variant report reads zero
rows, with no error.

The same ambiguity bites independently written code: the ADSP helper
`table()` (`biofilter_legacy/bf4_420/notebooks/Andre/adsp/step_01/bf4_map_coding_genes.py:77-85`)
checks `flat.exists()` first and would also read the stubs.

ADR-003 §2.9 already specified dropping the parents. They are still
being written.

**Action (build side):** stop emitting the parent stubs.
**Action (read side, ADR-004 §2.3):** register views from
`manifest.json` — each partitioned child already declares its logical
table — instead of scanning the directory.

```json
{"name": "variant_masters",      "rows": 0,        "file": "tables/variant_masters.parquet"}
{"name": "variant_masters_chr1", "table": "variant_masters", "branch": "variant",
 "rows": 14090551, "file": "tables/variant_masters/variant_masters_chr1.parquet"}
```

---

## 4. Two position conventions inside one bundle

| tables | convention |
| --- | --- |
| `variant_masters`, `variant_molecular_effects`, `variant_rsid` | `position` |
| `variant_alphamissense`, `variant_gtex` | `position_start`, `position_end` |

Measured on `variant_alphamissense` chr21: `position_start = position_end`
in **698,557 of 698,557 rows** — they are all SNVs, so no interval is
being represented.

The join works once the mismatch is handled, and works well:

| join (chr21) | result |
| --- | ---: |
| `variant_gtex` × `variant_masters` on (chrom, pos, ref, alt) | 191,716 / 191,786 = **99.96%** |
| observed missense variants with an AlphaMissense score | 33,000 / 37,689 = **87.6%** |

**Decision needed:** one convention. `position` alone is sufficient for
current data; `position_start`/`position_end` keeps the ability to
represent an interval if indels or CNVs are ever ingested. Whatever is
chosen, reports should not carry the branch forever.

---

## 5. `variant_masters`: the model and the data are different tables

`model_variants.py:109-170` declares the 4.2.x shape. The parquet the
ETL writes (`dtp_variant_gnomad_joint.py:323-326`) declares another.
**Four columns in common out of 25.**

`db verify --schema` reports `drift entries: 0`, because it compares the
models against the **parent stub** (§3), which was generated from the
models and therefore matches them perfectly.

### Column map

| model column | status | action |
| --- | --- | --- |
| `grpmax`, `grpmax_af` | present in the joint VCF, `load: false` | **config only** — set `load: true` on `grpmax_joint` and `AF_grpmax_joint` in `dtp_variant_gnomad_joint.json` |
| `cadd_phred`, `cadd_raw_score`, `revel_max`, `sift_max`, `polyphen_max`, `spliceai_ds_max`, `pangolin_largest_ds` | in neither DTP config | **ETL work** — see §6 |
| `af`, `ac`, `an` | superseded | rename to `af_joint`, `ac_joint`, `an_joint` |
| `variant_id` | superseded | drop — identity is `variant_key` |
| `position_start`, `position_end` | superseded | drop or unify per §4 |
| `variant_type`, `allele_type` | not produced | drop |

---

## 6. The in-silico predictors are not in either DTP config

Searched both configs for `cadd`, `revel`, `sift`, `polyphen`,
`spliceai`, `pangolin`, `alphamissense`, `phylop`, `primateai`, `esm`:

| config | fields | `load: true` | predictor fields |
| --- | ---: | ---: | --- |
| `dtp_variant_gnomad_joint.json` | 664 | 18 | **none** |
| `dtp_variant_gnomad_vep.json` | 46 | 26 | **none** |

**The joint VCF does not carry them.** Its 664 INFO fields, generated
from the v4.1 header, are entirely frequency/count/QC:

```
AC(102)  AF(102)  AN(102)  nhomalt(102)  CTT(68)
faf95(60)  faf99(60)  age(18)  fafmax(12)  dp(12)  gq(6)  ab(3)  grpmax(15)
```

No config change to `dtp_variant_gnomad_joint` can recover them.

**The VEP DTP reads the right files and only parses the wrong block.**
It downloads `gnomad.exomes.v{release}.sites.chr{chrom}.vcf.bgz` and the
genomes equivalent (`dtp_variant_gnomad_vep.py:47-51`), but its config
enumerates only the 46 VEP/CSQ subfields. The gnomAD v4 in-silico
predictors are **INFO-level fields of those same VCFs**, beside the CSQ
block rather than inside it.

The model's column names are the evidence: `cadd_phred`,
`cadd_raw_score`, `revel_max`, `sift_max`, `polyphen_max`,
`spliceai_ds_max`, `pangolin_largest_ds` is gnomAD's own INFO naming for
the exomes/genomes callsets. 4.2.x was reading them from there.

> **Unverified.** `biofilter_data/downloads/` is empty, so no VCF header
> was inspected. The claim that these are INFO fields of the
> exomes/genomes sites VCFs comes from knowledge of gnomAD v4, not from
> this repo. **Confirm by reading the header of one
> `gnomad.exomes.v4.1.sites.chr21.vcf.bgz` and listing its INFO fields
> before doing the work.**

**Action:** extend `dtp_variant_gnomad_vep` to read INFO fields
alongside the CSQ block, and decide where the predictors land —
`variant_masters` (they are attributes of the variant, and come from the
same callset) or a per-source table.

---

## 7. `canonical` works — this gap is already closed

The ADSP analysis recorded `canonical` and `mane_select` as 100% NULL in
4.2.0. **In 4.3.0 they are populated.** Config: `CANONICAL` (idx 24),
`MANE_SELECT` (25), `MANE_PLUS_CLINICAL` (26), all `load: true`.

`variant_molecular_effects`, chr21, identical in both bundles
(30,196,789 rows):

| field | non-null | share |
| --- | ---: | ---: |
| `canonical` | 4,598,116 | 15.2% |
| `mane_select` | 1,813,598 | 6.0% |
| `mane_plus_clinical` | **0** | — |

`canonical` takes only `NULL` and `'YES'`, and it disambiguates cleanly.
Per (variant, gene) pair on chr21:

```
average transcripts per pair              5.5
average canonical per pair                0.9

pairs with exactly 1 canonical      3,579,600   (90.1%)
pairs with no canonical               393,385   ( 9.9%)
pairs with more than 1 canonical            0   <- never ambiguous
total pairs                         3,972,985
```

A variant touching two genes still yields two rows, each with its own
canonical — correct, and the behaviour ADSP expected.

**Two things for the bundle side:**

- `mane_plus_clinical` is `load: true` and 100% empty on chr21. It is a
  column carried across 2.2 billion rows with no information in it —
  candidate for `load: false`, or removal from the schema.
- The 9.9% of pairs with no canonical are not only non-coding: by
  biotype (indicative — an arbitrary biotype per pair), `protein_coding`
  241,496; `processed_transcript` 74,620; `misc_RNA` 32,727; `lncRNA`
  23,560; `nonsense_mediated_decay` 16,284; `retained_intron` 4,698.
  `mane_select` does not help (it covers less). A report needing one
  transcript per variant×gene will need a fallback rule; that is a
  report-side decision, noted here only so the data side knows the
  column is not a complete key.

---

## Summary of what the report redesign is waiting on

| # | blocking | needed for |
| --- | --- | --- |
| 1 | `alphamissense` / `gtex` produce output in a full-genome build | any predictor or eQTL report |
| 3 | parent stubs stop being written | every variant report reading non-zero rows |
| 4 | one position convention | joins between gnomAD and predictor tables |
| 5 | `model_variants.py` matches the parquet; `db verify --schema` compares against the real data | `report_legacy` running at all, and trustworthy verification |
| 6 | predictors extracted, or formally dropped from the model | closing the column map |

§2 and §7 are cleanups, not blockers.

Not blocking, but worth knowing: the new report module reads parquet
directly and does not consult the ORM models, so §5 does not gate it —
it gates `report_legacy` and `db verify`.

---

## Build-side status — 2026-09-14

### Closed

**§3 — parent stubs.** Two independent fixes, because the bundles that
already exist cannot be re-made.

*Read side* (`database.py:_discover_bundle_sources`): when a directory
and a same-named `.parquet` are both present, the directory wins. This
needs no rebuild — `20260910` now serves its rows:

| view | before | after |
| --- | ---: | ---: |
| `variant_masters` | 0 | 177,520,333 |
| `variant_molecular_effects` | 0 | 2,238,929,441 |
| `variant_gwas` | 0 | 1,208,545 |
| `variant_rsid` | 712,358,067 | 712,358,067 |

*Build side* (`builder.py:_drop_parent_stubs`): the stub file and its
manifest entry are deleted once the partition directory is in place, so
new bundles do not carry them at all.

**§1 — root cause, found.** Not the builder's skip: the ETL's.
`_step_output_exists` asked whether the source directory had any entry,
and `_move_variant_tables` moves the parquet out of
`processed/AlphaMissense/alphamissense/predictions/` but leaves
`predictions/` standing. An empty subdirectory read as output, so the
transform skipped as not-applicable, and `_move_variant_tables` had
nothing to move. Both leftovers are still on disk and still empty —
that is the evidence.

Three changes:

- `etl_manager.py:_step_output_exists` now requires a *file*, at any depth.
- `builder.py:_prune_empty_dirs` removes the directories a move emptied,
  so there is nothing left to misread.
- `builder.py:_sources_without_output` — assembly refuses, and deletes
  the half-written bundle, when a planned variant source contributed no
  file. This is the guard the stated action asked for.

**§2 — `db verify`.** It now names the declared tables holding no rows,
without failing on them (a bundle built from a subset legitimately has
some). On `20260910` it lists exactly the ten:

```
ℹ️  10 declared table(s) hold no rows:
   chemical_masters, variant_biotypes, variant_effect_predictions,
   variant_gene_regulatory_evidence, variant_gwas, variant_gwas_snp,
   variant_impacts, variant_masters, variant_molecular_effects,
   variant_regulatory_elements
```

**§6 — verified.** The header of
`gnomad.exomes.v4.1.1.sites.chr21.vcf.bgz` was fetched (a 600 KB ranged
read) and lists 415 INFO fields. All seven model columns are there, at
INFO level, beside the CSQ block — plus one the model does not have:

```
cadd_raw_score  cadd_phred  revel_max  sift_max  polyphen_max
spliceai_ds_max  pangolin_largest_ds  phylop
```

All are `Number=1, Type=Float`, i.e. site-level, not per-transcript.
`revel_max` is scored at the MANE Select or canonical transcript;
`phylop` is the Zoonomia 241-mammal conservation score. The DTP already
downloads these files. The remaining work is parsing the INFO block and
deciding where the columns land.

### Found while fixing, not in the original list

**Reconnecting to a bundle emptied it.** `_normalize_uri` rewrites
`parquet://<bundle>` to `duckdb:///:memory:` and `Database.__init__`
connects when given a URI — so the sequence the docs recommend,

```python
bf = Biofilter(db_uri="parquet:///path/to/bundle")
bf.db.connect()
```

normalized an already-normalized URI, found no `parquet://` scheme, and
brought up an empty in-memory DuckDB with none of the bundle's views.
Every query after it failed with `Catalog Error: Table ... does not
exist`. `Database` now keeps the URI as the caller wrote it and
re-derives from that.

### Open

§1 still leaves `20260910` without the two tables — the fix stops it
happening again but does not fill the hole. §4 (position convention),
§5 (model vs parquet) and §6 (where the predictors land) are decisions,
not bugs.

Ten tests cover the four fixes: `tests/unit/bundle/test_bundle_assembly_guards.py`,
`tests/unit/etl/test_etl_skip_output_check.py`, and three added to
`tests/unit/db/test_db_bundle_foundation.py`.

---

## Build-side status — 2026-09-14, second pass

### §1 closed — `20260910` is complete

`variant_alphamissense` (71,697,556 rows) and `variant_gtex`
(18,470,502) were grafted from `20260909_full`. Both bundles ran the
same `dtp_version` (1.0.0) against the same `source_url` under the same
config, so that output is what this build would have written; it was
skipped, not produced differently. Recorded in `build_record.json`
under `amendments`.

### §4 closed — one convention, `position`

`position_start`/`position_end` collapsed on the way in. Checked per
file rather than assumed, and the check was worth running: AlphaMissense
has `start == end` in all 71,697,556 rows, but **GTEx does not** —
747,398 rows differ, up to a 63 bp span. They are indels, and
`position_end = position_start + len(reference_allele) - 1` in
**18,470,502 of 18,470,502 rows**. The column was derivable from two
others, so nothing was lost.

The join is now direct, and matches what you measured:

```sql
variant_gtex g JOIN variant_masters v
  ON g.chromosome = v.chromosome AND g.position = v.position
 AND g.reference_allele = v.reference_allele
 AND g.alternate_allele = v.alternate_allele
-- chr21: 191,716 rows
```

### §2 closed — nothing empty is left

Nine tables left the schema: the five dimensions
(`variant_consequences`, `_categories`, `_groups`, `variant_impacts`,
`variant_biotypes`), the three the per-source tables replaced
(`variant_effect_predictions`, `variant_gene_regulatory_evidence`,
`variant_regulatory_elements`) and `variant_gwas_snp`. `db verify` on
`20260910` now reports one empty table, `chemical_masters`, which is
empty because chemicals were excluded from the plan.

The ORM classes for the first eight stay until ADR-004 §2.2 retires
`modules/report/`, which still imports them; they are excluded from
every export (`builder.RETIRED_TABLES`), so no bundle carries them.
`variant_gwas_snp` went all the way — its foreign key pointed at a
`variant_gwas.id` the parquet never had, and its only other reference
was `_load_legacy`, dead since `load()` began raising.

### §5 closed — the model describes the parquet

`map_variant_masters` and `map_variant_molecular_effects` were rewritten
column for column against the files. Drift on `20260910` went from three
tables and ~45 columns to:

```
variant_masters: missing af_grpmax_joint, grpmax_joint
```

which is accurate — those two are the config change §5 asked for
(`grpmax_joint`, `AF_grpmax_joint` set to `load: true`), and this bundle
predates it. `VariantGWAS` lost `id`, `cnv` and `notes`, gained
`snp_rank`, and declares its identity through `__mapper_args__`.

### §6 closed — predictors extracted

`dtp_variant_gnomad_vep` now reads the INFO block alongside the CSQ
block and writes `variant_predictions`, following the `rsid_map`
pattern: its own config block, its own filters, captured **before** the
AC filter, declared Arrow schema. Eight fields, all `Number=1 Type=Float`.

One row per (variant allele × callset), not merged: a variant in both
exomes and genomes has two values and no principled rule picks one.
Records with no score at all are not written.

### Naming — source in every file name

| table | file |
| --- | --- |
| `variant_masters` | `variant_masters_gnomad_chr21.parquet` |
| `variant_molecular_effects` | `variant_molecular_effects_gnomad_chr21.parquet` |
| `variant_rsid` | `variant_rsid_gnomad_chr21.parquet` |
| `variant_predictions` | `variant_predictions_gnomad_chr21.parquet` |
| `variant_gwas` | `variant_gwas_gwascatalog.parquet` |
| `variant_alphamissense` | `variant_alphamissense_chr21.parquet` |
| `variant_gtex` | `variant_gtex_chr21.parquet` |

View names are unchanged. The last two take no tag because the table
name is already the source.

The table and source are also stamped into each file's **footer**
(`biofilter_table`, `biofilter_source`), and the build reads the table
from there instead of parsing the name — no rule splits
`variant_masters_gnomad_chr21` into table and source. This repaired a
real collision: `tables/variant_gwas/variant_gwas.parquet` shared the
stem `variant_gwas` with its parent stub, so dropping the stub's
manifest entry dropped the real one too.

### `20260910` as it stands

```
bundle_id   d72ba0636ec2aa34   (was e29a11604a326d2e)
entries     150                (29 core + 121 variant)
empty       chemical_masters   (excluded from the plan)
drift       2 columns, both added after this bundle was built
```

| table | rows | files |
| --- | ---: | ---: |
| `variant_masters` | 177,520,333 | 24 |
| `variant_molecular_effects` | 2,238,929,441 | 24 |
| `variant_rsid` | 712,358,067 | 24 |
| `variant_alphamissense` | 71,697,556 | 25 |
| `variant_gtex` | 18,470,502 | 23 |
| `variant_gwas` | 1,208,545 | 1 |

### Still open

Two reports beyond the four already known read the 4.2.x shape —
`report_snp_snp_model` and `report_variant_gene_location_model` use
`position_start`, `position_end`, `variant_id` and `allele_type`. Their
tests passed only because the fixture's own table definition was being
silently overridden by `Base.metadata.create_all()`, which matched the
old model. The fixtures now drop and recreate, so they test what they
declare — and the reports are on ADR-004's list.

`variant_predictions` has not run against a real VCF. The extraction is
covered by tests built from the real chr21 header, but the first full
gnomAD build is what will show its size.

278 tests pass.

---

## What still has to be reprocessed — 2026-09-14

The predictor columns are **not in any bundle**. Confirmed against
`20260910`:

```
variant_predictions present:                      False
predictor columns in variant_masters:             none
predictor columns in variant_molecular_effects:   none
```

They were never produced. The joint callset does not publish them, and
the VEP DTP read only the CSQ block until today. The extraction code is
in place and tested, but code does not fill a bundle — the VCFs have to
be read again.

### What a re-run costs, measured

Sizes from `Content-Length` on the gnomAD release bucket, 2026-09-14:

| to get | re-run | files | download | peak one chromosome |
| --- | --- | ---: | ---: | ---: |
| the 8 predictors | `variant_gnomad_vep` | 48 | **0.76 TB** | 63.1 GB (chr1: 18.9 exomes + 44.3 genomes) |
| `grpmax_joint`, `af_grpmax_joint` | `variant_gnomad_joint` | 24 | **0.88 TB** | 72.1 GB (chr1) |

Both fit the 150 GB working budget, because raw is discarded per source
as its parquet lands.

### The unit of work is the source, not the column

Re-running `variant_gnomad_vep` also rewrites `variant_molecular_effects`
(2.24 B rows) and `variant_rsid` (712 M) — one transform produces all
three. The CSQ side of its config did not change, so the output is the
same data; but the work cannot be scoped to the predictors alone.

`variant_masters` does **not** need re-running for the predictors. It
needs re-running only for the two `grpmax` columns, which is the 0.88 TB
line and a separate decision.

So there are two ways to a bundle with predictors:

- **VEP only** (0.76 TB): re-run `variant_gnomad_vep`, graft
  `variant_predictions` into `20260910` the way `variant_alphamissense`
  and `variant_gtex` were. Same config, same sources, same DTP — the
  argument that justified the first graft holds. `grpmax` stays missing.
- **Both** (1.64 TB): a full variant rebuild into a new bundle, which is
  also what closes the last two columns of drift.

---

## chr22 rebuilt — 2026-09-14, bundle `20260914_chr22`

A subset bundle to develop reports against, while the remaining
chromosomes run. Complete core (read from the staging database of the
previous build — no core source ran again) plus chr22 with everything
that landed after `20260910`.

```
bundle_id   97d769742b798f90
tables      32
drift       none
```

| table | rows |
| --- | ---: |
| `entities` | 203,393 |
| `gene_masters` | 72,660 |
| `variant_masters` | 2,889,803 |
| `variant_molecular_effects` | 39,777,067 |
| `variant_rsid` | 10,665,391 |
| `variant_predictions` | **16,453,342** |

`variant_masters` now carries 27 columns against 25 in `20260910` —
`grpmax_joint` and `af_grpmax_joint`, the config change §5 asked for.
Drift is **none**: the models and the parquet finally describe the same
table.

### Predictor coverage, chr22

| predictor | non-null | share |
| --- | ---: | ---: |
| `cadd_raw_score`, `cadd_phred` | 16,453,340 | 100.0% |
| `phylop` | 16,047,646 | 97.5% |
| `pangolin_largest_ds` | 10,406,739 | 63.3% |
| `spliceai_ds_max` | 8,253,506 | 50.2% |
| `polyphen_max` | 472,298 | 2.9% |
| `sift_max` | 469,569 | 2.9% |
| `revel_max` | 395,686 | 2.4% |

Biologically coherent: CADD and phyloP score any position, the splice
predictors only near junctions, and the missense trio only in coding
exons. None came back 100% null, which was the real risk — that is how
`mane_plus_clinical` went unnoticed.

`callset` keeps exomes (4,845,199) and genomes (11,608,143) apart rather
than merging them, so a disagreement is the report's decision.

### Sizing note for the report design

16.4 M predictor rows against 2.9 M variants in `variant_masters`. The
ratio is not transcript fanout — the predictors are captured **before**
the `AC >= 5` filter, as the rsID map is, so scores exist for rare
variants the bundle itself filtered out. Joined to `variant_masters` the
table yields 3,383,651 rows (622,319 exomes + 2,761,332 genomes).
Extrapolated genome-wide, expect roughly 10–12 GB. `predictors.filters.min_ac`
in `dtp_variant_gnomad_vep.json` trims it, at the cost of the rare
variant, which is where a predictor matters most.

### A bug the real build found that the tests did not

`variant_rsid` registered as the view **`variant_rsid_gnomad`**.

`_dedupe_rsid_files` rewrites each chromosome file in place with DuckDB's
`COPY ... TO ... (FORMAT parquet)`, which writes no key-value metadata —
so the `biofilter_table` stamp the sink had put in the footer was
stripped, and the build fell back to parsing the file name. That
fallback cannot tell a source suffix from part of a table name:
`variant_rsid_gnomad_chr22` reads as table `variant_rsid_gnomad`.

Two changes: the dedupe now streams DuckDB's DISTINCT through a pyarrow
writer carrying the sink's schema, so the stamp survives; and the
fallback logs a warning naming the table it guessed, instead of being
silent. The assembled bundle was repaired in place and the amendment
recorded.

The general lesson is the one this document keeps finding: a fallback
that cannot fail loudly will be wrong quietly.

284 tests pass.

### Correction: `variant_predictions` had a `callset` column it should not have

The column was justified on the assumption that exomes and genomes could
disagree on a score and that nothing could principledly choose between
them. Measured, that assumption is false.

| | |
| --- | ---: |
| rows | 16,453,342 |
| distinct variants | 15,551,628 |
| in one callset only | 14,649,914 |
| in both | 901,714 |
| of those, differing on CADD / REVEL / phyloP | **0** |
| of those, scored in one and null in the other | **0** |

Every predictor here is computed from the reference and the allele —
CADD, REVEL, SpliceAI, phyloP are properties of the variant, and gnomAD
annotates both VCFs from the same precomputed source. The callset does
not decide them. It is the same sentence already written in the rsID
dedupe.

The cost was a row multiplier: joined to `variant_masters` the table
returned 3,383,651 rows for 2,889,803 variants — 493,848 inflated.

Fixed: `callset` dropped from the schema and the model,
`_dedupe_rsid_files` generalised to `_dedupe_files` and applied to both
side tables. The chr22 bundle was collapsed in place, no re-download:

```
variant_predictions   16,453,342 -> 15,551,628 rows   227.6 -> 201.9 MB
join to variant_masters                 2,889,803 rows  (was 3,383,651)
bundle_id                                     8d7c03b38de588e8
drift                                                     none
```

Every variant in `variant_masters` has a predictor row, which follows
from capturing before the AC filter.

285 tests pass.

### `variant_predictions` now carries the bundle's variants, not gnomAD's

The table held a predictor row for every variant gnomAD scores,
including the ones the bundle filters out — five times more rows than
anything could join to.

The threshold is `AC` **summed across the callsets**, not either alone.
It cannot be applied while writing: the VCFs are read one after the
other, so the exome row is written before the genome AC exists. The
filter therefore runs in the same grouping pass as the dedupe, which
also drops the `ac` column once it has served its purpose. gnomAD's
joint AC is exactly `AC_exomes + AC_genomes`, so this reproduces the bar
`variant_masters` was built with; filtering each callset alone would
drop the variants that clear the combined bar without either reaching
it — 0.5% of the joint output, measured on chr21.

```
variant_predictions   15,551,628 -> 2,889,803 rows   201.9 -> 38.7 MB
                                    = variant_masters exactly
bundle_id                                     6a99b915f9530990
```

Genome-wide this takes the table from an estimated 10-12 GB to ~2 GB.

The existing chr22 bundle was filtered by semi-join against
`variant_masters` rather than re-downloaded. The DTP computes the sum
itself, so it stays independent of the joint branch as ADR-003 requires.

### VEP is the same in both callsets — measured

Ranged reads of 400 MB from each chr22 sites VCF, compared over the
window they share (10,736,024-12,151,319):

| | |
| --- | ---: |
| alleles with VEP, exomes | 8,695 |
| alleles with VEP, genomes | 609,220 |
| present in both | 3,039 |
| **byte-identical VEP** | **3,038 (99.97%)** |
| differing | 1 |
| only in exomes | 5,656 |
| only in genomes | 606,181 |

The single exception is not a disagreement about the variant: exomes
annotated it against a RefSeq transcript (`NR_132320.1`) and genomes
against an Ensembl one (`ENST…`), with the same consequence terms in a
different order.

So annotating a variant once, from whichever callset the `precedence`
setting reads first, and skipping it in the other loses nothing — the
161,827 skips on chr22 were all duplicates. `callset` in
`variant_molecular_effects` records which VCF a row was read from, not a
property of the annotation; its comment in the model said more than the
data supports and has been corrected.

291 tests pass.
