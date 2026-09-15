# BF4 User Documentation



<!-- ===== SOURCE FILE: docs/source/building_bundles.md ===== -->

# Building Bundles

A bundle is what Biofilter 4.3 produces and what it reads. It is a
directory of parquet files plus a manifest, built once and never changed.

This replaces the older model, where a PostgreSQL database was the
canonical store and a bundle was an export of it. There is no persistent
database any more: the build creates a throwaway SQLite, uses it, and
leaves the bundle behind.

## Why it is shaped this way

Two measurements drove it.

Variant data is the wrong shape for a relational store.
`variant_molecular_effects` costs about 255 bytes per row in PostgreSQL
and 4 in parquet — 64x. The full genome would be roughly 426 GB in that
one table, against 6.65 GB as parquet.

Everything else is small. The entire non-variant model — entities,
aliases, relationships, genes, proteins, pathways, diseases — is about
7 million rows and 105 MB. It was being hosted on a database server sized
for the 2 billion variant rows sitting next to it.

So the build splits in two. The **core branch** stages through SQLite,
because those sources resolve entities against each other and need a
transactional store. The **variant branch** writes parquet directly, with
no relational hop. They do not depend on each other.

## The three commands

```bash
biofilter bundle plan --out bundle_plan.json
biofilter bundle build --plan bundle_plan.json
biofilter bundle info ./biofilter_data/bundles/20260909
```

### plan

Writes the recipe. Every data source appears with an `include` flag, its
DTP and version, and the path of that DTP's field or tissue config, split
into the two branches.

Edit the flags to choose what the build covers.

**Order matters.** Sources run in the order they appear, and that order is
the dependency declaration — the core branch resolves entities against
what earlier sources created, so `hgnc` precedes `gene_ncbi`, which
precedes `ensembl`. Reordering the list reorders the build.

The plan is authoritative for one build. The `active` flag in the
database only seeds a new plan's defaults, and each DTP's own JSON config
still governs what is selected *within* a source — which INFO fields,
which GTEx tissues.

Writing over an existing plan needs `--force`, because a plan may be the
only record of how a published bundle was made.

### build

```bash
biofilter bundle build \
  --plan bundle_plan.json \
  --data-root biofilter_data \
  --out ./bundles/20260909
```

Creates `<data-root>/staging/bundle_staging.sqlite`, runs every included
source against it, reclaims disk as it goes, and assembles only once all
of them have succeeded.

`--out` defaults to `<data-root>/bundles/<YYYYMMDD>`. The build refuses to
overwrite a directory that already holds a bundle.

**Resume is the default.** An interrupted build re-runs only what is
pending: finished sources are skipped, both because their steps are
recorded and because the output they left is still there. `--restart`
discards the staging database and starts over — needed when a source that
loaded partially has to be excluded, since resuming would leave its rows
in place.

**Disk is the binding constraint.** A full gnomAD download is about
1.53 TB while the largest single chromosome is about 126 GB, which only
fits because raw files are dropped as soon as their parquet exists. What
gets dropped differs per branch: the variant branch keeps its parquet —
it is the artifact — and drops the raw VCFs; the core branch drops both
once its rows are in the staging database. `--keep-raw` disables this.

Sources run one at a time. That is a requirement, not a simplification:
running chromosomes concurrently multiplies the peak disk footprint, and
downloads gain nothing from concurrency — four parallel range streams
measured 66.6 MB/s against 64.2 MB/s for one, because the local link
saturates.

**Nothing is published unless every source succeeded.** A bundle missing a
table is indistinguishable from a complete one to whoever reads it. A
failed build stays resumable with its finished work intact.

### info

Prints what a bundle declares about itself: its id, the versions that
built it, when, and its tables broken down by branch.

## Reading a bundle

Point `--db-uri` at the bundle folder. No import, no database:

```bash
biofilter --bundle /path/to/bundle report list
biofilter --bundle /path/to/bundle \
  report run --report-name etl_status
```

From Python:

```python
from biofilter import Biofilter

bf = Biofilter(bundle="/path/to/bundle")
bf.db.connect()

print(bf.db.bundle_id())          # which data this is
df = bf.report.run("etl_status")
print(df.attrs["bundle_id"])      # which data the result came from
```

Opening a bundle warns if a table it carries is missing columns this
build expects, naming them. It does not refuse: a bundle built from a
subset of the sources legitimately has fewer tables, and refusing would
make it unusable for the ones it does have. For a strict check suitable
for gating:

```bash
biofilter db verify --in ./bundles/20260909 --schema
```

## Two properties to know about

### Ids are internal to one bundle

`entities.id`, `variant_masters.variant_id` and every other surrogate are
row identifiers **valid only inside the bundle that produced them**. They
are not stable across builds and no attempt is made to make them so.

This matters because the drift is small. Comparing two environments,
`APOE` was 11448 in one and 11450 in the other — and 11450 in the first is
`APOF`, a different gene in the same family. Nothing errors; the answer is
simply wrong.

Pin the bundle, not the id. Report results carry `bundle_id` in
`DataFrame.attrs` so an orphaned id can be recognised as one; note that
this attribute does not survive a CSV export.

Cross-domain links use natural keys instead — `chromosome:position:ref:alt`
for variants, `HGNC_ID` and gene symbols for genes.

### A bundle cannot be rebuilt

Sources move on. Ensembl publishes a new release and the previous file
stops being served; gnomAD versions its callsets independently. Building
the same plan a year later produces different data.

That is expected rather than a defect. When a source changes you build a
**new** bundle, and the old one remains a snapshot of a moment that can no
longer be recreated. Reproducibility lives in the retained artifact, not
in the ability to rebuild it — which is why bundles are archived, and why
the manifest, the plan and the build record travel inside the bundle.
They are the only account of it that survives.



<!-- ===== SOURCE FILE: docs/source/bundle_requirements.md ===== -->

# What Building a Bundle Costs

Measured on a full human-genome build completed 2026-09-12: 66 data
sources, the gnomAD v4 joint and VEP callsets for all 24 chromosomes,
plus AlphaMissense, GTEx and GWAS. Chemicals (ChEBI) were excluded.

These are figures from that run, not estimates, except where marked.

## Disk

| | |
| --- | --- |
| **Peak usage** | **~70 GB** |
| Raw downloaded and discarded | ~1.57 TB |
| Final bundle | 21 GB |
| Staging database | 0.9 GB |

The peak is **not** the total downloaded. Raw files are deleted as soon
as the parquet that supersedes them exists, so what has to fit at once is
the largest single source plus what has already been produced:

```
gnomad_joint_chr1 raw        67 GB   ← the largest single download
parquets produced so far      2 GB
staging database              1 GB
                            ──────
                             70 GB
```

That peak occurs early, on chromosome 1. Later sources are smaller while
the accumulated parquet grows, and the two never coincide badly: the
second-highest point is assembly, at about 43 GB.

Without the discard the same build needs the full 1.57 TB. This is why
`bundle build` reclaims per source and why it refuses to start one below
a free-space floor (`--min-free-gb`, default 100 GB).

### What to provision

| | |
| --- | --- |
| **Working space during a build** | **150 GB free** |
| **Storage per retained bundle** | **30 GB** |

150 GB is about twice the measured peak. It also keeps the default
`--min-free-gb` floor of 100 GB workable: the floor is checked before
each source starts, and between sources the space comes back, because raw
is discarded while only parquet accumulates. At the last source there is
still around 129 GB free.

Provisioning exactly 100 GB would not work — the build would refuse to
start, since free space at the peak drops below its own floor.

30 GB per bundle rather than the 21 GB this one occupies: a later gnomAD
release or adding ChEBI moves that number, and bundles are retained
rather than rebuilt, so archive storage is *N* × 30 GB for however many
you keep.

## Time

Wall-clock across the run was 64.5 hours, but that includes gaps between
stages. Summing the steps themselves:

| step | hours | what dominates |
| ---- | ----- | -------------- |
| extract | 6.5 | network: 1.57 TB at ~64 MB/s |
| transform | 40.3 | parsing VCF and writing parquet |
| **load** | **2.3** | only the core branch touches SQLite |
| **total** | **49.1** | |

**Plan for about two days**, serial. The build runs one source at a time
by design: concurrency multiplies the peak disk footprint that the
discard exists to control, and downloads gain nothing from it — four
parallel range streams measured 66.6 MB/s against 64.2 MB/s for one,
because the local link saturates.

Transform is the target for anything faster, not download or load.

## Memory

**Provision 32 GB.**

That is close to what was validated rather than an extrapolation: the
machine that ran this build has 36 GB and showed no pressure. The peak
itself was **not measured**, so 32 GB is a safe recommendation, not a
derived requirement — a smaller machine may well be enough, and that is
worth instrumenting on a future build.

What is known about the shape: transforms stream in batches of 250,000
rows and hold one writer per chromosome, so memory does not scale with
chromosome size. The exception is the rsID map's deduplication pass,
which hands a whole chromosome's file to DuckDB — on chr1 that is a
larger working set than anything else in the build.

## What you get

```
bundle/
  manifest.json          the dictionary BF4 reads
  bundle_plan.json       which sources were included, at which versions
  build_record.json      per-step timings, source URLs, content hashes
  tables/
    entities.parquet             ┐
    gene_masters.parquet         │ core: 41 files, one per table
    entity_relationships.parquet ┘
    variant_masters/
      variant_masters_chr1.parquet … chr24.parquet
    variant_molecular_effects/
    variant_rsid/
    variant_alphamissense/
    variant_gtex/
    variant_gwas.parquet
```

From the measured build:

| | |
| --- | --- |
| Files | 114 parquet |
| Rows | 3,135,885,652 |
| Size | 21 GB |
| Core | 41 tables, 5.87 M rows, 72 MB |
| Variant | 73 tables, 3.13 B rows, 21.2 GB |

`manifest.json` is what makes the directory a bundle rather than a pile
of files. It names every table, its row count and size, which branch
produced it, and carries a `bundle_id` derived from that content — so the
id can be recomputed to check the bundle has not changed.

`bundle_plan.json` and `build_record.json` travel with it because **a
bundle cannot be rebuilt**. Ensembl publishes a new release and the file
the build read stops being served; gnomAD versions its callsets
independently. Running the same plan a year later produces different
data. These files are the only surviving account of what a given bundle
holds, which is why they sit inside it.

## Reading it

No import, no database:

```bash
biofilter --bundle /path/to/bundle report list
```

```python
bf = Biofilter(bundle="/path/to/bundle")
bf.db.connect()
print(bf.db.bundle_id())
```

## One number worth keeping in mind

The same data is 21 GB as parquet and would be several hundred GB in
PostgreSQL — `variant_molecular_effects` alone costs 255 bytes per row
there against 4 in parquet. That ratio is why the build produces a bundle
instead of loading a database, and it is what makes a genome fit on a
laptop.



<!-- ===== SOURCE FILE: docs/source/cli_reference.md ===== -->

# CLI Reference

## Global

```bash
biofilter [--db-uri URI] [--debug] COMMAND ...
```

Groups:

- `config`
- `db`
- `etl`
- `report`

## Config

- `biofilter config show`
- `biofilter config get SECTION.KEY`
- `biofilter config set SECTION.KEY VALUE`
- `biofilter config init --path .`

## DB

- `biofilter db ping`
- `biofilter bundle plan`
- `biofilter bundle build`
- `biofilter bundle info`
- `biofilter db create-db`
- `biofilter db upgrade`
- `biofilter db backup`
- `biofilter db restore`
- `biofilter db export`
- `biofilter db import`

## ETL

- `biofilter etl update`
- `biofilter etl update-all`
- `biofilter etl explain`
- `biofilter etl status`
- `biofilter etl restart`
- `biofilter etl rollback`
- `biofilter etl index`

## Report

- `biofilter report list`
- `biofilter report explain --report-name <name>`
- `biofilter report example-input --report-name <name>`
- `biofilter report available-columns --report-name <name>`
- `biofilter report run --report-name <name> [options]`

Key `report run` options:

- `--input`, `--input-file`, `--input-column`
- `--param`, `--params-json`, `--params-file`
- `--params-template`
- `--output`



<!-- ===== SOURCE FILE: docs/source/configuration.md ===== -->

# Configuration

Biofilter resolves settings from:
1. command-line options (highest priority)
2. environment variables (`BIOFILTER_BUNDLE`, or `DATABASE_URL` /
   `BIOFILTER_DB_URI` for a database)
3. `.biofilter.toml`
4. internal defaults

## Where the data is

Reports read a **bundle**; the ETL and bundle builds write to a
**database**. Both live under `[database]`, and a configured bundle wins
over a configured `db_uri` — the same precedence `--bundle` has over
`--db-uri`.

```toml
[database]
# Where reports read from. A bundle is a directory — the one holding
# manifest.json, not its tables/ subdirectory. Pointing at tables/
# reaches the parquet but leaves behind the bundle id, the plan, and the
# table map the reader resolves its views from.
#
# A relative path is relative to THIS FILE, not the working directory, so
# it means the same thing from the project root and from a notebook two
# levels down.
bundle = "./biofilter_data/bundles/20260914"

# Only for writing. Keep credentials out of this file — use DATABASE_URL.
# db_uri = "postgresql+psycopg2://user:pass@host/biofilter_dev"
```

`biofilter config show` prints which one is in effect.

## Common Commands

Show resolved config:

```bash
biofilter config show
```

Get one value:

```bash
biofilter config get database.db_uri
```

Set one value:

```bash
biofilter config set database.db_uri "sqlite:///biofilter_dev.db"
```

Initialize template:

```bash
biofilter config init --path .
```

## Typical Keys

- `database.db_uri`
- `etl.data_root`

## Accepted `database.db_uri` values

| Scheme | Example | Writes |
|---|---|---|
| PostgreSQL | `postgresql+psycopg2://user:pass@host:5432/biofilter_prod` | yes |
| SQLite | `sqlite:///biofilter_dev.db` | yes |
| Parquet bundle | `parquet:///path/to/bundle` | no (read-only) |

The `parquet://` scheme reads a Parquet bundle directly via DuckDB, for
environments without a database server. See [Parquet Backend](parquet_backend.md).

## Tips

- Prefer `--db-uri` in CI or one-off commands.
- Prefer `DATABASE_URL` in containers and orchestrators.
- Prefer `.biofilter.toml` for local development defaults.



<!-- ===== SOURCE FILE: docs/source/database.md ===== -->

# Database Operations

Biofilter 4.3 has no persistent database. A build creates a throwaway
SQLite, stages the core sources through it, and leaves a parquet bundle
behind — see [Building Bundles](building_bundles.md).

The commands here remain for working with a database directly: creating
one for development, inspecting it, and moving data in and out.

## Creating and checking

```bash
biofilter db create-db --db-uri "sqlite:///biofilter_dev.db"
biofilter db ping --db-uri "sqlite:///biofilter_dev.db"
```

`create-db` builds the schema with `create_all` and applies the master
seeds. There is no migration step: 4.3 removed Alembic, because a
database is built once and a schema change produces a new bundle rather
than an in-place migration.

Applying seed updates to an existing database:

```bash
biofilter db upgrade
```

This is idempotent and seed-only. In earlier versions it also ran an
Alembic upgrade first.

## Backup and restore

Physical snapshot of a database, engine-specific:

```bash
biofilter db backup --out ./backups/dev.snapshot
biofilter db restore --in ./backups/dev.snapshot
```

## Bundles

A bundle is normally produced by `bundle build`. `db export` writes one
from an existing database, which is how bundles were made before 4.3:

```bash
biofilter db export --out ./exports/biofilter_bundle --format parquet
biofilter db import --in ./exports/biofilter_bundle --format parquet
```

Validate one without a database:

```bash
biofilter db verify --in ./exports/biofilter_bundle
biofilter db verify --in ./exports/biofilter_bundle --no-hashes
biofilter db verify --in ./exports/biofilter_bundle --schema
```

`--no-hashes` checks presence and size only. `--schema` also checks that
the tables present carry the columns this build expects, and exits 1 on
any problem, so CI can gate on it.

A bundle can be read directly, without importing it:

```bash
biofilter --bundle ./exports/biofilter_bundle report list
```

See [Parquet Backend](parquet_backend.md) and
[Building Bundles](building_bundles.md).



<!-- ===== SOURCE FILE: docs/source/entity_and_omics.md ===== -->

# Entity Model and Omics Domains

## Why `Entity` Exists

Biofilter 4 uses an entity-centric model so different biological domains can share identity and relationships.

Instead of keeping each source isolated, BF4 stores a common entity layer and links domain records to it. This enables cross-domain queries and reusable knowledge.

## Core Entity Objects

At the center of the schema:

- `EntityGroup`
  - semantic type bucket (for example: Variants, Genes, Proteins, Diseases)
- `Entity`
  - persistent concept record with activity/conflict flags and ETL provenance
- `EntityAlias`
  - names/codes/synonyms from multiple systems (`alias_type`, `xref_source`)
- `EntityRelationshipType`
  - relationship semantics (typed edge meaning)
- `EntityRelationship`
  - directed link between two entities with provenance

Practical effect:

- you can resolve aliases from many sources to one entity identity
- you can traverse relationships across domains without hardcoded paths

## Domain-Specific Master Data

The entity core is complemented by domain tables (master data), such as:

- genes (`GeneMaster` and gene-related tables)
- variants (variant master/effects/GWAS tables)
- proteins (`ProteinMaster`, Pfam links)
- pathways (`PathwayMaster`)
- gene ontology (`GOMaster`, `GORelation`)
- diseases (`DiseaseMaster`)
- chemicals (`ChemicalMaster`)

These domain tables provide rich attributes, while entities/aliases/relationships provide integration.

## Omics Domains in BF4

### Operational Domains (current)

Domains with active schema + ETL/report usage today:

- Variants
- Genes
- Proteins
- Pathways
- Gene Ontology
- Diseases
- Chemicals

These groups define semantic space and allow gradual expansion without redesigning the core model.

## How This Appears in ETL and Reports

- ETL loads source-specific master/relationship data and writes provenance (`ETLPackage`).
- Reports such as `entity_filter` and `entity_relationship_model` operate directly on this entity layer.
- Because identities are persistent, updates can be incremental and still query-consistent across domains.



<!-- ===== SOURCE FILE: docs/source/etl.md ===== -->

# ETL Operations

ETL is how Biofilter ingests, normalizes and versions knowledge from
external sources. Each source is a **data source**, driven by a **DTP**
(Data Transformation Package) through three steps: `extract`,
`transform`, `load`.

In 4.3 the ETL is a step inside a bundle build rather than an end in
itself. `bundle build` runs it for every source in a plan; the commands
here drive it directly, which is what you want when developing a DTP or
re-running one source. See [Building Bundles](building_bundles.md).

## Two branches

Sources fall into two groups, and they behave differently:

**Core** — genes, proteins, pathways, diseases, GO, chemicals and the
relationships between them. These DTPs resolve entities against each
other, so they run in order and load into a relational store: the
throwaway SQLite during a build, or whatever database you point them at
directly.

**Variant** — gnomAD, AlphaMissense, GTEx, GWAS. These write parquet
directly and never load into a database. Their `load()` raises
`NotImplementedError` by design, so run them with explicit steps:

```bash
biofilter etl update --data-source gnomad_joint_chr21 --run-step extract
biofilter etl update --data-source gnomad_joint_chr21 --run-step transform
```

Variant tables link to genes by natural key — `HGNC_ID`, gene symbols —
never by an entity id, which is what lets the two branches be built
independently.

## Commands

```bash
biofilter etl update --data-source hgnc
biofilter etl update --source-system KEGG
biofilter etl update-all
biofilter etl status
biofilter etl explain --data-source hgnc
```

Restrict or force individual steps:

```bash
biofilter etl update --data-source hgnc --run-step transform
biofilter etl update --data-source hgnc --force-step transform
```

A step is skipped when its input hash is unchanged **and** the output it
produced still exists. Deleting a processed file causes it to be rebuilt.

`etl update` exits non-zero when a source fails.

## Field and tissue selection

The variant DTPs read a JSON config next to them in
`biofilter/modules/etl/dtps/config/`, listing every field a source
publishes with a `load` flag. They are include-lists: gnomAD's joint
callset alone carries 664 INFO fields, so an exclude-list would silently
adopt whatever a future release adds.

The same mechanism selects GTEx tissues — all 50 are listed, 13 enabled
by default. Note that GTEx ships every tissue in one tarball and does not
expose them individually, so the selection narrows the transform and the
output, not the download.

Frequency filters live in the same files. The gnomAD joint config
defaults to `min_ac: 5`; setting it lower keeps rarer variants at
proportionally larger output.

## Adding a DTP

1. `biofilter/modules/etl/dtps/dtp_<name>.py` with `extract()`,
   `transform()` and, for a core source, `load()`
2. `biofilter/modules/etl/dtps_explain/dtp_<name>.md` — source, behaviour,
   caveats
3. Register the data source in the seed
4. Test with `biofilter etl update --data-source <name>`

See [Developer Extensions](developer_extensions.md).



<!-- ===== SOURCE FILE: docs/source/getting_started/finding_reports.md ===== -->

# Finding a Report

## What a report is

A **report** is a prepared question you can ask the data.

You give it an input — a list of gene symbols, an rsID, a disease name,
sometimes nothing at all — and it gives you back a table. Under the
covers it knows which files to open, how to resolve the names you typed
against the ones the sources use, and how to follow the links between
genes, proteins, pathways, diseases and variants. You do not write
queries and you do not need to know how the data is laid out.

```bash
biofilter --bundle /shared/bundles/bf4_20260912 \
  report run --report-name entity_filter --input APOE,TP53
```

Every report takes the same shape: a name, an input, optional parameters,
and a table out — to your screen, or to a CSV, or straight into a
DataFrame if you are working in Python.

BF4 ships around thirty of them: looking entities up, summarising what is
connected to what, annotating variants, and checking what data the bundle
actually holds. Three ways to find the one you want.

## 1. Browse the catalog

The [Report Catalog](../report_catalog.md) is the full index, grouped by
purpose. Each entry gives you:

- A one-line description of what it does.
- A link to its **Explain Guide** — parameters, output columns, examples.
- A link to a **notebook tutorial** that runs end-to-end.

Use the catalog when you want to see everything available.

## 2. Ask the assistant

For questions in plain language — *"I have a list of genes from a GWAS,
which report shows what pathways they touch?"* — there is a GPT assistant
trained on BF4's reports and terminology:

**[BF4 Assistant](https://chatgpt.com/g/g-6887cf80355c8191ab3f88bbd8955e0d-biofilter-4-assistant)**

Its source — system prompt, FAQ, and a manifest of every report with its
inputs and use cases — lives in the repository's `assistent/` folder.

## 3. Ask Biofilter itself

If you already have it installed:

```bash
biofilter report list
```

And for any one of them:

```bash
biofilter report explain --report-name entity_filter
```

That prints the full guide in your terminal — what it expects, what it
returns, and how to call it.

## Good places to start

Most reports fall into three kinds of work.

**Filtering** — narrowing a list down to what the data recognises or
supports.

| Report | Use it when |
| ------ | ----------- |
| `entity_filter` | You have a list of names and want to know which ones BF4 recognises |
| `gene_to_variant_filtering` | You have genes and want the variants inside them |
| `variant_list_intersect` | You have two variant lists and want what they share |

**Annotation** — attaching what is known to something you already have.

| Report | Use it when |
| ------ | ----------- |
| `annotate_gene` | You want to browse the gene catalog |
| `variant_single_gene_annotation` | You have variants and want their effect on one gene |
| `entity_neighborhood_summary` | You have one entity and want everything connected to it |

**Modeling** — building the sets and pairs an analysis consumes.

| Report | Use it when |
| ------ | ----------- |
| `variant_binning` | You want variants grouped into bins for burden testing |
| `snp_snp_pair_generator` | You need SNP pairs for an interaction scan |
| `entity_relationship_model` | You want the relationship graph around a set of entities |

And one worth running once on any bundle you have just been handed:

| Report | Use it when |
| ------ | ----------- |
| `etl_status` | You want to see which data sources went into this bundle, and when |

It answers "what is actually in here?" — which version of each source,
and whether it loaded.

## Next step

Picked one? [Run your first report](running_reports.md).



<!-- ===== SOURCE FILE: docs/source/getting_started/index.md ===== -->

# Getting Started

Biofilter 4 (BF4) resolves biological entities — genes, proteins,
pathways, diseases, variants — tracks the relationships between them, and
exposes all of it through ready-to-use reports.

What you read is a **bundle**: a directory of parquet files with a
manifest describing them. No database server, no import step. Point
Biofilter at a bundle and run reports.

## Choose your path

### Someone gave me a bundle

This is the common case, and it takes minutes.

1. [Install Biofilter](installing.md) — pip or Docker.
2. [Point at the bundle](reading_a_bundle.md) — one URI, no setup.
3. [Find a report](finding_reports.md) that fits your question.
4. [Run it](running_reports.md) — CLI or Python.

### I need to build a bundle

Only if no one has one for the data you need. The full human genome
means 1.5 TB of downloads, processed and discarded as the build goes, so
plan for **150 GB of working space**.

1. [Install Biofilter](installing.md) — from source if you will change DTPs.
2. [Build a bundle](../building_bundles.md) — `bundle plan`, then `bundle build`.
3. [Run a report](running_reports.md) against what you built.

[What it costs](../bundle_requirements.md) has the measured figures for
disk, memory and runtime before you start.

## What you'll need

- **Python 3.10+**, or **Docker** if you prefer containers.
- **A bundle** — a path you can read, local or on a shared filesystem.

## One thing to carry with you

Ids inside a bundle — `entities.id`, `variant_id` — are internal to that
bundle. They are not stable across bundles, and a stale one still
resolves: to a different gene, without an error. Pin the bundle, not the
id. [Reading a bundle](reading_a_bundle.md) explains how results carry
their origin.

## Where this guide stops

Once you can run a report, the rest goes deeper:

- [Report catalog](../report_catalog.md) — every report, with tutorials.
- [Building bundles](../building_bundles.md) — the plan/build/inspect flow.
- [What a build costs](../bundle_requirements.md) — measured disk, time, memory.
- [Parquet backend](../parquet_backend.md) — how views are registered, performance.
- [Data sources and ingestion](../etl.md) — where the data comes from, and how it gets in.
- [Configuration](../configuration.md) — `.biofilter.toml` options.
- [Troubleshooting](../troubleshooting.md) — common errors.



<!-- ===== SOURCE FILE: docs/source/getting_started/installing.md ===== -->

# Installing Biofilter

Three installation methods, in order of simplicity. Pick **one**.

## Which one should I use?

| Method     | Best for                                           | Requires                |
| ---------- | -------------------------------------------------- | ----------------------- |
| **pip**    | Most users — running reports, notebooks, scripting | Python 3.10+            |
| **Docker** | Avoiding any Python setup, reproducible CI runs    | Docker                  |
| **Source** | Contributors, debugging, modifying BF4 itself      | Python 3.10+ and Poetry |

## pip (recommended)

```bash
pip install biofilter
biofilter --help
```

That's it — `biofilter` is now available as a CLI command and the `biofilter` Python package is importable.

To verify:

```bash
biofilter --help
python -c "from biofilter import Biofilter; print('OK')"
```

## Docker

Build the application-only image:

```bash
docker build -t biofilter:bf4 -f docker/Dockerfile "https://github.com/RitchieLab/biofilter.git#biofilter3r"
```

Mount the bundle and point at it. The container needs read access to the
bundle directory and somewhere to write results:

```bash
docker run --rm -it \
  -v /shared/bundles/bf4_20260912:/bundle:ro \
  -v "$(pwd):/workspace" \
  -e BIOFILTER_BUNDLE="/bundle" \
  --entrypoint /bin/bash \
  biofilter:bf4
```

To run one report and keep the output:

```bash
docker run --rm \
  -v /shared/bundles/bf4_20260912:/bundle:ro \
  -v "$(pwd)/outputs:/workspace/outputs" \
  -e BIOFILTER_BUNDLE="/bundle" \
  biofilter:bf4 \
  biofilter report run --report-name etl_status --output /workspace/outputs/etl_status.csv
```

The bundle is mounted read-only because nothing writes to it — refreshing
data means a newer bundle, not an update to this one.

## From source

For contributors or anyone modifying BF4 itself.

```bash
git clone https://github.com/RitchieLab/biofilter.git
cd biofilter
poetry install
poetry run biofilter --help
```

## Next step

Once installed, [point Biofilter at a bundle](reading_a_bundle.md) — one URI, no server to set up.



<!-- ===== SOURCE FILE: docs/source/getting_started/reading_a_bundle.md ===== -->

# Pointing Biofilter at a Bundle

## What a bundle is

A **bundle** is a folder. Inside it are the knowledge base's data —
genes, proteins, pathways, diseases, variants and the relationships
between them — already gathered from their original sources, cleaned up
and written as parquet files. A `manifest.json` sits alongside them as
the dictionary: what each file holds, how many rows, and which version of
the data this is.

Biofilter both writes bundles and reads them. The data lives in the
folder, not in a server, so there is nothing to install, start or
connect to. To run a query you give Biofilter the path and it does the
rest.

A bundle is a **photograph**: it captures the sources exactly as they
were on the day it was built. Nothing inside it changes afterwards. When
the sources move on — a new Ensembl release, a new gnomAD callset — you
build a new bundle rather than update this one, and the old one stays
readable for anyone who needs to reproduce work done against it.

A full human-genome bundle is around **21 GB** and holds roughly three
billion rows across 114 files. You can keep it on a laptop, a shared
drive, or an HPC filesystem — anywhere you can read a folder.

If someone has given you one, this page is the whole setup. If you need
to build one yourself, see [Building Bundles](../building_bundles.md) —
borrow one first if you can.

## Point at it

Give Biofilter the bundle folder. It finds the data and the manifest
inside:

```bash
biofilter --bundle /shared/bundles/bf4_20260912 report list
```

If you use the same bundle every day, set it once:

```bash
export BIOFILTER_BUNDLE="/shared/bundles/bf4_20260912"

biofilter report list
biofilter report run --report-name etl_status
```

Or in `.biofilter.toml`:

```toml
[database]
db_uri = "parquet:///shared/bundles/bf4_20260912"
```

Relative paths work; Biofilter resolves them.

### The older form

`--db-uri "parquet:///shared/bundles/bf4_20260912"` does the same thing.
The `parquet://` scheme dates from when Biofilter spoke to several
database backends and you had to say which one. With bundles it carries
no information, so `--bundle` is the plainer way to say it. `--db-uri`
remains for the cases that really are a database: a staging SQLite during
a build, or an existing PostgreSQL.

Passing both is an error rather than a guess about which you meant.

## Check it worked

```bash
biofilter bundle info /shared/bundles/bf4_20260912
```

```
Bundle id:      e29a11604a326d2e
Biofilter:      4.3.0
Built:          2026-09-12T11:46:28+00:00
Tables:         114
  core        41 table(s)       5,869,266 rows       71.9 MB
  variant     73 table(s)   3,130,016,386 rows   21,226.3 MB
```

Reports work unchanged — the same code runs over parquet as over a
database, with DuckDB underneath.

## The one thing to watch

**Ids belong to one bundle.** `entities.id`, `variant_id` and the rest
are internal row identifiers, valid only inside the bundle that produced
them. They are not stable across bundles, and the drift is small enough
to be dangerous: id 11450 is APOE in one bundle and APOF — a different
gene in the same family — in another. Nothing errors; the answer is
simply about the wrong gene.

So pin the bundle, not the id. Every report result carries the bundle it
came from:

```python
df = bf.report.run("annotate_gene", input_data=["APOE"])
df.attrs["bundle_id"]     # 'e29a11604a326d2e'
```

Note that this does not survive a CSV export. If you are writing ids to a
file that will be read back later, write the bundle id beside them.

## If something is wrong with the bundle

```bash
biofilter db verify --in /shared/bundles/bf4_20260912 --no-hashes --schema
```

`--schema` also checks that the tables present carry the columns this
version of Biofilter expects. Opening a bundle only warns about that, so
a partial bundle stays usable; this turns it into an error you can gate
on.

## Next step

[Find a report](finding_reports.md), then [run it](running_reports.md).



<!-- ===== SOURCE FILE: docs/source/getting_started/running_reports.md ===== -->

# Running Your First Report

Two ways to run any report: from the command line (CLI) or from Python (notebook or script). Both produce the same output. Pick whichever fits your workflow.

## CLI — quickest path

List what's available:

```bash
biofilter report list
```

Run a report and print the result to the terminal:

```bash
biofilter report run --report-name etl_status
```

Save the output to a CSV file:

```bash
biofilter report run --report-name etl_status --output etl_status.csv
```

Pass parameters with `--param KEY=VALUE`:

```bash
biofilter report run \
  --report-name entity_filter \
  --input "BRCA1" \
  --input "TP53" \
  --param match_mode=exact
```

For input lists too long for the command line, use `--input-file`:

```bash
biofilter report run \
  --report-name entity_filter \
  --input-file ./genes.txt
```

To see what parameters a report accepts:

```bash
biofilter report explain --report-name entity_filter
```

## Python API — best for notebooks and scripts

```python
from biofilter import Biofilter

bf = Biofilter()  # picks up DB from .biofilter.toml or DATABASE_URL

df = bf.report.run(
    "entity_filter",
    input_data=["BRCA1", "TP53", "APOE"],
    match_mode="exact",
)

print(f"{len(df)} rows")
df.head()
```

Every report returns a pandas `DataFrame`, so you can chain it with the rest of your analysis without saving to disk first.

## A complete first example

Here's a full session — install, point, run:

```bash
# Install
pip install biofilter

# Point at a bundle
export BIOFILTER_BUNDLE="/shared/bundles/bf4_20260912"

# Run
biofilter report list
biofilter report run --report-name etl_status --output etl_status.csv
```

Open `etl_status.csv` and you'll see every data source that went into the
bundle, with the version and timestamp of the run that produced it.

## Next steps

- Browse the [Report Catalog](../report_catalog.md) for what else you can do.
- Each report has a notebook tutorial in [`biofilter_legacy/bf4_420/notebooks/Templates/`](https://github.com/RitchieLab/biofilter/tree/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates) — copy one and adapt it.
- For deeper CLI options, see the [CLI Reference](../cli_reference.md).
- For Python API patterns, see [Reports](../reports.md).



<!-- ===== SOURCE FILE: docs/source/index.md ===== -->

# Biofilter Documentation

Lightweight, user-focused documentation for running Biofilter today.

This documentation is intentionally practical:
- install/configure quickly (PyPI, source, or Docker)
- build a parquet bundle and read it
- run reports via CLI/API
- troubleshoot common operational issues

```{toctree}
:maxdepth: 2
:caption: Getting Started

getting_started/index
getting_started/installing
getting_started/reading_a_bundle
getting_started/finding_reports
getting_started/running_reports
```

```{toctree}
:maxdepth: 2
:caption: Reference

system_overview
entity_and_omics
developer_extensions
configuration
building_bundles
bundle_requirements
database
parquet_backend
schema
etl
reports
report_catalog
cli_reference
troubleshooting
```



<!-- ===== SOURCE FILE: docs/source/parquet_backend.md ===== -->

# Parquet Backend (read-only)

Since **4.2.0**, Biofilter can read the knowledge base directly from a
**Parquet bundle** using DuckDB as the query engine — no database server, no
container, no per-user copy of the data.

This exists for environments where running PostgreSQL is not an option. The
motivating case is HPC: on a shared cluster users typically have no privileges
to run a database daemon, no persistent service host, and no reasonable way to
maintain a multi-hundred-gigabyte data directory per user.

---

## When to use it

| Backend | Use for | Writes |
|---|---|---|
| PostgreSQL | Production, ETL, multi-user with writes | yes |
| SQLite | Local development, small datasets | yes |
| **Parquet bundle** | **HPC, shared read-only snapshots, air-gapped analysis** | **no** |

Choose the Parquet backend when all of the following hold:

- you only need to **read** — run reports, no ETL and no migrations;
- the data can be distributed as a point-in-time snapshot;
- you want many users querying the same files concurrently without copies.

If you need to ingest data or apply migrations, use PostgreSQL or SQLite.

---

## Connecting

Point Biofilter at the bundle's `tables/` directory with the `parquet://` URI
scheme:

```bash
export BIOFILTER_DB_URI="parquet:///shared/bundles/bf4_2026_06"

biofilter report run \
  --report-name annotate_gene \
  --input APOE \
  --output apoe.csv
```

`DATABASE_URL` works too, as does the `--db-uri` option for a single command:

```bash
biofilter --db-uri "parquet:///shared/bundles/bf4_2026_06" \
  report run --report-name annotate_variant --input rs429358 --output out.csv
```

From Python:

```python
from biofilter import Biofilter

bf = Biofilter(bundle="/shared/bundles/bf4_2026_06")
df = bf.report.run("annotate_variant", input_data=["rs429358"])
```

Both `parquet://relative/path` and `parquet:///absolute/path` are accepted;
the path is expanded and resolved to an absolute path either way.

---

## How it works

1. The `parquet://` URI is translated internally to an in-memory DuckDB engine
   (`duckdb:///:memory:`).
2. On connect, Biofilter scans the directory and registers one SQL `VIEW` per
   `*.parquet` file, backed by DuckDB's `read_parquet()`.
3. A SQLAlchemy `StaticPool` keeps every session on the same connection, so all
   sessions share the in-memory catalog where those views live.
4. From that point on the ORM resolves normally.

**Reports require no changes.** They run through the same ORM layer used by
PostgreSQL and SQLite, so every report in the catalog works unmodified against
a bundle.

### Partitioned tables

Files whose name contains `_chr_` are **skipped** during view registration.

On PostgreSQL, `variant_masters` and `variant_molecular_effects` are
partitioned by chromosome, and an export writes both the consolidated parent
(`variant_molecular_effects.parquet`) and its 25 children
(`variant_molecular_effects_chr_1.parquet`, …). Registering both would make
every row appear twice, so only the consolidated parent is queried.

This means the consolidated file is **required**. A bundle containing only the
`_chr_*` children will connect successfully but the corresponding table will
not exist, and variant reports will fail.

---

(read-only-enforcement)=
## Read-only enforcement

The backend is read-only at two levels:

- **Storage** — DuckDB rejects any write against a `read_parquet` view.
- **Application** — the `Database.read_only` flag exposes the same information
  to Biofilter code.

Deployments typically add a third level by making the bundle directory
non-writable on disk (`chmod -R a-w`).

Consequences:

- `biofilter etl update` / `update-all` — **not supported**; run the ETL
  against PostgreSQL and export a new bundle.
- `db upgrade` / `db create-db` — **not supported** against a bundle.
- Refreshing the data means producing a **new bundle**, not modifying the
  current one.

---

## Producing a bundle

Export from any existing PostgreSQL or SQLite instance:

```bash
biofilter db export \
  --db-uri "postgresql+psycopg2://user:password@host:5432/biofilter_prod" \
  --out /shared/bundles/bf4_2026_06 \
  --format parquet
```

This writes:

```
bf4_2026_06/
├── manifest.json
└── tables/
    ├── entities.parquet
    ├── gene_masters.parquet
    └── ...
```

The `tables/` subdirectory is what `parquet://` points at.

> **Production-scale caveat.** On a full production database the partitioned
> variant tables make the single-command export impractical: exporting the
> consolidated parent forces PostgreSQL to UNION all 25 partitions on every
> chunk. The bundle has to be assembled in stages instead — export the
> partition children individually, then concatenate them outside the database.
> The full procedure is documented in the
> [LPC deployment guide](https://github.com/RitchieLab/biofilter/blob/main/biofilter_legacy/bf4_420/notebooks/Templates/lpc__deploy.md).

---

## Performance

Queries stream from disk with column pruning and predicate pushdown, so memory
stays low even against billion-row tables. Measured on the production snapshot:

| Workload | Storage | Wall clock | Peak memory |
|---|---|---|---|
| 10,000 rsIDs against `variant_molecular_effects` (1.79 B rows) | local NVMe | 1.18 s | 89 MB |
| Same workload | shared GPFS | 15.52 s | 89 MB |
| End-to-end CLI report (`annotate_variant`, 3 rsIDs) | local NVMe | 1.22 s | — |

Shared network storage costs roughly an order of magnitude in wall clock and
still lands well inside interactive range.

---

## Troubleshooting

**`No *.parquet files found in <dir>`**
The URI points at the wrong directory. It must point at `tables/`, not at the
bundle root that holds `manifest.json`.

**`parquet:// directory not found: <dir>`**
The path does not exist or is not readable. Check the mount, and remember the
path is resolved relative to the process working directory when given without
a leading slash.

**A table is missing or a variant report fails**
The bundle is likely missing a consolidated parent for a partitioned table.
Check that `variant_molecular_effects.parquet` exists alongside the
`_chr_*` files — the children alone are not enough.

**A write command fails**
Expected. The backend is read-only; see [Read-only enforcement](#read-only-enforcement).

---

## See also

- [Pointing Biofilter at a bundle](getting_started/reading_a_bundle.md) — the quick setup
- [Database Operations](database.md) — export/import commands
- [Configuration](configuration.md) — how `db_uri` is resolved



<!-- ===== SOURCE FILE: docs/source/report_catalog.md ===== -->

# Report Catalog

Complete index of all reports available in Biofilter 4.
Each report has a **name** (used in CLI and Python API), a brief description,
and links to its explain guide and interactive notebook tutorial where available.

For general usage — how to run, list, and introspect reports — see [Reports](reports.md).

---

## Running any report

```bash
# CLI
biofilter report run --report-name <name> [--param KEY=VALUE ...] [--output file.csv]
biofilter report explain --report-name <name>
biofilter report run --report-name <name> --params-template
```

```python
# Python API
df = bf.report.run("<name>", param1=value1, param2=value2)
```

---

## ETL & Platform Monitoring

Reports for inspecting the state of the ETL pipeline and the knowledge base.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `etl_status` | Current status of all ETL packages (active, last run, row counts) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_etl_status.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__etl_status.ipynb) |
| `etl_packages` | Full provenance log of all ETL executions with timestamps and file hashes | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_etl_packages.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__etl_packages.ipynb) |
| `platform_data_statistics` | Row counts and coverage metrics across all master tables | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_platform_data_statistics.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__platform_data_statistics.ipynb) |
| `db_pg_table_stats` | PostgreSQL table sizes, row estimates, and bloat metrics *(PostgreSQL only)* | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_db_pg_table_stats.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__db_pg_table_stats.ipynb) |
| `db_pg_index_stats` | PostgreSQL index usage, size, and scan counts *(PostgreSQL only)* | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_db_pg_index_stats.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__db_pg_index_stats.ipynb) |

---

## Entity & Relationship

Reports for exploring the biological entity graph.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `entity_filter` | Filter and list entities (genes, pathways, diseases, …) by type, source, or name pattern | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_entity_filter.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__entity_filter.ipynb) |
| `entity_relationship_model` | Retrieve all entities related to an input list through shared biological groups (pathways, diseases, GO, PPI) | — | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__entity_relationship_model.ipynb) |
| `entity_neighborhood_summary` | Resolve heterogeneous inputs (gene:, disease:, pathway:, …) and return a 1-hop neighborhood summary grouped by neighbor type | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_entity_neighborhood_summary.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__entity_neighborhood_summary.ipynb) |

---

## Annotation Masters

Reference tables exposing the full content of each biological domain in the knowledge base.
Useful for exploring available terms before using them as filters in other reports.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `annotate_gene` | All genes with HGNC symbol, Ensembl ID, locus, and source provenance | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotate_gene.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__annotations_master_gene.ipynb) |
| `annotate_pathway` | All pathways across all source systems (Reactome, KEGG, …) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotate_pathway.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__annotations_master_pathway.ipynb) |
| `annotate_protein` | All proteins with UniProt IDs and gene mappings | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotate_protein.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__annotations_master_protein.ipynb) |
| `annotate_disease` | All diseases with MONDO/ClinGen IDs and gene associations | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotate_disease.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__annotations_master_disease.ipynb) |
| `annotate_go` | All Gene Ontology terms (BP, MF, CC) with gene memberships | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotate_go.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__annotations_master_go.ipynb) |
| `annotate_chemical` | All chemical compounds (ChEBI) with gene and pathway associations | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotate_chemical.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__annotations_master_chemical.ipynb) |
| `annotate_variant` | Full annotation for input variants: frequencies, pathogenicity scores, VEP consequences per transcript, AlphaMissense | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotate_variant.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__annotations_master_variant.ipynb) |

---

## Variant Analysis

Reports for annotating and filtering genomic variants.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `variant_binning` | Assign variants to genomic bins; useful for burden-test preparation | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_binning.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__variant_binning.ipynb) |
| `variant_gene_location_model` | Map variants to overlapping gene loci with distance and region annotations | — | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__variant_gene_location_model.ipynb) |
| `variant_annotation_expanded` | Full annotation expansion for a variant list (consequence, AF, predictions) | — | — |
| `variant_single_gene_annotation` | **Phase 1** — Given a seed variant, returns the seed gene and all partner genes sharing biological context | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_single_gene_annotation.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__variant_single_gene_annotation.ipynb) |
| `gene_to_variant_filtering` | **Phase 2** — Collect and filter variants across a gene list with SQL-level pathogenicity filters | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_gene_to_variant_filtering.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__gene_to_variant_filtering.ipynb) |
| `annotation_variant_regulatory_evidence` | Variant ↔ gene regulatory evidence (eQTL / sQTL). Accepts gene symbols, rsids, or chr:pos as input; returns one row per (variant × tissue × regulated gene) with effect size and p-value | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_annotation_variant_regulatory_evidence.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__annotation_variant_regulatory_evidence.ipynb) |

---

## Variant Interaction Modeling

Direct variant-to-variant interaction modeling from a pre-genotyped input list.
Both variants in every pair come from the input — no DB expansion.

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `variant_modeling` | Input variants → gene overlap → group co-membership → Variant×Variant pairs with group_support_count weight | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_modeling.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__variant_modeling.ipynb) |

---

## SNP×SNP Interaction Pipeline

Reports implementing the biologically-informed SNP×SNP interaction workflow.
See the full pipeline tutorial and methods document for end-to-end guidance.

| Resource | Link |
|---|---|
| Pipeline notebook | [pipeline__from_single_variant_to_interactions.ipynb](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb) |
| Pipeline methods doc | [pipeline__from_single_variant_to_interactions.md](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/pipeline__from_single_variant_to_interactions.md) |

| Report | Phase | Description | Explain | Notebook |
|---|---|---|---|---|
| `variant_single_gene_annotation` | Phase 1 | Seed variant → partner gene list via biological network | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_single_gene_annotation.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__variant_single_gene_annotation.ipynb) |
| `gene_to_variant_filtering` | Phase 2 | Gene list → filtered, annotated variant set (Lista A) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_gene_to_variant_filtering.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__gene_to_variant_filtering.ipynb) |
| `variant_list_intersect` | Phase 2.5 | Lista A ∩ Lista B → Lista C (genotyped subset, PLINK-ready) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_variant_list_intersect.md) | [Pipeline notebook](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb) |
| `snp_snp_pair_generator` | Phase 3 | Lista D → annotated interaction pairs with configurable pairing strategy | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_snp_snp_pair_generator.md) | [Pipeline notebook](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/pipeline__from_single_variant_to_interactions.ipynb) |
| `snp_snp_model` | Legacy | Earlier SNP×SNP pair model — expands variants from gene loci (superseded by `variant_modeling`) | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_snp_snp_model.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__snp_snp_model.ipynb) |

---

## Pathway Burden Pipeline

Pipeline for prioritising pathways given a list of significant genes (e.g., ExWAS hits) and a target pathway list, using cross-source convergence scoring.

| Resource | Link |
|---|---|
| Pipeline notebook | [pipeline__pathway_burden_score.ipynb](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/pipeline__pathway_burden_score.ipynb) |
| Pipeline methods doc | [pipeline__pathway_burden_score.md](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/pipeline__pathway_burden_score.md) |

---

## Utilities

| Report | Description | Explain | Notebook |
|---|---|---|---|
| `template` | Blank report template for development and testing | [Guide](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter/modules/report/reports_explain/report_template.md) | [Tutorial](https://github.com/RitchieLab/biofilter/blob/biofilter3r/biofilter_legacy/bf4_420/notebooks/Templates/reports__qry_template.ipynb) |

---

## Coverage summary

| Status | Count |
|---|---|
| Reports with explain guide + notebook | 20 |
| Reports with explain guide only | 2 (`variant_list_intersect`, `snp_snp_pair_generator` — covered by pipeline notebook) |
| Reports with notebook only | 2 (`entity_relationship_model`, `variant_gene_location_model`) |
| Reports with neither | 1 (`variant_annotation_expanded`) |
| **Total** | **25** |



<!-- ===== SOURCE FILE: docs/source/reports.md ===== -->

# Reports

Reports are the main read interface over Biofilter knowledge and ETL provenance.

For the complete index of all available reports with links to explain guides and notebook tutorials, see the **[Report Catalog](report_catalog.md)**.

## Discover and Inspect

List reports:

```bash
biofilter report list
biofilter report list --verbose
```

Explain report:

```bash
biofilter report explain --report-name etl_status
```

Show example input:

```bash
biofilter report example-input --report-name entity_relationship_model
```

Show output columns:

```bash
biofilter report available-columns --report-name etl_packages
```

## Run Reports

Basic run:

```bash
biofilter report run --report-name etl_status
```

Export CSV:

```bash
biofilter report run --report-name etl_packages --output ./etl_packages.csv
```

Template-driven params:

```bash
biofilter report run --report-name entity_relationship_model --params-template
```

## Dynamic Parameter Injection

Inputs:

```bash
biofilter report run --report-name entity_filter --input BRCA1 --input TP53
biofilter report run --report-name entity_filter --input-file ./entities.csv --input-column symbol
```

Options:

```bash
biofilter report run --report-name entity_relationship_model \
  --input TP53 --input BRCA1 \
  --param relationship_scope=input_to_any \
  --param deduplicate_pairs=true
```

JSON/YAML params:

```bash
biofilter report run --report-name entity_relationship_model --params-json '{"relationship_scope":"input_to_any"}'
biofilter report run --report-name entity_relationship_model --params-file ./params.yaml
```

Load one param from file:

```bash
biofilter report run --report-name entity_relationship_model --input TP53 --param relationship_types=@./relationship_types.txt
```

## Explain Guides

`report explain` prefers markdown guides stored in:

- `biofilter/modules/report/reports_explain/report_<module>.md`

If a guide file is missing, Biofilter falls back to the report class `explain()` method.

This model keeps report documentation maintainable:
- update the report module when behavior changes
- update the paired explain markdown for user-facing guidance

## Practical Examples

Repository-level example guides:

- `docs/reports/snp_snp_model.md`



<!-- ===== SOURCE FILE: docs/source/system_overview.md ===== -->

# System Overview

## What Is Biofilter 4 (BF4)?

Biofilter 4 is a persistent, entity-centric biological knowledge platform.

In practice, BF4 is designed to:
- ingest biological data sources through ETL
- normalize and store knowledge in a local or shared database
- expose this knowledge through CLI, Python API, SQL, and reports

The key idea is persistence: build once, reuse across many analyses.

## High-Level Architecture

BF4 has four practical layers:

1. Knowledge Storage (Database)
- relational schema for entities, aliases, relationships, and ETL metadata

2. ETL Orchestration
- `extract -> transform -> load` pipelines per data source
- package-level tracking and status history

3. Data Access and Report Layer
- generic report manager
- dynamic report execution with shared CLI/API contracts

4. User Interfaces
- CLI (`biofilter ...`)
- Python API (`bf = Biofilter(...)`)
- notebooks and SQL workflows

## Deployment Modes

BF4 supports these modes:

- Local managed database (for development, isolated workflows)
- Shared database (team/centralized operations)
- Containerized app-only runtime with external database (portable execution)
- Read-only Parquet bundle via DuckDB (HPC and other environments where no
  database server is available) — see [Parquet Backend](parquet_backend.md)

All modes use the same CLI/API patterns. The Parquet mode is read-only:
reports work unchanged, but ETL and migrations require a writable backend.

## ETL Data Lifecycle

For each data source, BF4 follows a staged lifecycle:

1. Extract
- source files are downloaded to a raw staging area

2. Transform
- raw files are normalized into curated intermediate outputs (typically parquet)

3. Load
- curated outputs are loaded into the database

Operationally, this enables:
- resumable updates
- selective rollback/restart
- optional cleanup of raw/processed files after successful loads

## Provenance and Reproducibility

Each ETL step execution is tracked via ETL packages, including:
- data source identity
- operation type (`extract`, `transform`, `load`, `rollback`)
- status and timestamps
- hash linkage across steps
- error notes/stats when failures occur

This metadata is used by:
- `biofilter etl status`
- `etl_status` and `etl_packages` reports

## Report Explain Guides

Report tutorials/explains are stored as markdown files in:

- `biofilter/modules/report/reports_explain/report_<module>.md`

`biofilter report explain --report-name <name>` prefers these guides. If not found, BF4 falls back to the report class `explain()` method.

For a focused explanation of the entity-centric model and current omics domains, see [Entity Model and Omics Domains](entity_and_omics.md).



<!-- ===== SOURCE FILE: docs/source/troubleshooting.md ===== -->

# Troubleshooting

## Report Not Found

- Run `biofilter report list`.
- Use `--report-name` with one of the listed names.

## Input Conflict in `report run`

If you pass `--input`/`--input-file`, do not also pass input keys through params (`input_data`, `items`, `input_path`).

## Explain Page Not Found

- Check if guide exists at `biofilter/modules/report/reports_explain/report_<module>.md`.
- If missing, Biofilter will fall back to class `explain()`.

## PostgreSQL-only Reports

`db_pg_table_stats` and `db_pg_index_stats` require PostgreSQL.

## Parquet Backend Errors

`No *.parquet files found in <dir>` — the `parquet://` URI must point at the
bundle's `tables/` directory, not at the bundle root holding `manifest.json`.

`parquet:// directory not found: <dir>` — the path does not exist or is not
readable; check the mount.

A write command failing (`etl update`, `db upgrade`) is expected:
the Parquet backend is read-only. Run those against PostgreSQL or SQLite.

A missing table or a failing variant report usually means the bundle lacks the
consolidated parent for a partitioned table — the `_chr_*` files alone are
skipped by design. See [Parquet Backend](parquet_backend.md).

## Migration/Upgrade Issues

Use:

```bash
biofilter db verify --in ./bundles/<YYYYMMDD> --schema
biofilter db upgrade
```

## ETL Batch Resume

If `etl update-all` was interrupted, run it again. Successful data sources are skipped.

## Report Output Not Found (Docker)

If you run BF4 in a container and export with `--output`, mount a host volume and write to that mounted path.

Example:

```bash
docker run --rm \
  -e DATABASE_URL="postgresql+psycopg2://user:pass@host:5432/db" \
  -v "$(pwd)/outputs:/workspace/outputs" \
  biofilter:bf4 \
  biofilter report run --report-name etl_status --output /workspace/outputs/etl_status.csv
```
