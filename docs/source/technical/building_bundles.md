# Building Bundles

A bundle is what Biofilter produces and what it reads: a directory of
parquet files plus a manifest, built once and never changed. This page is
how one gets made.

## Why the build has two branches

Two facts about the data decide the shape of the build.

**The relational core is small and needs transactions.** Entities, aliases,
relationships, genes, proteins, pathways, diseases, GO and chemicals come to
about 7 million rows and 105 MB. They also resolve against each other —
creating an entity means asking what earlier sources already created — so
they need a transactional store while they are being assembled.

**Variant data is large and needs none of that.** It runs to roughly
2 billion rows. `variant_masters` carries no `entity_id` and no foreign
keys; variants link to genes through natural keys that VEP emitted, so
there is nothing to resolve. At about 4 bytes per row in parquet the whole
variant set is 15.6 GB — the same table in a row store costs about 255 bytes
per row, which puts it in the hundreds of gigabytes.

So the build splits. The **core branch** stages through a throwaway SQLite,
written through the ORM models. The **variant branch** writes its final
parquet directly, one file per chromosome, with no relational hop and no
load step. The branches do not depend on each other.

## Before the first build

`bundle plan` reads the list of data sources from a database, so you need
one before you can write a plan:

```bash
biofilter db create-db --db-uri sqlite:///biofilter_data/registry.sqlite
```

This creates the schema and applies the JSON seeds, including the data
source registry the plan is generated from. It is a registry, not a data
store — the build creates its own staging database and never writes here.

## The three commands

```bash
biofilter bundle plan  --out bundle_plan.json --db-uri sqlite:///biofilter_data/registry.sqlite
biofilter bundle build --plan bundle_plan.json
biofilter bundle info  ./biofilter_data/bundles/20260914
```

### plan

Writes the recipe. Every data source appears with an `include` flag, its
DTP and version, and the path of that DTP's field or tissue config, split
into the two branches.

Edit the flags to choose what the build covers. `--all-sources` enables
every source rather than only the ones currently flagged active — note that
this covers the whole genome, and the gnomAD download alone is about 1.5 TB.

**Order matters.** Sources run in the order they appear, and that order is
the dependency declaration — the core branch resolves entities against what
earlier sources created, so `hgnc` precedes `gene_ncbi`, which precedes
`ensembl`. Reordering the list reorders the build.

The plan is authoritative for one build. The `active` flag in the registry
only seeds a new plan's defaults, and each DTP's own JSON config still
governs what is selected *within* a source — which INFO fields, which GTEx
tissues.

Writing over an existing plan needs `--force`, because a plan may be the
only record of how a published bundle was made.

### build

```bash
biofilter bundle build \
  --plan bundle_plan.json \
  --data-root biofilter_data \
  --out ./bundles/20260914
```

Creates `<data-root>/staging/bundle_staging.sqlite`, runs every included
source against it, reclaims disk as it goes, and assembles only once all of
them have succeeded.

| Option | Default | Does |
|---|---|---|
| `--plan` | `bundle_plan.json` | The plan to build from |
| `--data-root` | `biofilter_data` | Where raw, processed and staging live |
| `--out` | `<data-root>/bundles/<YYYYMMDD>` | Where to write the bundle |
| `--restart` | off | Discard the staging database and start over |
| `--keep-raw` | off | Keep downloads after their output exists |
| `--min-free-gb` | `100` | Refuse to start a source below this much free space |
| `--into` | — | Fold this run's variant output into an existing bundle |
| `--keep-processed` | off | Copy the variant parquet in rather than moving it |
| `--no-assemble` | off | Run the sources but do not publish |

**Resume is the default.** An interrupted build re-runs only what is
pending: finished sources are skipped, both because their steps are recorded
in the staging ledger and because the output they left is still there.
`--restart` discards the staging database and starts over — needed when a
source that loaded partially has to be excluded, since resuming would leave
its rows in place.

**Disk is the binding constraint.** A full gnomAD download is about 1.53 TB
while the largest single chromosome is about 126 GB, which only fits because
raw files are dropped as soon as their parquet exists. What gets dropped
differs per branch: the variant branch keeps its parquet — it is the
artifact — and drops the raw VCFs; the core branch drops both once its rows
are in the staging database. `--keep-raw` disables this. `--min-free-gb`
stops a source from starting into a disk that cannot hold it; one gnomAD
chromosome needs up to 67 GB of raw before its parquet exists.

Sources run one at a time. That is a requirement, not a simplification:
running chromosomes concurrently multiplies the peak disk footprint, and
downloads gain nothing from concurrency — four parallel range streams
measured 66.6 MB/s against 64.2 MB/s for one, because the local link
saturates.

**Nothing is published unless every source succeeded.** A bundle missing a
table is indistinguishable from a complete one to whoever reads it. A failed
build stays resumable with its finished work intact.

### Building a genome in stages

A full variant branch is days of work and more raw input than most machines
hold at once. `--into` lets it be done in stages, each one folding its
chromosomes into a bundle that already exists:

```bash
# first stage publishes the bundle, core branch included
biofilter bundle build --plan plan_chr1_4.json --out ./bundles/20260914

# later stages fold their variant output in
biofilter bundle build --plan plan_chr5_8.json --into ./bundles/20260914
```

The core is not touched after the first assembly — it came out of the
staging database then and has not changed. Only the variant branch is
folded in.

Three things to know about a staged build:

- **The bundle id changes every time.** It is derived from content, and the
  content grew. That is the honest outcome and also the cost: a result
  already stamped with the old id now names a bundle that no longer exists.
  `build_record.json` gains a `merges` entry per fold, recording
  `bundle_id_before` and `bundle_id_after`, so the sequence can be read
  back.
- **It refuses to replace a file the bundle already has.** Re-folding the
  same chromosome would silently double its rows, and deciding which copy is
  right is not something the build can know.
- **The guards still apply.** A planned source that produced nothing stops
  the merge, and nothing is moved until that check passes — a collision
  found halfway would leave the bundle holding part of a run.

Two options exist for the same workflow. `--no-assemble` runs the sources
without publishing, for when assembling early would produce a bundle holding
only what has run so far. `--keep-processed` copies the variant parquet into
the bundle instead of moving it, so `processed/` still holds it afterwards —
useful for building a chromosome-subset bundle to develop against without
consuming the files the eventual full bundle needs.

### info

Prints what a bundle declares about itself, which after its sources have
moved on is the only surviving account of what it holds:

```
Bundle id:      39c56b50adeb1dc5
Biofilter:      4.3.0
Schema:         4.3.0
Built:          2026-09-14T21:28:42+00:00
Built from:     sqlite
Tables:         103
  core        30 table(s)       5,869,254 rows       71.9 MB
  variant     73 table(s)   1,313,355,486 rows   10,057.3 MB
Plan:           bundle_plan.json
Build record:   build_record.json
```

## Reading what you built

```bash
biofilter --bundle ./bundles/20260914 report list
```

```python
from biofilter import Biofilter

bf = Biofilter(bundle="./bundles/20260914")
result = bf.report.run("annotate_gene", input_data=["TP53"])

result.provenance["bundle_id"]   # which build these rows came from
result.write("genes.csv")        # writes genes.csv.provenance.json beside it
```

Opening a bundle warns if a table it carries is missing columns this build
expects, naming them. It does not refuse: a bundle built from a subset of
the sources legitimately has fewer tables, and refusing would make it
unusable for the ones it does have. For a strict check suitable for gating:

```bash
biofilter db verify --in ./bundles/20260914 --schema
```

See [The Read Path](read_path.md) for what happens between opening a
directory and getting rows back.

## Two properties to know about

### Ids are internal to one bundle

`entities.id`, `variant_masters.variant_id` and every other surrogate are
row identifiers **valid only inside the bundle that produced them**. They
are not stable across builds and no attempt is made to make them so.

This matters because the drift is small. Comparing two environments, `APOE`
was 11448 in one and 11450 in the other — and 11450 in the first is `APOF`,
a different gene in the same family. Nothing errors; the answer is simply
wrong.

Pin the bundle, not the id. Every report result carries `bundle_id` in its
provenance, and writing a result saves that provenance beside the file, so
an orphaned id can be recognised as one.

Cross-domain links use natural keys instead — `chromosome:position:ref:alt`
for variants, `HGNC_ID` and gene symbols for genes.

### A bundle cannot be rebuilt

Sources move on. Ensembl publishes a new release and the previous file stops
being served; gnomAD versions its callsets independently. Building the same
plan a year later produces different data.

That is expected rather than a defect. When a source changes you build a
**new** bundle, and the old one remains a snapshot of a moment that can no
longer be recreated. Reproducibility lives in the retained artifact, not in
the ability to rebuild it — which is why bundles are archived, and why the
manifest, the plan and the build record travel inside the bundle. They are
the only account of it that survives.
