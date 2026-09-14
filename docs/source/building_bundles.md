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

bf = Biofilter(db_uri="parquet:///path/to/bundle")
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
