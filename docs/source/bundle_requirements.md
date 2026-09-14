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
bf = Biofilter(db_uri="parquet:///path/to/bundle")
bf.db.connect()
print(bf.db.bundle_id())
```

## One number worth keeping in mind

The same data is 21 GB as parquet and would be several hundred GB in
PostgreSQL — `variant_molecular_effects` alone costs 255 bytes per row
there against 4 in parquet. That ratio is why the build produces a bundle
instead of loading a database, and it is what makes a genome fit on a
laptop.
