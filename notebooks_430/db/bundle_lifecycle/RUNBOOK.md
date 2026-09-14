# Full bundle — staged runbook

Builds the complete bundle (minus chemicals) in five stages plus an
assembly pass. Every stage writes into the same staging database, so
finished work is never repeated; only the last stage assembles.

**Why staged.** The whole thing is 1.53 TB of downloads and roughly
30–40 hours of work. Splitting it gives checkpoints to inspect, and a
failure costs one stage rather than the run.

## Before starting

```bash
df -h ~          # need >150 GB free: peak is one gnomAD chromosome
                 # (chr1 raw is 67 GB) plus the growing bundle
```

The staging database is `biofilter_data/staging/bundle_staging.sqlite`.
Do **not** pass `--restart` between stages — that discards it and every
stage starts over.

## The stages

Each is one command. Run them in order; each is resumable on its own.

```bash
cd /Users/andrerico/Works/Sys/biofilter_430
B=".venv/bin/biofilter bundle build --data-root biofilter_data --no-assemble"
OUT=biofilter_data/bundles/full_20260910

# 1) Core: 15 sources, no chemicals. ~3-4 h, dominated by the load.
$B --plan notebooks_430/db/bundle_lifecycle/01_core.json --out $OUT

# 2) gnomAD chr1-5: the heavy end. chr1 alone is 126 GB of raw.
$B --plan notebooks_430/db/bundle_lifecycle/02_gnomad_1_5.json --out $OUT

# 3) gnomAD chr6-13
$B --plan notebooks_430/db/bundle_lifecycle/03_gnomad_6_13.json --out $OUT

# 4) gnomAD chr14-22, X, Y
$B --plan notebooks_430/db/bundle_lifecycle/04_gnomad_14_y.json --out $OUT

# 5) AlphaMissense, GTEx, GWAS — genome-wide, not per chromosome
$B --plan notebooks_430/db/bundle_lifecycle/05_other_variants.json --out $OUT

# 6) Assemble. Everything is already done, so this only builds the bundle.
.venv/bin/biofilter bundle build \
  --plan notebooks_430/db/bundle_lifecycle/06_assemble.json \
  --data-root biofilter_data \
  --out $OUT
```

## What each stage covers

| stage | sources | notes |
| ----- | ------- | ----- |
| 01_core | 15 | ChEBI and OMIM excluded |
| 02_gnomad_1_5 | 10 | joint + vep for chr1-5 |
| 03_gnomad_6_13 | 16 | chr6-13 |
| 04_gnomad_14_y | 22 | chr14-22, X, Y |
| 05_other_variants | 3 | AlphaMissense, GTEx, GWAS |
| 06_assemble | 66 | all of the above; assembly only |

## Checking on it

Progress lives in the staging database, not the log, which buffers:

```bash
sqlite3 biofilter_data/staging/bundle_staging.sqlite \
  "SELECT d.name, p.operation_type, p.status
   FROM etl_packages p JOIN etl_data_sources d ON d.id = p.data_source_id
   ORDER BY p.id DESC LIMIT 10;"

df -h ~ | tail -1
du -sh biofilter_data/raw biofilter_data/processed
```

## If a stage fails

Run the same command again. Finished sources are skipped — both because
their steps are recorded and because the output they left is still there.
Nothing is assembled while anything is outstanding.

The build refuses to start a source with less than 100 GB free
(`--min-free-gb`), because one gnomAD chromosome needs up to 67 GB of raw
before its parquet exists and the raw can be dropped.

## Two things to know

**Raw files are deleted as each source finishes.** That is what keeps the
peak near 70 GB instead of 1.53 TB. `--keep-raw` disables it, at the cost
of needing the full 1.53 TB.

**The core branch's processed files survive until the whole core stage
finishes**, because the `*_relationships` DTPs read the parquet their
masters wrote. They are ~72 MB, so the wait costs nothing.
