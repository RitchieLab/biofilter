# Deploying Biofilter 4.3 on the Penn LPC

Operational guide for whoever installs or updates Biofilter on the LPC:
building a bundle, publishing it on shared storage, and managing updates.

> **Audience:** the person who owns the Biofilter environment on the
> cluster. If you just want to run reports, see
> [lpc__quickstart.md](lpc__quickstart.md).

---

## 1. What a deployment consists of

Three things, and only the first is large:

| | What | Where |
|---|---|---|
| **A bundle** | A dated directory of parquet plus its manifest. The data. | `/project/hall_shared/datasets/biofilter/<YYYYMMDD>/` |
| **An install** | The `biofilter` CLI — a venv, or the container image. | `/project/hall_shared/biofilter/venv/` or `images/` |
| **A modulefile** | Puts the CLI on `PATH` and sets `BIOFILTER_BUNDLE`. | the lab's shared module tree |

There is no database server, no import phase and no per-user copy. One
bundle on shared storage serves any number of concurrent readers, because
every reader opens the files and queries them in its own process.

### Directory layout

```
/project/hall_shared/
├── datasets/biofilter/
│   ├── 20260914/                    ← a published bundle
│   │   ├── manifest.json            the catalogue readers trust
│   │   ├── bundle_plan.json         which sources, at which versions
│   │   ├── build_record.json        what each run did, in order
│   │   └── tables/                  the parquet
│   └── 20261120/                    ← the next one, beside it
└── biofilter/
    ├── venv/bf4-4.3.0/              the install users get via module
    ├── images/bf4-4.3.0.sif         the container alternative
    └── jobs/                        the .bsub scripts for builds
```

Date the snapshots and keep generations side by side. **Treat a published
bundle as immutable** — once its sources move on it cannot be rebuilt, so
it is the only surviving account of the data it holds.

---

## 2. Prerequisites

- LPC account with write access to both trees above
- `python/3.12` available; `apptainer` if you publish the image route
- Free space for the build: **150 GB working**, which is about twice the
  measured peak. See
  [Bundle Requirements](https://biofilter.readthedocs.io/en/latest/technical/bundle_requirements.html)
  for the measured disk, time and memory figures.

---

## 3. Building a bundle

The build produces the bundle directly. It creates a throwaway SQLite for
the core branch, writes variant parquet straight to disk, and assembles
only once every planned source has succeeded.

### 3.1 A registry to plan from

`bundle plan` reads the list of data sources from a database, so create a
small one first. It is a registry, not a data store — the build makes its
own staging database and never writes here.

```bash
biofilter db create-db --db-uri sqlite:///$SCRATCH/bf_registry.sqlite
```

### 3.2 Write the plan

```bash
biofilter bundle plan \
  --db-uri sqlite:///$SCRATCH/bf_registry.sqlite \
  --out /project/hall_shared/biofilter/jobs/bundle_plan.json
```

Edit the `include` flags to choose what the build covers. Two things the
file will tell you itself, worth repeating:

- **Order is the dependency declaration.** Sources run top to bottom and
  the core branch resolves entities against what ran before it, so `hgnc`
  precedes `gene_ncbi`, which precedes `ensembl`.
- **The plan is authoritative for one build.** The `active` flag in the
  registry only seeded its defaults, and each DTP's own JSON config still
  governs what is selected within a source.

Archive the plan with the bundle — the build copies it in, and it is the
record of how that bundle was made.

### 3.3 Run it

One LSF job, because the build is serial by design: running chromosomes
concurrently multiplies the peak disk footprint that the per-source
discard exists to control, and downloads gain nothing from concurrency.

```bash
#!/bin/bash
#BSUB -J bf4-build
#BSUB -o /project/hall_shared/biofilter/jobs/build-%J.log
#BSUB -W 72:00
#BSUB -M 32000
#BSUB -n 8

if ! type module >/dev/null 2>&1; then
    for init in /etc/profile.d/modules.sh /etc/profile.d/lmod.sh \
                /usr/share/lmod/lmod/init/bash /usr/share/Modules/init/bash; do
        [ -r "$init" ] && source "$init" && type module >/dev/null 2>&1 && break
    done
fi
source /project/hall_shared/hall_shared.sh
module load biofilter/4.3.0

biofilter bundle build \
  --plan /project/hall_shared/biofilter/jobs/bundle_plan.json \
  --data-root $SCRATCH/biofilter_data \
  --out /project/hall_shared/datasets/biofilter/$(date +%Y%m%d)
```

Budget about two days: roughly 6.5 h of download, 40 h of transform and
2.3 h of load, serial.

**Resume is the default.** An interrupted build re-runs only what is
pending — finished sources are skipped, because the staging ledger
recorded them and their output is still there. Just submit the same job
again. `--restart` throws the staging database away and starts over,
which you want only when a partially loaded source has to be excluded.

### 3.4 Building in stages

If the wall-clock limit or the disk will not take a whole genome at once,
build it in stages. Each later stage folds its chromosomes into the bundle
the first one published:

```bash
# stage 1 — core branch plus the first chromosomes, publishes the bundle
biofilter bundle build --plan plan_chr1_4.json \
  --out /project/hall_shared/datasets/biofilter/20260914

# stage 2+ — fold in, do not republish
biofilter bundle build --plan plan_chr5_8.json \
  --into /project/hall_shared/datasets/biofilter/20260914
```

The core is written once, on the first assembly. **The bundle id changes
with every fold**, because it is derived from content and the content
grew — `build_record.json` gains a `merges` entry per stage recording the
id before and after, so the sequence can be read back. A result stamped
with an earlier id names a bundle that no longer exists, which is worth
knowing if anyone runs reports between stages.

Re-folding the same chromosome is refused rather than silently doubling
its rows.

---

## 4. Verifying before publishing

```bash
BUNDLE=/project/hall_shared/datasets/biofilter/20260914

biofilter bundle info  "$BUNDLE"
biofilter db verify --in "$BUNDLE" --schema
```

`bundle info` prints what the bundle says about itself: its id, the
versions that built it, and its tables by branch.

`db verify` checks every file the manifest declares is present at the
right size, and `--schema` additionally checks that the tables carry the
columns this build expects, exiting non-zero on drift. That exit code is
what makes it usable as a gate.

Note that the hash tier only runs where the manifest recorded a digest,
and `bundle build` currently records size but not SHA-256 — so for a
freshly built bundle, `verify` is checking presence and size.

Then run something real before anyone else does:

```bash
biofilter --bundle "$BUNDLE" report run \
  --report-name platform_data_statistics --output /tmp/contents.csv

biofilter --bundle "$BUNDLE" report run \
  --report-name annotate_variant --input rs429358 --output /tmp/smoke.csv
```

**The variant report is the one that matters.** A gene report only
exercises the core branch; the variant path is what proves the partitioned
tables resolved into a single view across their per-chromosome files.

---

## 5. Publishing

Make the snapshot read-only so nothing can corrupt it:

```bash
chmod -R a-w /project/hall_shared/datasets/biofilter/20260914
```

That is belt and braces rather than the enforcement: the read path opens
parquet and has no write path at all. The filesystem flag protects against
everything *other* than Biofilter.

---

## 6. User access

Users reach the bundle through the module tree. The modulefile does two
things:

1. puts the `biofilter` CLI on `PATH` — from the venv, or via a wrapper
   around the image;
2. exports `BIOFILTER_BUNDLE` pointing at the current snapshot:

```
BIOFILTER_BUNDLE=/project/hall_shared/datasets/biofilter/20260914
```

Point it at the bundle **directory**, the one holding `manifest.json`, not
at `tables/`. With that set, users never pass a path.

The modulefile lives in the lab's shared module tree, outside this
repository.

### Container alternative

```bash
apptainer pull /project/hall_shared/biofilter/images/bf4-4.3.0.sif \
  docker://ghcr.io/ritchielab/biofilter-hpc:4.3.0

apptainer run \
  --bind /project/hall_shared/datasets/biofilter/20260914:/bundle:ro \
  --bind ~/bf4_output:/workspace \
  /project/hall_shared/biofilter/images/bf4-4.3.0.sif \
  report run --report-name annotate_gene --input APOE --output /workspace/apoe.csv
```

The image defaults `BIOFILTER_BUNDLE` to `/bundle`, so binding there is
all it takes. See [`docker/README.md`](../docker/README.md).

---

## 7. Updates

### New Biofilter version

Publish a new venv or image, add the modulefile version, smoke-test it
against the current bundle, then move `latest`. Users can pin an older
version until they are ready.

A newer Biofilter reading an older bundle **warns** — it prints which
release built the bundle and suggests `db verify --schema`. It does not
refuse, because refusing would make an archived bundle unreadable by the
only install available. The warning is recorded in every result's
provenance under `version_mismatch`.

The reverse — an older Biofilter meeting a bundle it cannot read — is a
hard refusal, with a message saying to update the package rather than the
bundle.

### New data snapshot

```bash
# 1) build a new dated bundle (§3)
# 2) verify and smoke-test it (§4)
# 3) freeze it (§5)
# 4) point the modulefile's BIOFILTER_BUNDLE at the new date
# 5) tell users the new date
```

Keep the previous snapshot in place. Anyone reproducing earlier work needs
it, and it cannot be rebuilt.

---

## 8. Retention and backup

A bundle is the artifact, so backup means keeping it rather than being
able to remake it. Budget about 30 GB per retained snapshot.

The three JSON files inside — `manifest.json`, `bundle_plan.json`,
`build_record.json` — travel with the data because they are the only
account of what it holds and how it was produced. Never separate them
from `tables/`.

---

## 9. References

- [Documentation](https://biofilter.readthedocs.io/)
- [Building Bundles](https://biofilter.readthedocs.io/en/latest/technical/building_bundles.html)
- [Bundle Requirements](https://biofilter.readthedocs.io/en/latest/technical/bundle_requirements.html) — measured disk, time and memory
- [`docker/README.md`](../docker/README.md) — the image, and running it under Apptainer
- [lpc__quickstart.md](lpc__quickstart.md) — what to hand users
