# Deploying Biofilter 4 on the Penn LPC

Operational guide for the **maintainer** who installs or updates BF4 on the
LPC cluster: building the Parquet bundle from a production snapshot,
publishing it on shared storage, and managing future updates.

> **Audience:** the person who owns the BF4 environment on the cluster.
> If you just want to _run reports_, see [lpc\_\_quickstart.md](lpc__quickstart.md)
> instead.

---

## 0. What changed in 4.2.0

Up to 4.1.x, the LPC deployment restored a full PostgreSQL database on the
cluster: a ~480 GB `pgdata/` directory bind-mounted into a container that
embedded PG 16. That model is retired.

Since **4.2.0**, BF4 reads the knowledge base directly from a **Parquet
bundle** via DuckDB — see [ADR-001](../Andre/ADR/0001-duckdb-parquet-strategy.md)
for the rationale and benchmarks. The practical consequences for deployment:

| | 4.1.x (retired) | 4.2.0+ (current) |
|---|---|---|
| Storage on cluster | ~480 GB `pgdata/` | Parquet bundle (much smaller, columnar) |
| Database server | PG 16 inside the container | none |
| Concurrency | one postmaster → per-user copies | native, read-only, multi-user |
| First run | `initdb` + `pg_restore` (3–8 h) | none — read files in place |
| Image size | ~1.5 GB | ~400 MB |
| Write operations | possible on cluster | **not possible** — bundle is read-only |

The legacy flow is preserved in [Appendix A](#appendix-a--legacy-postgresql-deployment-41x)
for reference; do not use it for new deployments.

---

## 1. Directory layout

```
/project/hall_shared/biofilter/
├── images/
│   └── bf4-hpc-<version>.sif           ← Apptainer image (optional; see §5)
├── venv/
│   └── bf4-<version>/                  ← native venv used by the build jobs
├── jobs/                               ← the .bsub scripts and helper scripts
│   ├── fase1.bsub
│   ├── fase1_resume.bsub
│   ├── fase1_merge.bsub
│   ├── merge_variant_molecular_effects.py
│   └── regen_manifest.py
└── databases/
    └── <snapshot-date>/
        ├── bundle/
        │   ├── manifest.json
        │   └── tables/                 ← *.parquet — this is what users read
        ├── logs/                       ← LSF job output
        └── pgdata/                     ← only if building the bundle on the LPC (§3)
```

Reference copies of the build scripts live in this repo at
[`notebooks/Andre/`](../Andre/) — copy them to `jobs/` on the cluster.

Dating the snapshot folders lets you keep multiple generations side by side.
Treat each snapshot as **immutable** once published.

---

## 2. Prerequisites

- LPC account with write access to `/project/hall_shared/biofilter/`
- `apptainer` and `python/3.12` modules available on the cluster
- A native venv with BF4 installed (used by the merge/manifest steps, which
  run outside the container and need only `pyarrow`)
- Free space: budget for the Parquet bundle **plus** the `pgdata/` if you
  build on the cluster (§3). The build jobs warn below 250 GB free.

---

## 3. Building the Parquet bundle

The bundle is produced by `biofilter db export --format parquet`, which needs
a live PostgreSQL to read from. Two options:

- **On the VPS** (recommended going forward) — export against the production
  database, then `rsync` only the bundle to the cluster. Avoids restoring a
  ~480 GB `pgdata/` on the LPC entirely.
- **On the LPC** (what was done for the `20260514` snapshot) — restore the
  dump into a cluster-side `pgdata/` first (Appendix A, §A.2), then export
  using the legacy PG-bundled image. Documented below because it is the path
  that was actually exercised.

Either way, the bundle contents and the partition caveat in §3.2 are the same.

### 3.1 Why this is three jobs, not one

`variant_masters` and `variant_molecular_effects` are **partitioned by
chromosome** in PostgreSQL, with child tables named `<parent>_chr_<N>`
(1–22, X, Y, MT → 23, 24, 25).

`db export` discovers tables via SQLAlchemy's inspector, which on PostgreSQL
returns **both the partitioned parent and every child** as independent tables.
So a full export writes 25 `variant_molecular_effects_chr_N.parquet` files
_and_ a consolidated `variant_molecular_effects.parquet`.

The problem is the parent. Exporting it runs
`SELECT * FROM variant_molecular_effects`, forcing PostgreSQL to UNION all 25
partitions on every chunk. On the 1.79-billion-row table this is slow enough
to be impractical — the first attempt was killed after the job wall clock ran
out.

The consolidated file is **not optional**: DuckDB registers one view per
`*.parquet` but explicitly skips any file whose name contains `_chr_`
(otherwise the same rows would appear twice — once via the parent, once via
the children). Without `variant_molecular_effects.parquet`, that table simply
does not exist for the reports.

The workaround is to build the parent outside the database: export the 25
children (each a fast direct scan), then concatenate them with
`pyarrow.ParquetWriter` in streaming append mode.

### 3.2 The three phases

| Phase | Script | What it does |
|---|---|---|
| 1 | [`fase1.bsub`](../Andre/fase1.bsub) | Full `db export`. Completes every table except the giant partitioned parent. |
| 1-resume | [`fase1_resume.bsub`](../Andre/fase1_resume.bsub) | `db export --table variant_molecular_effects_chr_1 … _chr_25` — the 25 children only. |
| 1-merge | [`fase1_merge.bsub`](../Andre/fase1_merge.bsub) | Concatenates the children into the consolidated parent, then regenerates `manifest.json`. |

Each script has a configuration block at the top (`ROOT`, `SNAPSHOT`,
`VERSION`). Edit those, then submit:

```bash
cd /project/hall_shared/biofilter/jobs

bsub < fase1.bsub          # wait for completion, check logs/export-<jobid>.out
bsub < fase1_resume.bsub   # wait for completion
bsub < fase1_merge.bsub    # wait for completion
```

Phase 1 is expected to end with `variant_molecular_effects.parquet` missing or
truncated — that is normal and is exactly what phases 1-resume and 1-merge
exist to repair.

**Phase 1-merge runs in the native venv, not the container.** It needs only
`pyarrow`; no PostgreSQL, no Apptainer:

```bash
module load python/3.12
source /project/hall_shared/biofilter/venv/bf4-<version>/bin/activate

python jobs/merge_variant_molecular_effects.py \
    --in  databases/<snapshot>/bundle/tables \
    --out databases/<snapshot>/bundle/tables/variant_molecular_effects.parquet

python jobs/regen_manifest.py \
    --bundle databases/<snapshot>/bundle \
    --biofilter-version <version>
```

`regen_manifest.py` is required because the bundle was assembled by several
partial exports, so the `manifest.json` left by phase 1 covers only a subset
of the files. It rescans `tables/*.parquet`, rewrites the manifest in place,
and backs up the previous one as `manifest.json.bak`.

### 3.3 Schema note on null columns

`merge_variant_molecular_effects.py` promotes `null`-typed columns to nullable
`string` before writing. Without this, a first chunk coming from an all-null
partition fixes the schema as `null` and every later chunk fails to cast.

The same fix exists in
[`transfer.py`](../../biofilter/modules/db/transfer.py) for the in-database
export path, and has been upstream **since 4.2.0**. The `.bsub` scripts from
the `20260514` build bind-mount a patched `transfer.py` over the one inside
the container:

```bash
--bind "${ROOT}/images/patches/transfer.py:/opt/biofilter/venv/lib/python3.11/site-packages/biofilter/modules/db/transfer.py:ro"
```

**Drop that bind when using a 4.2.0 or newer image** — the patch is already in
the installed package.

### 3.4 Verifying the bundle

```bash
BUNDLE=/project/hall_shared/biofilter/databases/<snapshot>/bundle

# every table present, and the consolidated parent among them
ls -1 "${BUNDLE}/tables"/*.parquet | wc -l
ls -lh "${BUNDLE}/tables/variant_molecular_effects.parquet"

# manifest covers all files and row counts look sane
python -c "import json,sys; m=json.load(open('${BUNDLE}/manifest.json')); \
print(len(m['tables']),'tables'); \
[print(f\"{t['name']:45s} {t['rows']}\") for t in m['tables'][:10]]"
```

Expected row counts for the current production data:

```
variant_masters                     152,084,680
variant_molecular_effects         1,793,092,126
variant_gene_regulatory_evidence     18,231,184
```

---

## 4. Publishing the bundle

Once verified, make the snapshot read-only so no user can corrupt it:

```bash
chmod -R a-w /project/hall_shared/biofilter/databases/<snapshot>/bundle
```

Read-only is enforced at three levels — filesystem permissions here, DuckDB
rejecting writes against `read_parquet` views, and the `Database.read_only`
flag at the application layer. A single copy safely serves the whole group;
no per-user duplication.

---

## 5. User access

Users reach the bundle through the lab's module tree
(see [lpc\_\_quickstart.md](lpc__quickstart.md)):

```bash
source /project/hall_shared/hall_shared.sh
module load biofilter/<version>
```

The modulefile must do two things:

1. put the `biofilter` CLI on `PATH` (from the native venv, or via a wrapper
   around the Apptainer image);
2. export `BIOFILTER_DB_URI` pointing at the current snapshot's `tables/`
   directory:

```
parquet:///project/hall_shared/biofilter/databases/<snapshot>/bundle/tables
```

With that set, users never pass `--db-uri`. The modulefile itself lives in the
lab's shared module tree, outside this repository.

**Container alternative** — for users who prefer Apptainer over the module,
the read-only HPC image works with a bind mount:

```bash
apptainer run \
  --bind /project/hall_shared/biofilter/databases/<snapshot>/bundle/tables:/bundle:ro \
  --bind ~/bf4_output:/workspace \
  --env BIOFILTER_DB_URI=parquet:///bundle \
  bf4-hpc.sif \
  biofilter report run --name annotation_master_gene --input APOE --output /workspace/apoe.csv
```

See [docker/hpc/README.md](../../docker/hpc/README.md) for image details and
publishing.

---

## 6. Smoke test after install

```bash
source /project/hall_shared/hall_shared.sh
module load biofilter/<version>

biofilter --version
biofilter report list | head

# gene path
biofilter report run --name annotation_master_gene --input APOE --output /tmp/smoke_gene.csv

# variant path — exercises the merged parent table specifically
biofilter report run --name annotation_master_variant --input rs429358 --output /tmp/smoke_variant.csv

head -3 /tmp/smoke_gene.csv /tmp/smoke_variant.csv
```

Both should complete in about a second. **The variant report is the one that
matters** — it is the only check that the consolidated
`variant_molecular_effects.parquet` was merged correctly. A gene report passing
tells you nothing about the merge.

Expected performance (from the ADR-001 POC): 10,000 rsIDs annotated against
the 1.79-billion-row table in ~1.2 s on local NVMe, ~15.5 s over GPFS, peak
memory ~89 MB.

---

## 7. Updates

### 7.1 New BF4 version

Publish a new venv and/or image, add the new modulefile version, smoke-test it
against the current snapshot, then point `latest` at it. Users can pin a
version explicitly with `module load biofilter/<version>`.

Because the bundle is data-only, a BF4 upgrade does **not** require rebuilding
it — unless the release changes the schema. In that case export a new bundle
from a migrated PostgreSQL (§3) rather than trying to migrate the bundle;
there is no write path against Parquet.

### 7.2 New data snapshot

```bash
# 1) Produce a new dated bundle following §3
# 2) Verify it (§3.4) and freeze it (§4)
# 3) Smoke-test with an explicit --db-uri before switching anyone over:
biofilter --db-uri "parquet:///project/hall_shared/biofilter/databases/<new>/bundle/tables" \
  report run --name annotation_master_variant --input rs429358 --output /tmp/check.csv
# 4) Update the modulefile's BIOFILTER_DB_URI to the new snapshot
# 5) Announce the new date to users
```

Old snapshots can stay as long as disk allows — they are reference data for
reproducibility, and users can still target them with `--db-uri`.

---

## 8. Backup

The bundle is plain files on shared storage, so it backs up like any other
research data. The canonical recovery source remains the PostgreSQL dump the
bundle was exported from — keep those alongside the snapshot they produced.

Rebuilding a bundle from a dump is the §3 flow again, so the dump plus this
guide is a complete recovery path.

---

## 9. References

- End-user usage: [lpc\_\_quickstart.md](lpc__quickstart.md)
- Design rationale and benchmarks: [ADR-001](../Andre/ADR/0001-duckdb-parquet-strategy.md)
- Build scripts: [`notebooks/Andre/`](../Andre/)
- HPC image: [docker/hpc/README.md](../../docker/hpc/README.md)
- GHCR publish workflow: [.github/workflows/docker-publish-hpc.yml](../../.github/workflows/docker-publish-hpc.yml)

---

## Appendix A — Legacy PostgreSQL deployment (4.1.x)

> **Deprecated.** Kept for reference and for the one case where it is still
> needed: building a Parquet bundle on the cluster (§3) requires a live
> PostgreSQL to export from. The 4.2.0 HPC image no longer embeds PG — use a
> 4.1.4 image for this. Do not use this flow for regular user-facing
> deployments.

### A.1 Layout

```
/project/${PROJECT}/
├── env/modules/biofilter/<version>/bf4-hpc.sif
└── datasets/bf4/
    ├── <snapshot-date>/pgdata/
    └── dumps/biofilter-<date>.dump
```

Budget ~500 GB for `pgdata/` and ~20 GB for the compressed dump.

### A.2 Restore from a production dump

On the VPS:

```bash
pg_dump -Fc -d biofilter -f /tmp/biofilter-$(date +%Y%m%d).dump
```

Transfer (~20 GB, 1–4 h depending on bandwidth):

```bash
rsync --partial --progress \
  /tmp/biofilter-20260514.dump \
  user@lpc:/project/${PROJECT}/datasets/bf4/dumps/biofilter-20260514.dump
```

Restore inside the legacy PG-bundled container:

```bash
SNAPSHOT_DATE=20260514
VERSION=4.1.4

DB_DIR=/project/${PROJECT}/datasets/bf4/${SNAPSHOT_DATE}
DUMP=/project/${PROJECT}/datasets/bf4/dumps/biofilter-${SNAPSHOT_DATE}.dump
SIF=/project/${PROJECT}/env/modules/biofilter/${VERSION}/bf4-hpc.sif

mkdir -p "${DB_DIR}/pgdata"
TMP_DIR=$(mktemp -d -t bf4-restore-XXXXXX)
mkdir -p "${TMP_DIR}/tmp" "${TMP_DIR}/pg-run"

apptainer run \
  --writable-tmpfs \
  --pwd /tmp \
  --bind "${DB_DIR}/pgdata:/var/lib/postgresql/data" \
  --bind "${DUMP}:/restore.dump:ro" \
  --bind "${TMP_DIR}/tmp:/tmp" \
  --bind "${TMP_DIR}/pg-run:/var/run/postgresql" \
  --env BIOFILTER_RESTORE_DUMP=/restore.dump \
  --env BIOFILTER_RESTORE_JOBS=8 \
  "${SIF}" \
  biofilter db migrate --status

rm -rf "${TMP_DIR}"
```

On first run with an empty `pgdata/` and `BIOFILTER_RESTORE_DUMP` set, the
entrypoint runs `initdb`, creates the application database, then `pg_restore`
with the specified parallelism before starting PG normally. Expect **3–8 hours**
on GPFS-class storage; run it as an LSF job.

### A.3 Fresh empty database

Only for isolated test environments:

```bash
apptainer run --writable-tmpfs --pwd /tmp \
  --bind "${DB_DIR}/pgdata:/var/lib/postgresql/data" \
  --bind "${TMP_DIR}/tmp:/tmp" \
  --bind "${TMP_DIR}/pg-run:/var/run/postgresql" \
  "${SIF}" \
  sh -c 'biofilter db create-db --db-uri "$DATABASE_URL"'

apptainer run --writable-tmpfs --pwd /tmp \
  --bind "${DB_DIR}/pgdata:/var/lib/postgresql/data" \
  --bind "${TMP_DIR}/tmp:/tmp" \
  --bind "${TMP_DIR}/pg-run:/var/run/postgresql" \
  "${SIF}" \
  biofilter db migrate --stamp-head
```

### A.4 Why it was retired

Beyond the cost of a ~480 GB per-snapshot `pgdata/`, the blocking issue was
concurrency: PostgreSQL allows a single postmaster per data directory, which
under Apptainer meant either serialized access or a per-user copy of the whole
database. The `PGDATA` mode `0700` requirement also conflicted with GPFS
`chown` semantics under fakeroot.

A SQLite migration was attempted first (2026-06-17 → 06-22) and abandoned: after
~70 h of cumulative LSF runtime the import never finished
`variant_molecular_effects` and never reached the smaller tables. That failure
is what motivated ADR-001 — see §6 of the ADR for the full history.
