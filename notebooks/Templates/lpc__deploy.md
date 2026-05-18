# Deploying Biofilter 4 on the Penn LPC

Operational guide for the **maintainer** who installs or updates BF4 on the
LPC cluster: pulling the Apptainer image, restoring the database from a
production dump, and managing future updates.

> **Audience:** the person who owns the BF4 environment on the cluster.
> If you just want to _run reports_, see [lpc\_\_quickstart.md](lpc__quickstart.md)
> instead.

> **Before you start:** every script in this guide expects `PROJECT` to be set
> to your LPC project allocation name (the folder under `/project/` you have
> write access to). Export it once per shell session:
>
> ```bash
> export PROJECT=your-project-name
> ```

---

## 1. Directory layout

```
/project/${PROJECT}/
├── env/modules/biofilter/
│   ├── <version>/
│   │   └── bf4-hpc.sif                  ← the Apptainer image
│   └── latest -> <version>              ← optional symlink
└── datasets/bf4/
    ├── <snapshot-date>/
    │   └── pgdata/                      ← the PostgreSQL data directory
    └── dumps/
        └── biofilter-<date>.dump        ← raw pg_dump archives (kept after restore)
```

Versioning the image and dating the dataset folders lets you keep multiple
generations side by side — useful for reproducibility and for testing a new
release without breaking existing users.

---

## 2. Prerequisites

- LPC account with write access to `/project/${PROJECT}/env/modules/` and `/project/${PROJECT}/datasets/bf4/`
- `apptainer` module available on the cluster (`module load apptainer`)
- Enough free space:
  - **~2 GB** for each image version
  - **~500 GB** for the uncompressed `pgdata/` of each snapshot (current production size)
  - **~20 GB** for the compressed dump archive (kept for re-imports)
- GitHub access to `ghcr.io/ritchielab/biofilter-hpc`
  - If the package is **public**: no auth needed
  - If still **private**: a GitHub PAT with `read:packages` scope

---

## 3. Pulling the image (first time and updates)

### 3.1 Public image

```bash
module load apptainer

VERSION=4.1.2
mkdir -p /project/${PROJECT}/env/modules/biofilter/${VERSION}
cd /project/${PROJECT}/env/modules/biofilter/${VERSION}

apptainer pull bf4-hpc.sif docker://ghcr.io/ritchielab/biofilter-hpc:${VERSION}
```

For the very first install, you may also want a `latest` symlink so users can
omit the version:

```bash
cd /project/${PROJECT}/env/modules/biofilter
ln -sfn ${VERSION} latest
```

Verify:

```bash
apptainer inspect /project/${PROJECT}/env/modules/biofilter/${VERSION}/bf4-hpc.sif
```

### 3.2 Private image (PAT auth)

If the GHCR package is still private:

```bash
# 1) On a machine where you have your PAT, log in to GHCR for Apptainer
echo "<your_PAT>" | apptainer remote login \
    --username <your_github_user> \
    --password-stdin docker://ghcr.io

# 2) Pull as in 3.1
```

The login persists in `~/.apptainer/docker-config.json` until you `apptainer
remote logout`.

---

## 4. Database setup

Two paths: restore from a VPS production dump (typical) or initialize a fresh
empty database (rarely needed on the cluster).

### 4.1 Restore from a production dump (typical)

**On the VPS** (or wherever the live BF4 PostgreSQL runs):

```bash
pg_dump -Fc -d biofilter -f /tmp/biofilter-$(date +%Y%m%d).dump
```

Estimates for the current production DB:

- Source size: ~480 GB
- Compressed dump (`-Fc`): ~20 GB

**Transfer to the LPC:**

```bash
rsync --partial --progress \
  /tmp/biofilter-20260514.dump \
  user@lpc:/project/${PROJECT}/datasets/bf4/dumps/biofilter-20260514.dump
```

Bandwidth permitting, 20 GB takes 1–4 hours.

**Restore inside the BF4-HPC container:**

```bash
SNAPSHOT_DATE=20260514
VERSION=4.1.2

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
container's entrypoint runs `initdb`, creates the application database, then
`pg_restore` with the specified parallelism, _before_ starting PG normally.
The final `biofilter db migrate --status` is just a sanity check that
PostgreSQL is up and the schema is reachable.

Expected runtime for the production-sized dump on Lustre/GPFS-class storage:
**3–8 hours**, dominated by `pg_restore`. Recommend running inside `screen`,
`tmux`, or as a SLURM job:

```bash
#!/bin/bash
#SBATCH --job-name=bf4-restore
#SBATCH --output=bf4-restore-%j.log
#SBATCH --time=12:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=8

module load apptainer
# ... paste the apptainer block above ...
```

### 4.2 Initialize a fresh empty database (no dump)

Only needed if there's no production dump to start from — e.g., setting up an
isolated test environment.

```bash
SNAPSHOT_DATE=test
VERSION=4.1.2

DB_DIR=/project/${PROJECT}/datasets/bf4/${SNAPSHOT_DATE}
SIF=/project/${PROJECT}/env/modules/biofilter/${VERSION}/bf4-hpc.sif
mkdir -p "${DB_DIR}/pgdata"

TMP_DIR=$(mktemp -d) && mkdir -p "${TMP_DIR}/tmp" "${TMP_DIR}/pg-run"

# Step 1 — create the application DB, tables, and seeds
apptainer run --writable-tmpfs --pwd /tmp \
  --bind "${DB_DIR}/pgdata:/var/lib/postgresql/data" \
  --bind "${TMP_DIR}/tmp:/tmp" \
  --bind "${TMP_DIR}/pg-run:/var/run/postgresql" \
  "${SIF}" \
  sh -c 'biofilter db create-db --db-uri "$DATABASE_URL"'

# Step 2 — stamp Alembic to head
apptainer run --writable-tmpfs --pwd /tmp \
  --bind "${DB_DIR}/pgdata:/var/lib/postgresql/data" \
  --bind "${TMP_DIR}/tmp:/tmp" \
  --bind "${TMP_DIR}/pg-run:/var/run/postgresql" \
  "${SIF}" \
  biofilter db migrate --stamp-head

rm -rf "${TMP_DIR}"
```

Result: a fresh DB with the BF4 schema and master seeds, but no ETL data.
To populate it, the ETL flow (`biofilter etl update-all`) has to run from a
container that can reach the source data files — usually not what you want on
the cluster.

---

## 5. Smoke test after install

Run a tiny report against the new install. If this succeeds, the
deployment is good.

```bash
SNAPSHOT_DATE=20260514
VERSION=4.1.2
OUT=$(mktemp -d)

TMP_DIR=$(mktemp -d) && mkdir -p "${TMP_DIR}/tmp" "${TMP_DIR}/pg-run"

apptainer run --writable-tmpfs --pwd /tmp \
  --bind "/project/${PROJECT}/datasets/bf4/${SNAPSHOT_DATE}/pgdata:/var/lib/postgresql/data" \
  --bind "${TMP_DIR}/tmp:/tmp" \
  --bind "${TMP_DIR}/pg-run:/var/run/postgresql" \
  --bind "${OUT}:/workspace" \
  "/project/${PROJECT}/env/modules/biofilter/${VERSION}/bf4-hpc.sif" \
  biofilter report run \
    --name annotation_master_gene \
    --input APOE \
    --output /workspace/smoke.csv

head -3 "${OUT}/smoke.csv"
rm -rf "${TMP_DIR}" "${OUT}"
```

Expected: 3 lines of CSV with the APOE annotation.

---

## 6. Updates

### 6.1 New BF4 version

```bash
module load apptainer
NEW_VERSION=4.1.3

mkdir -p /project/${PROJECT}/env/modules/biofilter/${NEW_VERSION}
cd /project/${PROJECT}/env/modules/biofilter/${NEW_VERSION}
apptainer pull bf4-hpc.sif docker://ghcr.io/ritchielab/biofilter-hpc:${NEW_VERSION}

# Smoke-test the new image against the current snapshot before flipping latest
# ... (see section 5) ...

# Once green, flip the symlink so default invocations pick up the new version
cd /project/${PROJECT}/env/modules/biofilter
ln -sfn ${NEW_VERSION} latest
```

If a new BF4 release includes schema migrations, after the symlink flip:

```bash
TMP_DIR=$(mktemp -d) && mkdir -p "${TMP_DIR}/tmp" "${TMP_DIR}/pg-run"
apptainer run --writable-tmpfs --pwd /tmp \
  --bind "/project/${PROJECT}/datasets/bf4/${SNAPSHOT_DATE}/pgdata:/var/lib/postgresql/data" \
  --bind "${TMP_DIR}/tmp:/tmp" \
  --bind "${TMP_DIR}/pg-run:/var/run/postgresql" \
  "/project/${PROJECT}/env/modules/biofilter/${NEW_VERSION}/bf4-hpc.sif" \
  biofilter db migrate --target head
rm -rf "${TMP_DIR}"
```

### 6.2 New database snapshot

Treat each snapshot as an immutable dated folder. Don't `pg_restore` over an
existing one.

```bash
# 1) Take a fresh dump on the VPS, transfer it (see 4.1)
# 2) Create a new dated folder and restore into it
NEW_SNAPSHOT=20260901
mkdir -p /project/${PROJECT}/datasets/bf4/${NEW_SNAPSHOT}/pgdata

# 3) Run the restore command from section 4.1 with SNAPSHOT_DATE=${NEW_SNAPSHOT}

# 4) Smoke-test (section 5) against the new snapshot
# 5) Announce the new date to users (update lpc__quickstart.md if needed)
```

Old snapshots can stay around as long as disk allows — they're reference data
for reproducibility.

---

## 7. Backup

The bind-mounted `pgdata/` is the only stateful asset. Back it up like
any research data.

**Cold backup** (when nobody is using that snapshot):

```bash
tar czf /project/${PROJECT}/datasets/bf4/backups/pgdata-${SNAPSHOT_DATE}-$(date +%Y%m%d).tar.gz \
    -C /project/${PROJECT}/datasets/bf4/${SNAPSHOT_DATE} pgdata
```

**Logical backup** (while a container is running):

```bash
# inside any container pointed at this pgdata
pg_dump -U biofilter -Fc biofilter > /workspace/backup-$(date +%Y%m%d).dump
```

The dumps in `datasets/bf4/dumps/` already function as the canonical backup
for the corresponding snapshot — keep them.

---

---

## 9. References

- End-user usage: [lpc\_\_quickstart.md](lpc__quickstart.md)
- Image source: [docker/hpc/](../../docker/hpc/) in the repo
- GHCR publish workflow: [.github/workflows/docker-publish-hpc.yml](../../.github/workflows/docker-publish-hpc.yml)
