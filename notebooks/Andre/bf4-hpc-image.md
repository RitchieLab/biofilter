# BF4 HPC Image — Implementation Notes

> Personal notes documenting the work that produced the `docker/hpc/` image and
> the supporting GitHub Actions workflow. Captures the _why_, the layout of the
> new files, the first-run workflow we validated locally, the bugs we ran into
> during testing, and what still has to be done on the cluster side.
>
> Date: 2026-05-13
> Branch: `biofilter3r`

---

## 1. Goal

Run BF4 on the Penn LPC (HPC cluster) given the constraints:

- LPC does **not** offer PostgreSQL as a service
- LPC offers **Apptainer/Singularity** (not Docker) and Lmod
- Shared filesystem is Lustre/GPFS-class; NFS is a no-go for PG
- Current production DB on the VPS is ~600 GB

After ruling out SQLite (file-locking on shared FS, performance at this scale)
and a pure Parquet pivot (too invasive for a short timeline), we landed on a
**self-contained container image** that ships BF4 + PostgreSQL together, with
`PGDATA` bind-mounted to a host directory on the cluster's project storage.

The same image runs in Docker locally and converts cleanly to a `.sif` for
Apptainer on the cluster.

---

## 2. What was added to the project

```
docker/hpc/                                 # NEW — self-contained image
├── Dockerfile                              # postgres:16-bookworm + Python venv + BF4
├── entrypoint.sh                           # initdb / optional restore / start PG / exec biofilter
├── .env.example                            # documented env vars
└── README.md                               # user-facing docs (build, run, Apptainer, Lmod)

.github/workflows/
└── docker-publish-hpc.yml                  # NEW — publishes to ghcr.io/ritchielab/biofilter-hpc

docker/README.md                            # MODIFIED — link to hpc/ image
```

The existing app-only image at `docker/Dockerfile` (which targets an external
PostgreSQL) was left untouched.

---

## 3. Architecture summary

- **Base image:** `postgres:16-bookworm` (official PostgreSQL Docker image).
  Inherits a working PG cluster setup, `gosu`, and proper user/group handling.
- **BF4 install:** isolated Python venv at `/opt/biofilter/venv`, installed
  from the in-tree source via `pip install .`. No PyPI publish needed.
- **PG configuration:** listens **only on `localhost`** inside the container,
  trust auth (safe because PG is not exposed outside the container).
- **PGDATA:** bind-mounted to a host directory. This is the only stateful piece
  of the deployment — back it up like project data.
- **Single-container model:** PG and BF4 share one container, orchestrated by a
  shell entrypoint. Simpler than docker-compose, plays well with Apptainer
  (one `.sif` per job).
- **Auth model:** the entrypoint creates a single role (`biofilter`) at
  `initdb` time; `DATABASE_URL` is built and exported automatically, so BF4
  just connects to a working DB without seeing any secrets.

### Entrypoint flow

1. If running as root (Docker default), `chown` PGDATA to `postgres` and
   re-exec as that user via `gosu`. This branch is skipped under Apptainer
   (which runs as the invoking host user).
2. Detect first run by checking for `$PGDATA/PG_VERSION`.
3. **First run only:**
   - Run `initdb --username="$POSTGRES_USER" --auth-local=trust --auth-host=trust`
   - Write a localhost-only `pg_hba.conf`
   - Set `listen_addresses = 'localhost'` in `postgresql.conf`
   - If `BIOFILTER_RESTORE_DUMP` is set: boot PG, `CREATE DATABASE`, `pg_restore`, stop PG.
   - Otherwise: leave the application DB to be created by `biofilter db create-db`
4. Install a shutdown trap that calls `pg_ctl stop -m fast` on `EXIT/INT/TERM`.
5. Start PG, wait for `pg_isready`.
6. Export `DATABASE_URL` so any BF4 invocation finds the DB automatically.
7. (Optional) Run `biofilter db migrate --target head` if `BIOFILTER_AUTO_MIGRATE=1`.
8. Execute the user command (defaults to `biofilter --help`).

---

## 4. Environment variables

| Variable                 | Default                    | Purpose                                                                      |
| ------------------------ | -------------------------- | ---------------------------------------------------------------------------- |
| `POSTGRES_USER`          | `biofilter`                | DB superuser created at initdb                                               |
| `POSTGRES_DB`            | `biofilter`                | Application database name                                                    |
| `POSTGRES_PORT`          | `5432`                     | PG port (inside container only)                                              |
| `PGDATA`                 | `/var/lib/postgresql/data` | Bind-mount target                                                            |
| `BIOFILTER_AUTO_MIGRATE` | `0`                        | If `1`, run `biofilter db migrate --target head` at startup                  |
| `BIOFILTER_RESTORE_DUMP` | _unset_                    | Path inside container to a `pg_dump -Fc` archive. Restored on first run only |
| `BIOFILTER_RESTORE_JOBS` | `4`                        | Parallelism for `pg_restore`                                                 |

---

## 5. First-run workflows

### 5.1 Empty database (typical first deployment)

```bash
mkdir -p ~/works/temp/bf4-hpc/pgdata
cd ~/works/temp/bf4-hpc

# 1. Create the application DB + tables + seeds (BF4 does all of this)
docker run --rm \
  -v "$(pwd)/pgdata:/var/lib/postgresql/data" \
  biofilter-hpc:latest \
  sh -c 'biofilter db create-db --db-uri "$DATABASE_URL"'

# 2. Tell Alembic the schema is at head
docker run --rm \
  -v "$(pwd)/pgdata:/var/lib/postgresql/data" \
  biofilter-hpc:latest \
  biofilter db migrate --stamp-head

# 3. Confirm
docker run --rm \
  -v "$(pwd)/pgdata:/var/lib/postgresql/data" \
  biofilter-hpc:latest \
  biofilter db migrate --status
```

The `sh -c '... "$DATABASE_URL"'` wrapping in step 1 is required because
`create-db` mandates an explicit `--db-uri` and the variable is only set
inside the container by the entrypoint. After step 1, BF4 has created the
DB itself, populated all tables via the ORM, and seeded the master data
(`_seed_all` is called inside `create_db`). After step 2, Alembic is in sync.

### 5.2 Restore from an existing dump (VPS → cluster)

```bash
# On the source host (VPS)
pg_dump -Fc -d biofilter -f biofilter.dump
rsync --partial --progress biofilter.dump user@cluster:/path/to/biofilter.dump

# On the target host (cluster) — first run only
docker run --rm \
  -v "$(pwd)/pgdata:/var/lib/postgresql/data" \
  -v "$(pwd)/biofilter.dump:/restore.dump:ro" \
  -e BIOFILTER_RESTORE_DUMP=/restore.dump \
  -e BIOFILTER_RESTORE_JOBS=4 \
  biofilter-hpc:latest \
  biofilter db migrate --status
```

The entrypoint creates an empty `biofilter` DB and runs `pg_restore` against
it inside the same first-run block. Subsequent invocations skip restore even
if the variable is still set.

For the 600 GB VPS DB, expect:

- Dump size compressed (`-Fc`): ~100–250 GB depending on content
- Transfer time: 6–24 h depending on bandwidth between sites
- Restore time with `-j 4`: 2–6 h

### 5.3 Day-to-day use after bootstrap

```bash
docker run --rm \
  -v "$(pwd)/pgdata:/var/lib/postgresql/data" \
  biofilter-hpc:latest \
  biofilter report list

docker run --rm \
  -v "$(pwd)/pgdata:/var/lib/postgresql/data" \
  -v "$(pwd):/workspace" \
  biofilter-hpc:latest \
  biofilter report run --report-name <name> --input-file /workspace/in.txt --output /workspace/out.csv
```

---

## 6. Local test sequence (validated 2026-05-13)

Exact commands executed during validation, in order:

```bash
# Build
cd /Users/andrerico/Works/Sys/biofilter
docker build -t biofilter-hpc:latest -f docker/hpc/Dockerfile .

# Clean test area
mkdir -p ~/works/temp/bf4-hpc/pgdata
cd ~/works/temp/bf4-hpc

# 1. Bootstrap (create DB + tables + seeds)
docker run --rm \
  -v "$(pwd)/pgdata:/var/lib/postgresql/data" \
  biofilter-hpc:latest \
  sh -c 'biofilter db create-db --db-uri "$DATABASE_URL"'
# Expected: 🆕 Created PostgreSQL database 'biofilter'
#           📦 Bootstrapping models...
#           🏗️ Creating tables...
#           🌱 Seeding initial data...
#           ✅ Database created at postgresql+psycopg2://biofilter@localhost:5432/biofilter

# 2. Stamp Alembic to head
docker run --rm \
  -v "$(pwd)/pgdata:/var/lib/postgresql/data" \
  biofilter-hpc:latest \
  biofilter db migrate --stamp-head
# Expected: 🏷️  Stamping DB to head revision: <hash>
#           ✅ Stamp completed.

# 3. Confirm
docker run --rm \
  -v "$(pwd)/pgdata:/var/lib/postgresql/data" \
  biofilter-hpc:latest \
  biofilter db migrate --status
# Expected: DB revision: <hash> / Versioned DB?: True / ✅ Up-to-date.
```

Final image size: **1.44 GB** (postgres:16-bookworm ~430 MB + Python venv + BF4 deps).

---

## 7. Issues encountered during testing — and the fixes

These are documented because the same traps will catch anyone building a
similar image.

### 7.1 `initdb: command not found`

**Symptom:** entrypoint failed at the very first `initdb` call.

**Root cause:** the `ENV PATH=...` in my Dockerfile fully overrode the base
image's PATH, removing `/usr/lib/postgresql/16/bin/` (where `initdb`,
`pg_ctl`, `psql` live in Debian's PG layout).

**Fix:** include the PG bin directory explicitly in the Dockerfile's PATH:

```dockerfile
PATH=/opt/biofilter/venv/bin:/usr/lib/postgresql/16/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
```

### 7.2 `pg_ctl: another server might be running` + `automatic recovery in progress`

**Symptom:** every run after the first one logged a noisy recovery sequence.

**Root cause:** the entrypoint used `exec "$@"` at the end, which replaces
the shell process with the user command. The `trap shutdown_pg EXIT` therefore
**never fired** when the user command finished — PG was killed abruptly when
the container exited, leaving a stale `postmaster.pid` and an unrecovered WAL.

**Fix:** drop the `exec`, run the command in the same shell, propagate its
exit code explicitly so the EXIT trap stops PG cleanly:

```bash
set +e
"$@"
cmd_exit=$?
set -e
exit "$cmd_exit"
```

### 7.3 `FATAL: role "postgres" does not exist`

**Symptom:** PG server logs printed a FATAL line on every startup.

**Root cause:** `pg_isready` without `-U` defaulted to the OS user — `postgres`
inside the container after `gosu`. Since we only create the `biofilter` role
(via `initdb --username`), the connection probe got rejected. Cosmetic, but
alarming.

**Fix:** pass explicit user and a database that always exists:

```bash
pg_isready -h localhost -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d postgres -q
```

### 7.4 `create-db` silently skipped (the big one)

**Symptom:** after `create-db` reported success, subsequent commands hit
`relation "system_config" does not exist` / `relation "biofilter_metadata"
does not exist`.

**Root cause:** the entrypoint was pre-creating the `biofilter` database
during `initdb` (via `psql -c "CREATE DATABASE biofilter"`). Then `create-db`
runs `exists_db()` → returns True → **short-circuits without creating any
tables** ([create_db_mixin.py:117-120](../../biofilter/modules/db/create_db_mixin.py#L117-L120)):

```python
if self.exists_db(new_db=True) and not overwrite:
    msn = f"Database already exists at {self.db_uri}"
    self.logger.log(msn, "WARNING")
    return False  # ← no tables created
```

The warning `Database already exists at ...` in the output was the only hint.

**Fix:** the entrypoint should own the PG **cluster** but not the application
DB. Removed the `CREATE DATABASE` from the standard first-run path; left it
only inside the `BIOFILTER_RESTORE_DUMP` branch (where it's needed to give
`pg_restore` a target). The empty-DB path now relies on
`biofilter db create-db` to create both the DB and the schema.

### 7.5 BF4 quirk: `--force` is not the same as `create-db`

**Observation, not a container bug.** Running `biofilter db migrate --target
head --force` on an empty PG cluster lets Alembic upgrade run (no exception),
but BF4 then calls `_mirror_revision_to_metadata` ([migrate.py:211](../../biofilter/modules/db/migrate.py#L211))
which queries `biofilter_metadata`. On a fresh DB the table was never created
because BF4 builds the schema via the ORM (`Base.metadata.create_all`) inside
`create-db`, not through Alembic migrations.

**Workflow lesson:** on a fresh PG cluster the entry point is **always**
`db create-db` first, then `db migrate --stamp-head`. Never `migrate --force`.

---

## 8. Publishing to GHCR

The workflow at `.github/workflows/docker-publish-hpc.yml` publishes the HPC
image to `ghcr.io/<owner>/biofilter-hpc`. Triggers:

- Push a Git tag matching `v*` (e.g., `v4.1.3`) — publishes that version tag plus `:latest`
- Manual dispatch via GitHub Actions UI ("Publish HPC Docker Image" → "Run workflow")

The workflow uses the auto-provided `GITHUB_TOKEN` — no additional secrets
required. The owner segment is lowercased automatically so `RitchieLab` ends
up as `ghcr.io/ritchielab/...`.

**Public visibility gotcha:** the `RitchieLab` org currently disables public
package creation. After the first publish, the package shows up as private
and the LPC will need authenticated pulls (or org-admin action to flip the
"allow public packages" setting). Pending coordination with the org admin.

Once published, anyone on the cluster can do:

```bash
apptainer pull bf4-hpc.sif docker://ghcr.io/ritchielab/biofilter-hpc:latest
```

---

## 9. Deployment paths on the LPC

### 9.1 Convert Docker image → Apptainer `.sif`

Done on a machine that has both Docker and Apptainer (e.g., a build server,
not the HPC itself):

```bash
apptainer build bf4-hpc.sif docker-daemon://biofilter-hpc:latest
```

Or pull directly from GHCR on the cluster (no Docker required):

```bash
apptainer pull bf4-hpc.sif docker://ghcr.io/ritchielab/biofilter-hpc:latest
```

### 9.2 Run on the cluster

```bash
apptainer run \
  --bind /project/<your>/bf4-pgdata:/var/lib/postgresql/data \
  bf4-hpc.sif \
  biofilter report list
```

Apptainer runs the container as the invoking host user. The bind-mounted
PGDATA directory must therefore be writable by that user (not by uid 999 as
in Docker).

### 9.3 Lmod module wrapper

Final UX target: users do `module load biofilter4` and run `biofilter` as
they're used to. Suggested module file (Lua / Lmod):

```lua
help([[Biofilter 4 — HPC image (BF4 + PostgreSQL).]])

local sif  = "/opt/biofilter/bf4-hpc-4.1.2.sif"
local data = os.getenv("BIOFILTER_PGDATA") or pathJoin(os.getenv("HOME"), "bf4-pgdata")

set_alias("biofilter", "apptainer run --bind " .. data .. ":/var/lib/postgresql/data " .. sif .. " biofilter")
setenv("BIOFILTER_SIF", sif)
```

End-user workflow then becomes:

```bash
module load biofilter4
biofilter db create-db --db-uri "postgresql+psycopg2://biofilter@localhost:5432/biofilter"
biofilter db migrate --stamp-head
biofilter report list
```

---

## 10. Open items / next steps

- [ ] Confirm with the LPC admin: `.sif` size limits, availability of a
      persistent / service node for long-lived PG, exact shared FS type
- [ ] Coordinate with org admin to enable public package creation for the
      `RitchieLab` GHCR (or get admin access). Currently the published image
      is private, which blocks unauthenticated `apptainer pull` on the LPC.
- [ ] Build a `.sif` from the published GHCR image and smoke-test it on the
      cluster
- [ ] Decide whether to add a `BIOFILTER_AUTO_BOOTSTRAP=1` env var that
      runs `create-db` + `migrate --stamp-head` automatically on the very
      first run (currently the user has to run two commands)
- [ ] Pilot the dump-restore path end-to-end with a small subset of the
      production DB before doing the full 600 GB transfer
- [ ] Coordinate with LPC IT on a module file (`biofilter4/<version>.lua`)
      that wraps the Apptainer call
- [ ] Future phase (out of scope here): re-evaluate the Parquet-based model
      (IGEM pattern) as a long-term alternative — would eliminate PG on the
      cluster entirely

---

## 11. Related files in the project

- [docker/hpc/Dockerfile](../../docker/hpc/Dockerfile) — image definition
- [docker/hpc/entrypoint.sh](../../docker/hpc/entrypoint.sh) — first-run logic, PG lifecycle, restore hook
- [docker/hpc/README.md](../../docker/hpc/README.md) — user-facing documentation
- [docker/hpc/.env.example](../../docker/hpc/.env.example) — annotated env vars
- [.github/workflows/docker-publish-hpc.yml](../../.github/workflows/docker-publish-hpc.yml) — GHCR publishing workflow
- [biofilter/modules/db/create_db_mixin.py](../../biofilter/modules/db/create_db_mixin.py) — `create_db` (creates DB + tables + seeds)
- [biofilter/modules/db/migrate.py](../../biofilter/modules/db/migrate.py) — Alembic wrapper used by `db migrate`
- [biofilter/api/cli/groups/db.py](../../biofilter/api/cli/groups/db.py) — CLI surface for `db create-db`, `db migrate`, `db upgrade`

#

module load apptainer

# Variáveis

DB_DIR=/project/ritchie/datasets/bf4/20260514
SIF=/project/ritchie/env/modules/biofilter/4.1.2/bf4-hpc.sif

# /tmp scratch local pro PG socket

TMP_DIR=$(mktemp -d -t bf4-test-XXXXXX)
mkdir -p "${TMP_DIR}/tmp" "${TMP_DIR}/pg-run"

# Teste 1: status do Alembic (mesma coisa do job, confirma persistência)

apptainer run \
 --writable-tmpfs \
 --bind "${DB_DIR}/pgdata:/var/lib/postgresql/data" \
  --bind "${TMP_DIR}/tmp:/tmp" \
 --bind "${TMP_DIR}/pg-run:/var/run/postgresql" \
  "${SIF}" \
 biofilter db migrate --status

# Teste 2: listar reports (confirma que BF4 lê catálogo de funcionalidades)

apptainer run \
 --writable-tmpfs \
 --bind "${DB_DIR}/pgdata:/var/lib/postgresql/data" \
  --bind "${TMP_DIR}/tmp:/tmp" \
 --bind "${TMP_DIR}/pg-run:/var/run/postgresql" \
  "${SIF}" \
 biofilter report list

# Limpa

rm -rf "${TMP_DIR}"
