# Docker (BF4 + PostgreSQL — HPC image)

Self-contained image that ships Biofilter 4 together with a PostgreSQL 16
server inside the same container. The database files live in a host directory
bind-mounted at `/var/lib/postgresql/data`, so the data survives container
removal and can be moved between hosts as a plain folder.

Use this image when an external PostgreSQL is not available — HPC clusters
(LPC, etc.), single-node deployments, sandbox/test environments. For
deployments that connect to an existing PostgreSQL, use the app-only image
at [../Dockerfile](../Dockerfile).

## How it works

- Base: `postgres:16-bookworm` (official PostgreSQL image)
- BF4 installed in an isolated Python venv at `/opt/biofilter/venv`
- PostgreSQL listens **only on localhost** inside the container (trust auth, no password)
- `DATABASE_URL` is assembled by the entrypoint and exported automatically — BF4 just sees a working database
- First run detects an empty `PGDATA` and runs `initdb` + creates the application database; subsequent runs only start the server
- Optional first-run restore from a `pg_dump -Fc` archive (see below)

## Quick start — pull a published image

The image is published to GitHub Container Registry. On the LPC (or any
machine with Apptainer/Singularity), pull the `.sif` and run:

```bash
apptainer pull bf4-hpc.sif docker://ghcr.io/ritchielab/biofilter-hpc:latest

mkdir -p ~/bf4-pgdata
apptainer run \
  --bind ~/bf4-pgdata:/var/lib/postgresql/data \
  bf4-hpc.sif \
  biofilter db migrate --target head
```

For Docker locally:

```bash
docker pull ghcr.io/ritchielab/biofilter-hpc:latest

mkdir -p ./bf4-pgdata
docker run --rm \
  -v "$(pwd)/bf4-pgdata:/var/lib/postgresql/data" \
  ghcr.io/ritchielab/biofilter-hpc:latest \
  biofilter db migrate --target head
```

## Build from source (development)

From the project root:

```bash
docker build -t biofilter-hpc:latest -f docker/hpc/Dockerfile .
```

Build directly from a Git ref (no local clone needed):

```bash
docker build -t biofilter-hpc:bf4 -f docker/hpc/Dockerfile \
  "https://github.com/RitchieLab/biofilter.git#biofilter3r"
```

## Initialize from an existing dump

Useful for migrating a populated database from another host (e.g., VPS → HPC).

On the source host:

```bash
pg_dump -Fc -d biofilter -f biofilter.dump
rsync --partial --progress biofilter.dump user@hpc:/path/to/biofilter.dump
```

On the target host, on the **first run**, mount the dump and set
`BIOFILTER_RESTORE_DUMP`:

```bash
docker run --rm \
  -v "$(pwd)/bf4-pgdata:/var/lib/postgresql/data" \
  -v "$(pwd)/biofilter.dump:/restore.dump:ro" \
  -e BIOFILTER_RESTORE_DUMP=/restore.dump \
  -e BIOFILTER_RESTORE_JOBS=4 \
  biofilter-hpc:latest \
  biofilter db migrate --status
```

The entrypoint runs `pg_restore` with the indicated parallelism, then
proceeds to start PostgreSQL normally. The variable is silently ignored on
subsequent runs (PGDATA is already populated).

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `POSTGRES_USER` | `biofilter` | DB superuser created on first init |
| `POSTGRES_DB` | `biofilter` | Application database created on first init |
| `POSTGRES_PORT` | `5432` | PostgreSQL port inside the container |
| `PGDATA` | `/var/lib/postgresql/data` | Bind-mount target for the database files |
| `BIOFILTER_AUTO_MIGRATE` | `0` | Set to `1` to run `biofilter db migrate --target head` on startup |
| `BIOFILTER_RESTORE_DUMP` | _unset_ | Path inside the container to a `pg_dump -Fc` archive. Restored on first run only |
| `BIOFILTER_RESTORE_JOBS` | `4` | Parallelism for `pg_restore` |

A starter file is provided at [.env.example](.env.example).

## Common runs

Migrate schema to head:

```bash
docker run --rm \
  -v "$(pwd)/bf4-pgdata:/var/lib/postgresql/data" \
  biofilter-hpc:latest \
  biofilter db migrate --target head
```

Run a report against mounted input/output:

```bash
docker run --rm \
  -v "$(pwd)/bf4-pgdata:/var/lib/postgresql/data" \
  -v "$(pwd):/workspace" \
  biofilter-hpc:latest \
  biofilter report run \
    --report-name annotation_master_gene \
    --input-file /workspace/gene.txt \
    --output /workspace/annotation_master_gene.csv
```

Interactive shell with both `biofilter` and `psql` available:

```bash
docker run --rm -it \
  -v "$(pwd)/bf4-pgdata:/var/lib/postgresql/data" \
  --entrypoint /bin/bash \
  biofilter-hpc:latest
```

## Persistence and backup

The host directory bind-mounted at `PGDATA` is the only place data lives.
Treat it like a project asset: back it up the same way you back up source
code or research data.

```bash
# Logical backup against a running container (preferred)
docker exec <container> pg_dump -U biofilter -Fc biofilter > backup.dump

# Cold backup (only when no container is running against this PGDATA)
tar czf bf4-pgdata-$(date +%Y%m%d).tar.gz bf4-pgdata/
```

## Concurrency

PostgreSQL allows **only one** postmaster per `PGDATA`. Do not start a second
container against the same bind-mounted directory while another is running —
the second one will fail (or, worse on broken filesystems, corrupt data).
For multi-user access, run a single long-lived container and connect
additional clients to it over the network.

## HPC deployment via Apptainer/Singularity

The image is built to be Apptainer-compatible. Pull straight from GHCR:

```bash
apptainer pull bf4-hpc.sif docker://ghcr.io/ritchielab/biofilter-hpc:latest

apptainer run \
  --bind /project/<your>/bf4-pgdata:/var/lib/postgresql/data \
  bf4-hpc.sif \
  biofilter report list
```

Apptainer runs the container as the **invoking host user**, not as root. The
bind-mounted `PGDATA` directory must therefore be readable and writable by
that user.

## Packaging as an Lmod module on the LPC

LPC users are accustomed to `module load biofilter`. The `.sif` can be wrapped
transparently so the user experience is identical:

```lua
-- /opt/lmod/modulefiles/biofilter4/4.1.2.lua
help([[Biofilter 4 — HPC image (BF4 + PostgreSQL).]])

local sif  = "/opt/biofilter/bf4-hpc-4.1.2.sif"
local data = os.getenv("BIOFILTER_PGDATA") or pathJoin(os.getenv("HOME"), "bf4-pgdata")

set_alias("biofilter", "apptainer run --bind " .. data .. ":/var/lib/postgresql/data " .. sif .. " biofilter")
setenv("BIOFILTER_SIF", sif)
```

End-user workflow on the cluster:

```bash
module load biofilter4
biofilter db migrate --target head
biofilter report list
```

The `.sif` and the user's `PGDATA` folder are the only artifacts IT needs to
manage on the cluster.

## Limits and caveats

- `BIOFILTER_AUTO_MIGRATE=1` is convenient for dev but risky against a
  populated `PGDATA` created by a newer BF4 version — keep it off in
  production environments and run migrations explicitly.
- Trust auth is safe only because PostgreSQL is bound to localhost inside
  the container. Do not publish port 5432 to the host (`-p 5432:5432`) without
  switching to password or scram auth first.
- File ownership on bind-mounted `PGDATA`: Docker writes as `postgres` user
  (uid/gid 999 in the base image). If you need different ownership for
  Apptainer or non-default deployments, pass `--user` at run time and make
  sure the host directory is writable by that uid.
