# Docker (BF4 — HPC read-only image)

Lightweight container that ships Biofilter 4 configured to read a
**Parquet bundle** directly via DuckDB. No PostgreSQL bundled, no
import phase, no per-user data copy — multiple processes can read the
same bundle on shared storage concurrently.

Use this image on HPC clusters (LPC, etc.) and any other environment
where the data ships as a Parquet snapshot. For deployments that need
to connect to an external PostgreSQL (VPS, ETL), use the app-only image
at [../Dockerfile](../Dockerfile).

## How it works

- Base: `python:3.12-slim` (~150 MB)
- BF4 installed in an isolated Python venv at `/opt/biofilter/venv`
- DuckDB and `duckdb-engine` pulled in as BF4 dependencies
- Entrypoint maps `DATABASE_URL` / `BIOFILTER_DB_URI` into a `--db-uri`
  argument, so the bundle path is passed as plain env

The bundle is **not** baked into the image. You bind-mount it at run
time so the same image serves any snapshot. Image size: ~400-500 MB
total (down from ~1.5 GB for the legacy PG-bundled variant).

## Quick start — pull a published image

The image is published to GitHub Container Registry. On the LPC (or any
machine with Apptainer/Singularity):

```bash
apptainer pull bf4-hpc.sif docker://ghcr.io/ritchielab/biofilter-hpc:latest

mkdir -p ~/bf4_output

apptainer run \
  --bind /project/hall_shared/biofilter/databases/20260514/bundle/tables:/bundle:ro \
  --bind ~/bf4_output:/workspace \
  --env BIOFILTER_DB_URI=parquet:///bundle \
  bf4-hpc.sif \
  biofilter report run \
    --name annotation_master_gene \
    --input APOE \
    --output /workspace/apoe.csv
```

For Docker locally:

```bash
docker pull ghcr.io/ritchielab/biofilter-hpc:latest

docker run --rm \
  -v "$(pwd)/bundle:/bundle:ro" \
  -v "$(pwd)/out:/workspace" \
  -e BIOFILTER_DB_URI=parquet:///bundle \
  ghcr.io/ritchielab/biofilter-hpc:latest \
  biofilter report run --name annotation_master_gene --input APOE --output /workspace/apoe.csv
```

## Build from source (development)

From the project root:

```bash
docker build -t biofilter-hpc:latest -f docker/hpc/Dockerfile .
```

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `BIOFILTER_DB_URI` | _unset_ | URI passed to `--db-uri`. Typical value: `parquet:///path/to/bundle/tables` |
| `DATABASE_URL` | _unset_ | Mirrored to `BIOFILTER_DB_URI` for backwards-compat |

Pass them with `-e VAR=value` (Docker) or `--env VAR=value` (Apptainer).

## Common runs

Pull the image once, then:

```bash
# Reusable handle (assumes the bundle is bind-mounted at /bundle)
alias bf4hpc='docker run --rm \
  -v "$BUNDLE_DIR:/bundle:ro" \
  -v "$PWD:/workspace" \
  -e BIOFILTER_DB_URI=parquet:///bundle \
  ghcr.io/ritchielab/biofilter-hpc:latest'

# List reports
bf4hpc biofilter report list

# Annotate genes
bf4hpc biofilter report run \
  --name annotation_master_gene \
  --input "TP53,BRCA1,APOE" \
  --output /workspace/genes.csv

# Annotate variants from file
bf4hpc biofilter report run \
  --name annotation_master_variant \
  --input-file /workspace/rsids.txt \
  --output /workspace/variants.csv
```

## What changed from the PG-bundled image (4.1.x)

- **PostgreSQL is gone.** The image previously embedded PG 16 with a
  bind-mounted PGDATA at `/var/lib/postgresql/data`. With Parquet
  reads, no DB server is needed.
- **No more `PGDATA` permissions dance.** The old image required mode
  `0700` and tripped on the GPFS `chown` semantics under Apptainer
  fakeroot. Parquet bind-mounts are just regular read-only files.
- **Multi-user concurrent access** is now native. The PG single-
  postmaster constraint that forced per-user data copies is gone.
- **No first-run init.** The old image ran `initdb` on first launch
  and (optionally) `pg_restore` from a bind-mounted dump. The new
  image just reads the Parquet files in place.
- **Image size**: ~400 MB instead of ~1.5 GB.

The legacy PG-bundled image is preserved in git history at the tag
`v4.1.4` if needed for ETL-on-cluster experiments. For production HPC
deployments, this Parquet-backed image is the supported path going
forward.

## HPC deployment

See [../../notebooks/Templates/lpc__quickstart.md](../../notebooks/Templates/lpc__quickstart.md)
for the LPC-specific quick-start (often you don't need the container at
all — just `pip install biofilter` in a venv on the cluster and point at
the Parquet bundle).

For Apptainer/Singularity packaging and module-wrapping, see
[../../notebooks/Templates/lpc__deploy.md](../../notebooks/Templates/lpc__deploy.md).

## Limits and caveats

- **Read-only.** All writes against `parquet://` views fail at the
  DuckDB level. Do not point this image at a write workload (ETL,
  `db create-db`, `db import`) — use the app-only image at
  `../Dockerfile` with a real PostgreSQL.
- **Schema-pinned to the bundle.** When BF4 ORM evolves, the Parquet
  bundle has to be regenerated to match. Versions are tracked in the
  bundle's `manifest.json`.
- **Bundle path semantics.** `parquet:///foo/bar/tables` resolves to
  the absolute path `/foo/bar/tables` inside the container. Make sure
  the bind-mount target matches the URI.
