# Docker (BF4 only)

This folder contains the Biofilter 4 container image setup (application only, no bundled database).

> Looking for a self-contained image that ships BF4 **together with PostgreSQL**
> (HPC clusters, single-node deployments)? See [hpc/README.md](hpc/README.md).

## Build

From the project root:

```bash
docker build -t biofilter:latest -f docker/Dockerfile .
```

## Run with external DB

Use `DATABASE_URL` to point to any external PostgreSQL or SQLite database URI accepted by SQLAlchemy.

```bash
docker run --rm \
  -e DATABASE_URL="postgresql+psycopg2://user:password@host:5432/biofilter" \
  biofilter:latest report list
```

Or with env file:

```bash
cp docker/.env.example docker/.env
docker run --rm --env-file docker/.env biofilter:latest report list
```

You can also mount your project configuration to keep using `.biofilter.toml`:

```bash
docker run --rm \
  -v "$(pwd):/workspace" \
  biofilter:latest config show
```

## Resolution precedence

Inside the container, DB URI resolution follows:

1. `--db-uri` CLI option
2. `DATABASE_URL` (or `BIOFILTER_DB_URI`)
3. `.biofilter.toml` (`database.db_uri`)

`DATABASE_URL` is automatically mirrored to `BIOFILTER_DB_URI` for Alembic compatibility.

## Publish manually (Docker Hub)

```bash
docker login
docker buildx create --use
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f docker/Dockerfile \
  -t ricoandre/biofilter:4.1.1 \
  -t ricoandre/biofilter:latest \
  --provenance=false \
  --sbom=false \
  --push .
```

## Publish via GitHub Actions (recommended)

Workflow file: `.github/workflows/docker-publish.yml`

Required repository secrets:

- `DOCKERHUB_USERNAME`
- `DOCKERHUB_TOKEN` (Docker Hub access token)

How to trigger:

1. Push a git tag like `v4.1.1` (publishes `4.1.1` and `latest`)
2. Or run manually via `Actions -> Publish Docker Image -> Run workflow`

## Quick Start Workflows

Use this section for the fastest day-to-day flows.

### 1) Build image from a specific Git ref

```bash
docker build -t biofilter:bf4 -f docker/Dockerfile \
  "https://github.com/RitchieLab/biofilter.git#biofilter3r"
```

### 2) Run a single command (non-interactive)

```bash
docker run --rm \
  -e DATABASE_URL="postgresql+psycopg2://user:password@host:5432/biofilter" \
  biofilter:bf4 report list
```

### 3) Run reports with local input/output files

```bash
docker run --rm \
  -e DATABASE_URL="postgresql+psycopg2://user:password@host:5432/biofilter" \
  -v "$(pwd):/workspace" \
  biofilter:bf4 report run \
    --report-name annotation_master_gene \
    --input-file /workspace/gene.txt \
    --param include_relationships=true \
    --param include_variant_summary=true \
    --param emit_not_found_rows=true \
    --output /workspace/annotation_master_gene.csv
```

### 4) Open an interactive shell in the container

```bash
docker run --rm -it \
  -e DATABASE_URL="postgresql+psycopg2://user:password@host:5432/biofilter" \
  -v "$(pwd):/workspace" \
  --entrypoint /bin/bash \
  biofilter:bf4
```
