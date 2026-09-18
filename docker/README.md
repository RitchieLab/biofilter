# Docker

Run Biofilter without installing it. **One image**, built from
[`Dockerfile`](Dockerfile) and published to the GitHub Container Registry:

```
ghcr.io/ritchielab/biofilter
```

The same image serves Docker locally and Apptainer on a cluster — what
changes is the flag that mounts the bundle, not the image.

The image contains no data. A bundle is bind-mounted at run time, so the
same image serves any bundle — and a 20+ GB bundle of ZSTD parquet is
something an image layer would not compress anyway.

The contract is two mounts:

| Mount | Mode | Holds |
|---|---|---|
| `/bundle` | read-only | The bundle directory, the one with `manifest.json` |
| `/workspace` | writable | Where `--output` writes |

`BIOFILTER_BUNDLE` defaults to `/bundle` in the image, so binding there
needs no extra environment.

## Build

From the project root:

```bash
docker build -t biofilter:latest -f docker/Dockerfile .
```

## Run a report against a bundle

Mount the bundle read-only, mount a directory for output, and name the
bundle path as the container sees it:

```bash
docker run --rm \
  -v /path/to/bundles/20260914:/bundle:ro \
  -v "$(pwd)/out:/workspace" \
  biofilter:latest \
  report run --report-name annotate_gene --input TP53 --output /workspace/genes.csv
```

Four things to get right:

- **Mount the bundle root**, the directory holding `manifest.json` — not
  its `tables/` subdirectory.
- **`--output` writes inside the container.** Point it at the mounted
  `/workspace`, or the file disappears with the container.
- **`:ro` is worth setting.** A bundle is read-only by nature and nothing
  in the read path writes to it, so the mount can say so.
- **Output ownership.** The image runs as its own user, so under Docker
  the files land owned by that uid. Add `--user "$(id -u):$(id -g)"` to
  get your own. Under Apptainer this does not arise — the container runs
  as you.

With an env file instead:

```bash
cp docker/.env.example docker/.env      # then edit
docker run --rm --env-file docker/.env \
  -v /path/to/bundles/20260914:/bundle:ro \
  biofilter:latest report list
```

A platform that mounts elsewhere — WDL and CWL runners generally do —
either sets `BIOFILTER_BUNDLE` to its own path or passes `--bundle`.

## On a cluster, with Apptainer

The same image, converted to a `.sif` on pull. Nothing else changes —
`--bind` where Docker says `-v`, `--env` where Docker says `-e`:

```bash
apptainer pull bf4.sif docker://ghcr.io/ritchielab/biofilter:latest

mkdir -p ~/bf4_output

apptainer run \
  --bind /project/hall_shared/datasets/biofilter/20260914:/bundle:ro \
  --bind ~/bf4_output:/workspace \
  bf4.sif \
  report run --report-name annotate_gene --input APOE --output /workspace/apoe.csv
```

Two differences from Docker worth knowing:

- **Output ownership is not a problem.** Apptainer runs the container as
  the invoking user, so the image's own user is ignored and files land
  owned by you.
- **The container filesystem is read-only.** Anything written has to go to
  a bind, which is what `/workspace` is for.

### Do you need the container at all?

Often not. On a cluster where you can create a virtualenv,
`pip install biofilter` and pointing at the bundle works and skips the
image entirely. The container earns its place when you want the identical
environment across machines, or when cluster policy prefers it.

For the Penn LPC specifically — module tree, shared bundle location, LSF
job templates — see
[`notebooks/lpc__quickstart.md`](../notebooks/lpc__quickstart.md) for
users and [`notebooks/lpc__deploy.md`](../notebooks/lpc__deploy.md) for
whoever maintains the install.

## Run against a database

Only the ETL, `bundle plan` and the `db` commands need one. Reports do
not.

```bash
docker run --rm \
  -e DATABASE_URL="postgresql+psycopg2://user:password@host:5432/biofilter_dev" \
  biofilter:latest db ping
```

A full `bundle build` inside a container is possible but rarely what you
want: it needs around 150 GB of working space and runs for about two days.
See [Bundle Requirements](../docs/source/technical/bundle_requirements.md).

## How the container finds its data

The CLI resolves this itself; the entrypoint passes the environment
through untouched. In order:

1. `--bundle` on the command line
2. `--db-uri` on the command line
3. `BIOFILTER_BUNDLE`
4. `DATABASE_URL`, then `BIOFILTER_DB_URI`
5. `.biofilter.toml` — `[database] bundle`, then `[database] db_uri`

`--bundle` and `--db-uri` together is an error rather than a guess about
which you meant.

Mounting your project directory at `/workspace` lets the container pick up
a `.biofilter.toml` you already have — though inside a container, the
paths in it have to be the paths the container sees:

```bash
docker run --rm -v "$(pwd):/workspace" biofilter:latest config show
```

## Interactive shell

```bash
docker run --rm -it \
  -v /path/to/bundles/20260914:/bundle:ro \
  -e BIOFILTER_BUNDLE=/bundle \
  --entrypoint /bin/bash \
  biofilter:latest
```

## Publishing

Via GitHub Actions, which is the supported path:

`.github/workflows/docker-publish.yml` builds and publishes. It triggers
on a pushed git tag (`v4.3.0` publishes `4.3.0` and `latest`), or
manually from the Actions tab.

GHCR authenticates with the workflow's own token, so publishing needs no
repository secrets and works in a fork.

Manually, if you have to:

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f docker/Dockerfile \
  -t ghcr.io/ritchielab/biofilter:4.3.0 \
  -t ghcr.io/ritchielab/biofilter:latest \
  --provenance=false --sbom=false \
  --push .
```
