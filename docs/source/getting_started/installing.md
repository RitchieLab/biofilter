# Installing Biofilter

Three installation methods, in order of simplicity. Pick **one**.

## Which one should I use?

| Method     | Best for                                           | Requires                |
| ---------- | -------------------------------------------------- | ----------------------- |
| **pip**    | Most users — running reports, notebooks, scripting | Python 3.10+            |
| **Docker** | Avoiding any Python setup, reproducible CI runs    | Docker                  |
| **Source** | Contributors, debugging, modifying BF4 itself      | Python 3.10+ and Poetry |

## pip (recommended)

```bash
pip install biofilter
biofilter --help
```

That's it — `biofilter` is now available as a CLI command and the `biofilter` Python package is importable.

To verify:

```bash
biofilter --help
python -c "from biofilter import Biofilter; print('OK')"
```

## Docker

One image, published to two registries. Pull it rather than building:

```bash
docker pull ricoandre/biofilter:latest
```

The image carries no data. It expects two mounts:

| Mount | Mode | Holds |
|---|---|---|
| `/bundle` | read-only | the bundle directory, the one with `manifest.json` |
| `/workspace` | writable | where `--output` writes |

`BIOFILTER_BUNDLE` already defaults to `/bundle` inside the image, so a
normal run names no paths beyond the mounts:

```bash
docker run --rm \
  -v /shared/bundles/20260914:/bundle:ro \
  -v "$(pwd)/out:/workspace" \
  --user "$(id -u):$(id -g)" \
  ricoandre/biofilter:latest \
  report run --report-name annotate_gene --input TP53 --output /workspace/genes.csv
```

Three things worth knowing:

- **Mount the bundle root**, not its `tables/` subdirectory.
- **`--output` writes inside the container.** Point it at the mounted
  `/workspace` or the file leaves with the container.
- **`--user "$(id -u):$(id -g)"`** makes the output yours. Without it the
  files belong to the image's own user.

An interactive shell:

```bash
docker run --rm -it \
  -v /shared/bundles/20260914:/bundle:ro \
  -v "$(pwd):/workspace" \
  --entrypoint /bin/bash \
  ricoandre/biofilter:latest
```

To build it yourself from a checkout:

```bash
docker build -t biofilter:latest -f docker/Dockerfile .
```

### On a cluster (Apptainer/Singularity)

The same image. `--bind` replaces `-v`, and output ownership takes care of
itself because the container runs as you:

```bash
apptainer pull bf4.sif docker://ghcr.io/ritchielab/biofilter-hpc:latest

apptainer run \
  --bind /shared/bundles/20260914:/bundle:ro \
  --bind ~/bf4_output:/workspace \
  bf4.sif \
  report run --report-name annotate_gene --input APOE --output /workspace/apoe.csv
```

The GHCR name `biofilter-hpc` predates the merge of what used to be two
images; it is the same image as Docker Hub's.

## From source

For contributors or anyone modifying BF4 itself.

```bash
git clone https://github.com/RitchieLab/biofilter.git
cd biofilter
poetry install
poetry run biofilter --help
```

## Next step

Once installed, [point Biofilter at a bundle](reading_a_bundle.md) — one URI, no server to set up.
