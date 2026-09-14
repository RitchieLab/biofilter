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

Build the application-only image:

```bash
docker build -t biofilter:bf4 -f docker/Dockerfile "https://github.com/RitchieLab/biofilter.git#biofilter3r"
```

Mount the bundle and point at it. The container needs read access to the
bundle directory and somewhere to write results:

```bash
docker run --rm -it \
  -v /shared/bundles/bf4_20260912:/bundle:ro \
  -v "$(pwd):/workspace" \
  -e BIOFILTER_BUNDLE="/bundle" \
  --entrypoint /bin/bash \
  biofilter:bf4
```

To run one report and keep the output:

```bash
docker run --rm \
  -v /shared/bundles/bf4_20260912:/bundle:ro \
  -v "$(pwd)/outputs:/workspace/outputs" \
  -e BIOFILTER_BUNDLE="/bundle" \
  biofilter:bf4 \
  biofilter report run --report-name etl_status --output /workspace/outputs/etl_status.csv
```

The bundle is mounted read-only because nothing writes to it — refreshing
data means a newer bundle, not an update to this one.

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
