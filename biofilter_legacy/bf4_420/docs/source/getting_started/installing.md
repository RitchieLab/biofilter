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

Run any Biofilter command inside the container, passing the database URL via environment variable:

```bash
docker run --rm -it \
  -e DATABASE_URL="postgresql+psycopg2://user:pass@host:5432/biofilter_dev" \
  -v "$(pwd):/workspace" \
  --entrypoint /bin/bash \
  biofilter:bf4
```

To save report outputs to your local filesystem, mount a volume:

```bash
docker run --rm \
  -e DATABASE_URL="postgresql+psycopg2://user:pass@host:5432/biofilter_dev" \
  -v "$(pwd)/outputs:/workspace/outputs" \
  biofilter:bf4 \
  biofilter report run --report-name etl_status --output /workspace/outputs/etl_status.csv
```

## From source

For contributors or anyone modifying BF4 itself.

```bash
git clone https://github.com/RitchieLab/biofilter.git
cd biofilter
poetry install
poetry run biofilter --help
```

## Next step

Once installed, [connect to a database](connecting_db.md) — either an existing instance or a fresh local one.
