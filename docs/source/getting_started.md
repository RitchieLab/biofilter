# Getting Started

Biofilter 4 is a persistent biological knowledge layer.

If you are new to the platform, read [System Overview](system_overview.md) first, then follow the steps below.

## 1) Install

PyPI:

```bash
pip install biofilter
biofilter --help
```

Docker (application-only container):

```bash
docker build -t biofilter:bf4 -f docker/Dockerfile .
docker run --rm \
  -e DATABASE_URL="postgresql+psycopg2://bioadmin:change_me@host:5432/biofilter_dev" \
  biofilter:bf4
```

Run a report and save output to host filesystem:

```bash
docker run --rm \
  -e DATABASE_URL="postgresql+psycopg2://bioadmin:change_me@host:5432/biofilter_dev" \
  -v "$(pwd)/outputs:/workspace/outputs" \
  biofilter:bf4 \
  biofilter report run --report-name etl_status --output /workspace/outputs/etl_status.csv
```

Source (contributors):

```bash
git clone <repo>
cd biofilter
poetry install
poetry run biofilter --help
```

## 2) Initialize Configuration

Create `.biofilter.toml`:

```bash
biofilter config init --path .
```

Set DB URI:

```bash
biofilter config set database.db_uri "postgresql+psycopg2://bioadmin:change_me@localhost:5432/biofilter_dev"
```

Optional (especially in containers/CI), use environment variable instead:

```bash
export DATABASE_URL="postgresql+psycopg2://bioadmin:change_me@localhost:5432/biofilter_dev"
```

Set ETL data root:

```bash
biofilter config set etl.data_root "./biofilter_data"
```

Validate:

```bash
biofilter config show
```

## 3) Bootstrap Database

```bash
biofilter db migrate --target head
biofilter db upgrade
```

## 4) Run First ETL

```bash
biofilter etl update --data-source hgnc
biofilter etl status
```

## 5) Run First Report

```bash
biofilter report list
biofilter report run --report-name etl_status
```
