# syntax=docker/dockerfile:1.7
#
# Biofilter 4 — HPC image (read-only, Parquet-backed via DuckDB).
#
# Use case: HPC clusters where users run reports against a snapshot
# Parquet bundle on shared storage. No PostgreSQL, no import phase,
# no bind-mount of a PGDATA, no single-user lock — multiple processes
# can read the same bundle concurrently.
#
# Point BIOFILTER_DB_URI (or pass --db-uri) at the bundle's tables/
# directory:
#
#     parquet:///path/to/bundle/tables
#
# For deployments that point at an external PostgreSQL (VPS / ETL),
# use the app-only image at docker/Dockerfile instead.

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PATH="/opt/biofilter/venv/bin:${PATH}"

WORKDIR /app

# Minimal system deps. libpq5 is kept so psycopg2-binary stays importable
# (some BF4 utilities still touch it) but no PostgreSQL server is bundled.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/biofilter/venv \
    && /opt/biofilter/venv/bin/pip install --upgrade pip setuptools wheel

COPY pyproject.toml poetry.lock README.md ./
COPY biofilter ./biofilter

RUN /opt/biofilter/venv/bin/pip install .

# Reuse the shared entrypoint from the app-only image — it just maps
# DATABASE_URL / BIOFILTER_DB_URI into a --db-uri argument and execs.
COPY docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh \
    && useradd --create-home --shell /bin/bash biofilter \
    && mkdir -p /workspace \
    && chown -R biofilter:biofilter /workspace

USER biofilter
WORKDIR /workspace

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["biofilter", "--help"]
