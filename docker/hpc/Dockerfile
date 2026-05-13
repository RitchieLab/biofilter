# syntax=docker/dockerfile:1.7
#
# Biofilter 4 — HPC image (BF4 + PostgreSQL 16 in one container).
#
# Use case: HPC clusters and standalone deployments where no external
# PostgreSQL service is available. PGDATA must be bind-mounted to a host
# directory so the database survives container removal.
#
# For deployments that point at an external PostgreSQL, prefer the app-only
# image at docker/Dockerfile.

FROM postgres:16-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    VENV_PATH=/opt/biofilter/venv \
    PATH=/opt/biofilter/venv/bin:/usr/lib/postgresql/16/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    POSTGRES_USER=biofilter \
    POSTGRES_DB=biofilter \
    POSTGRES_PORT=5432 \
    PGDATA=/var/lib/postgresql/data \
    BIOFILTER_AUTO_MIGRATE=0

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-venv \
        python3-pip \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv "$VENV_PATH" \
    && "$VENV_PATH/bin/pip" install --upgrade pip setuptools wheel

WORKDIR /app

COPY pyproject.toml poetry.lock README.md ./
COPY biofilter ./biofilter

RUN "$VENV_PATH/bin/pip" install . \
    && chown -R postgres:postgres "$VENV_PATH" /app

COPY docker/hpc/entrypoint.sh /usr/local/bin/hpc-entrypoint.sh
RUN chmod +x /usr/local/bin/hpc-entrypoint.sh

RUN mkdir -p /workspace && chown postgres:postgres /workspace
WORKDIR /workspace

EXPOSE 5432

ENTRYPOINT ["/usr/local/bin/hpc-entrypoint.sh"]
CMD ["biofilter", "--help"]
