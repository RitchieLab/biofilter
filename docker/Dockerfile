# syntax=docker/dockerfile:1.7
#
# Biofilter 4 — one image, for local Docker and for HPC under
# Apptainer/Singularity alike.
#
# The image carries no data. A bundle is bind-mounted at run time, so the
# same image serves any bundle — and a bundle is 20+ GB of ZSTD parquet
# that an image layer would not compress.
#
# The contract is two mounts:
#
#   /bundle      read-only   the bundle directory, the one with manifest.json
#   /workspace   writable    where --output writes
#
#   docker run --rm \
#     -v /path/to/bundles/20260914:/bundle:ro \
#     -v "$PWD/out:/workspace" \
#     biofilter:4.3.0 report run --report-name annotate_gene --input TP53 \
#       --output /workspace/genes.csv
#
# BIOFILTER_BUNDLE defaults to /bundle, so the usual run names no paths
# beyond the mounts. Override it, or pass --bundle, when a platform
# mounts elsewhere — WDL and CWL runners generally do.

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PATH="/opt/biofilter/venv/bin:${PATH}" \
    BIOFILTER_BUNDLE=/bundle

WORKDIR /app

# libpq5 keeps psycopg2 importable for the write path (ETL, bundle plan).
# No database server is bundled, and reading needs none.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/biofilter/venv \
    && /opt/biofilter/venv/bin/pip install --upgrade pip setuptools wheel

COPY pyproject.toml poetry.lock README.md ./
COPY biofilter ./biofilter

RUN /opt/biofilter/venv/bin/pip install .

COPY docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# /bundle and /workspace exist so a bind has somewhere to land, which
# older Apptainer needs and newer Apptainer is happier with.
RUN chmod +x /usr/local/bin/docker-entrypoint.sh \
    && useradd --create-home --shell /bin/bash biofilter \
    && mkdir -p /bundle /workspace \
    && chown -R biofilter:biofilter /workspace

# Under Apptainer this is ignored — the container runs as the invoking
# user, which is what makes output land with the right owner on a shared
# filesystem. Under Docker it applies, so pass
# --user "$(id -u):$(id -g)" if you want your own uid on the output.
USER biofilter
WORKDIR /workspace

LABEL org.opencontainers.image.title="Biofilter 4" \
      org.opencontainers.image.description="Entity-centric biological knowledge platform. Reads a parquet bundle; mount it at /bundle." \
      org.opencontainers.image.source="https://github.com/RitchieLab/biofilter" \
      org.opencontainers.image.licenses="MIT"

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["biofilter", "--help"]
