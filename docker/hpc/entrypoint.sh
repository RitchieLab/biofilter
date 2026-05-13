#!/usr/bin/env bash
#
# HPC entrypoint: initialize PGDATA on first run, optionally restore from a
# pg_dump file, start PostgreSQL in the background, optionally run Alembic
# migrations, then exec the user command (defaults to `biofilter --help`).
#
# DB authentication: trust auth on localhost only. PG is never exposed outside
# the container, so no password is required.

set -euo pipefail

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
POSTGRES_USER="${POSTGRES_USER:-biofilter}"
POSTGRES_DB="${POSTGRES_DB:-biofilter}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"

log() { printf '[hpc] %s\n' "$*"; }

# Docker typically starts the container as root; PostgreSQL refuses to run as
# root, so drop to the `postgres` user. When invoked via Apptainer/Singularity
# this branch is skipped (the container runs as the host user).
if [ "$(id -u)" -eq 0 ]; then
    mkdir -p "$PGDATA"
    chown -R postgres:postgres "$PGDATA"
    exec gosu postgres "$0" "$@"
fi

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    log "First run — initializing PGDATA at $PGDATA"
    initdb \
        --pgdata="$PGDATA" \
        --username="$POSTGRES_USER" \
        --encoding=UTF8 \
        --auth-local=trust \
        --auth-host=trust \
        --no-instructions

    cat > "$PGDATA/pg_hba.conf" <<'EOF'
# BF4 HPC image — trust auth, localhost only.
# PostgreSQL is not exposed outside the container; no password required.
local   all   all                    trust
host    all   all   127.0.0.1/32     trust
host    all   all   ::1/128          trust
EOF

    sed -i "s/^#\?listen_addresses.*/listen_addresses = 'localhost'/" "$PGDATA/postgresql.conf"

    if [ -n "${BIOFILTER_RESTORE_DUMP:-}" ]; then
        if [ ! -f "$BIOFILTER_RESTORE_DUMP" ]; then
            log "ERROR: BIOFILTER_RESTORE_DUMP=$BIOFILTER_RESTORE_DUMP not found"
            exit 1
        fi

        pg_ctl -D "$PGDATA" -o "-p $POSTGRES_PORT" -w start

        psql -h localhost -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d postgres \
             -v ON_ERROR_STOP=1 \
             -c "CREATE DATABASE \"$POSTGRES_DB\";"

        restore_jobs="${BIOFILTER_RESTORE_JOBS:-4}"
        log "Restoring dump $BIOFILTER_RESTORE_DUMP into $POSTGRES_DB (jobs=$restore_jobs)"

        pg_restore \
            --host=localhost \
            --port="$POSTGRES_PORT" \
            --username="$POSTGRES_USER" \
            --dbname="$POSTGRES_DB" \
            --no-owner \
            --no-privileges \
            --jobs="$restore_jobs" \
            --exit-on-error \
            "$BIOFILTER_RESTORE_DUMP"

        log "Restore complete."
        pg_ctl -D "$PGDATA" -m fast -w stop
        log "PGDATA initialized with restored dump."
    else
        log "PGDATA initialized. Run 'biofilter db create-db --db-uri \"\$DATABASE_URL\"' to bootstrap schema."
    fi
elif [ -n "${BIOFILTER_RESTORE_DUMP:-}" ]; then
    log "PGDATA already initialized — ignoring BIOFILTER_RESTORE_DUMP=$BIOFILTER_RESTORE_DUMP"
fi

shutdown_pg() {
    log "Stopping PostgreSQL..."
    pg_ctl -D "$PGDATA" -m fast -w stop 2>/dev/null || true
}
trap shutdown_pg EXIT INT TERM

log "Starting PostgreSQL on localhost:$POSTGRES_PORT"
pg_ctl -D "$PGDATA" -o "-p $POSTGRES_PORT" -w start

until pg_isready -h localhost -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d postgres -q; do
    sleep 1
done
log "PostgreSQL ready."

export DATABASE_URL="postgresql+psycopg2://${POSTGRES_USER}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}"
export BIOFILTER_DB_URI="$DATABASE_URL"

if [ "${BIOFILTER_AUTO_MIGRATE:-0}" = "1" ]; then
    log "BIOFILTER_AUTO_MIGRATE=1 — running 'biofilter db migrate --target head'"
    biofilter db migrate --target head
fi

if [ "$#" -eq 0 ]; then
    set -- biofilter --help
elif [ "${1#-}" != "$1" ]; then
    set -- biofilter "$@"
fi

# Do NOT use exec here: we need the EXIT trap to fire so PG is stopped cleanly.
set +e
"$@"
cmd_exit=$?
set -e
exit "$cmd_exit"
