#!/usr/bin/env sh
set -eu

if [ "$#" -eq 0 ]; then
    set -- biofilter
fi

# Allow: docker run image --help
if [ "${1#-}" != "$1" ]; then
    set -- biofilter "$@"
fi

# Keep compatibility with Alembic env override.
if [ -n "${DATABASE_URL:-}" ] && [ -z "${BIOFILTER_DB_URI:-}" ]; then
    export BIOFILTER_DB_URI="$DATABASE_URL"
fi

if [ "${1}" = "biofilter" ] && [ -n "${DATABASE_URL:-}" ]; then
    has_db_uri_arg=0
    for arg in "$@"; do
        case "$arg" in
            --db-uri|--db-uri=*)
                has_db_uri_arg=1
                break
                ;;
        esac
    done

    if [ "$has_db_uri_arg" -eq 0 ]; then
        shift
        set -- biofilter --db-uri "$DATABASE_URL" "$@"
    fi
fi

exec "$@"
