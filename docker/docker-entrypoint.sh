#!/usr/bin/env sh
set -eu

# Run `biofilter` by default.
if [ "$#" -eq 0 ]; then
    set -- biofilter
fi

# Prepend `biofilter` unless the caller asked for a real executable.
#
# Covers both shapes people actually type:
#   docker run <image> --bundle /bundle report list   (starts with a dash)
#   docker run <image> report list                    (a subcommand)
# while leaving `docker run <image> bash` alone.
if [ "${1#-}" != "$1" ] || ! command -v "$1" >/dev/null 2>&1; then
    set -- biofilter "$@"
fi

# Nothing is injected into the argument list.
#
# The CLI resolves its own data source, in this order: --bundle, then
# --db-uri, then BIOFILTER_BUNDLE, then DATABASE_URL / BIOFILTER_DB_URI,
# then .biofilter.toml. Passing every one of those through as plain
# environment is enough, so this script only has to decide what to exec.
#
# It used to append `--db-uri "$DATABASE_URL"` when that variable was
# set. That broke the normal 4.3 invocation: `--bundle` and `--db-uri`
# together is a usage error, so a DATABASE_URL left over in the
# environment made `docker run ... --bundle /bundle report list` fail
# with a message about an argument the caller never passed.

exec "$@"
