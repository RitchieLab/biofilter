# Database Operations

Biofilter 4.3 has no persistent database. A build creates a throwaway
SQLite, stages the core sources through it, and leaves a parquet bundle
behind — see [Building Bundles](building_bundles.md).

The commands here remain for working with a database directly: creating
one for development, inspecting it, and moving data in and out.

## Creating and checking

```bash
biofilter db create-db --db-uri "sqlite:///biofilter_dev.db"
biofilter db ping --db-uri "sqlite:///biofilter_dev.db"
```

`create-db` builds the schema with `create_all` and applies the master
seeds. There is no migration step: 4.3 removed Alembic, because a
database is built once and a schema change produces a new bundle rather
than an in-place migration.

Applying seed updates to an existing database:

```bash
biofilter db upgrade
```

This is idempotent and seed-only. In earlier versions it also ran an
Alembic upgrade first.

## Backup and restore

Physical snapshot of a database, engine-specific:

```bash
biofilter db backup --out ./backups/dev.snapshot
biofilter db restore --in ./backups/dev.snapshot
```

## Bundles

A bundle is normally produced by `bundle build`. `db export` writes one
from an existing database, which is how bundles were made before 4.3:

```bash
biofilter db export --out ./exports/biofilter_bundle --format parquet
biofilter db import --in ./exports/biofilter_bundle --format parquet
```

Validate one without a database:

```bash
biofilter db verify --in ./exports/biofilter_bundle
biofilter db verify --in ./exports/biofilter_bundle --no-hashes
biofilter db verify --in ./exports/biofilter_bundle --schema
```

`--no-hashes` checks presence and size only. `--schema` also checks that
the tables present carry the columns this build expects, and exits 1 on
any problem, so CI can gate on it.

A bundle can be read directly, without importing it:

```bash
biofilter --bundle ./exports/biofilter_bundle report list
```

See [Parquet Backend](parquet_backend.md) and
[Building Bundles](building_bundles.md).
