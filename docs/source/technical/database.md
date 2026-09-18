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
seeds, including the data source registry that `bundle plan` reads. There
is no migration step and no migration chain: a database is built once, and
a schema change produces a new bundle rather than an in-place upgrade.

Applying seed updates to an existing database:

```bash
biofilter db upgrade
```

This is idempotent and seed-only.

## Backup and restore

Physical snapshot of a database, engine-specific:

```bash
biofilter db backup --out ./backups/dev.snapshot
biofilter db restore --in ./backups/dev.snapshot
```

## Bundles

A bundle is normally produced by `bundle build`. `db export` writes one
from a database you already have, which is useful for a development
snapshot:

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

Note that the hash tier only runs where the manifest recorded a digest.
Bundles produced by `bundle build` record size but not SHA-256, so for
those `verify` is checking presence and size whether or not you pass
`--no-hashes`.

A bundle can be read directly, without importing it:

```bash
biofilter --bundle ./exports/biofilter_bundle report list
```

See [The Read Path](read_path.md) and
[Building Bundles](building_bundles.md).
