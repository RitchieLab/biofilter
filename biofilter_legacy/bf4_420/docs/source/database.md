# Database Operations

## Core Commands

Create DB:

```bash
biofilter db create-db --db-uri "sqlite:///biofilter_dev.db"
```

Check DB:

```bash
biofilter db ping --db-uri "sqlite:///biofilter_dev.db"
```

Migrate schema:

```bash
biofilter db migrate --target head
biofilter db migrate --status
```

Upgrade schema + master seeds:

```bash
biofilter db upgrade
```

Backup / restore:

```bash
biofilter db backup --out ./backups/dev.snapshot
biofilter db restore --in ./backups/dev.snapshot
```

Export / import logical bundle:

```bash
biofilter db export --out ./exports/biofilter_bundle --format parquet
biofilter db import --in ./exports/biofilter_bundle --format parquet
```

A parquet bundle can also be **read directly**, without importing it into a
database, by pointing `--db-uri` at its `tables/` directory:

```bash
biofilter --db-uri "parquet:///exports/biofilter_bundle/tables" report list
```

See [Parquet Backend](parquet_backend.md).

## Recommended Flow

```bash
biofilter db migrate --target head
biofilter db upgrade
biofilter db migrate --status
```
