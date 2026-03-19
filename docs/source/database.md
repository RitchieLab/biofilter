# Database Operations

## Core Commands

Create DB:

```bash
biofilter db create-db --db-uri "sqlite:///biofilter_dev.db"
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

## Recommended Flow

```bash
biofilter db migrate --target head
biofilter db upgrade
biofilter db migrate --status
```
