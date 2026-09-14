# Configuration

Biofilter resolves settings from:
1. command-line options (highest priority)
2. environment variables (`DATABASE_URL` or `BIOFILTER_DB_URI`)
3. `.biofilter.toml`
4. internal defaults

## Common Commands

Show resolved config:

```bash
biofilter config show
```

Get one value:

```bash
biofilter config get database.db_uri
```

Set one value:

```bash
biofilter config set database.db_uri "sqlite:///biofilter_dev.db"
```

Initialize template:

```bash
biofilter config init --path .
```

## Typical Keys

- `database.db_uri`
- `etl.data_root`

## Accepted `database.db_uri` values

| Scheme | Example | Writes |
|---|---|---|
| PostgreSQL | `postgresql+psycopg2://user:pass@host:5432/biofilter_prod` | yes |
| SQLite | `sqlite:///biofilter_dev.db` | yes |
| Parquet bundle | `parquet:///path/to/bundle/tables` | no (read-only) |

The `parquet://` scheme reads a Parquet bundle directly via DuckDB, for
environments without a database server. See [Parquet Backend](parquet_backend.md).

## Tips

- Prefer `--db-uri` in CI or one-off commands.
- Prefer `DATABASE_URL` in containers and orchestrators.
- Prefer `.biofilter.toml` for local development defaults.
