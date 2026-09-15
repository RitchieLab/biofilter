# Configuration

Biofilter resolves settings from:
1. command-line options (highest priority)
2. environment variables (`BIOFILTER_BUNDLE`, or `DATABASE_URL` /
   `BIOFILTER_DB_URI` for a database)
3. `.biofilter.toml`
4. internal defaults

## Where the data is

Reports read a **bundle**; the ETL and bundle builds write to a
**database**. Both live under `[database]`, and a configured bundle wins
over a configured `db_uri` — the same precedence `--bundle` has over
`--db-uri`.

```toml
[database]
# Where reports read from. A bundle is a directory — the one holding
# manifest.json, not its tables/ subdirectory. Pointing at tables/
# reaches the parquet but leaves behind the bundle id, the plan, and the
# table map the reader resolves its views from.
#
# A relative path is relative to THIS FILE, not the working directory, so
# it means the same thing from the project root and from a notebook two
# levels down.
bundle = "./biofilter_data/bundles/20260914"

# Only for writing. Keep credentials out of this file — use DATABASE_URL.
# db_uri = "postgresql+psycopg2://user:pass@host/biofilter_dev"
```

`biofilter config show` prints which one is in effect.

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
| Parquet bundle | `parquet:///path/to/bundle` | no (read-only) |

The `parquet://` scheme reads a Parquet bundle directly via DuckDB, for
environments without a database server. See [Parquet Backend](parquet_backend.md).

## Tips

- Prefer `--db-uri` in CI or one-off commands.
- Prefer `DATABASE_URL` in containers and orchestrators.
- Prefer `.biofilter.toml` for local development defaults.
