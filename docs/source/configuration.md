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

## Tips

- Prefer `--db-uri` in CI or one-off commands.
- Prefer `DATABASE_URL` in containers and orchestrators.
- Prefer `.biofilter.toml` for local development defaults.
