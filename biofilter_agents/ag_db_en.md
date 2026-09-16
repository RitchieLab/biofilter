# AG DB - Database Operations in Biofilter (CLI/API)

Maintainer guide for the `biofilter db` group.

**Read this first:** Biofilter 4.3 has no persistent database. The product is
a **bundle** — a directory of parquet plus a manifest — produced by
`bundle build`, which creates its own throwaway SQLite and leaves the bundle
behind. See `ag_start.md` for reading one and
`docs/source/technical/building_bundles.md` for building one.

The `db` group remains for the jobs around that: creating the small registry a
build plans from, validating a bundle, and moving data between environments.

---

## 1) The commands

```
create-db   build the schema and apply seeds
ping        reachability and latency only
upgrade     re-apply seeds to an existing database (idempotent)
verify      validate a bundle against its manifest — needs no database
export      write a bundle from a database you already have
import      read one back in
backup      physical snapshot
restore     read a snapshot back
```

There is **no migration command and no migration chain.** A database is built
once by `create-db` with `create_all`, and a schema change produces a new
bundle rather than an in-place upgrade. If you are following an older note
that mentions `db migrate` or Alembic, it predates 4.3.

---

## 2) `db create-db`

```bash
biofilter db create-db --db-uri "sqlite:///biofilter_registry.sqlite"
biofilter db create-db --db-uri "postgresql+psycopg2://user:pass@host:5432/bf_dev"
```

| Option | Does |
|---|---|
| `--db-uri` | required — where to create it |
| `--overwrite` | replace an existing one |

Builds the schema with `create_all` and applies the JSON seeds from
`biofilter/modules/db/seed/`, including `initial_data_sources.json`.

**This is the prerequisite for `bundle plan`,** which reads the data source
registry from a database. It is a registry, not a data store: the build
creates its own staging database and never writes here.

> **PostgreSQL:** the target database must exist before Biofilter can connect.
> `createdb <name>` first, then `create-db`.

---

## 3) `db upgrade`

```bash
biofilter db upgrade --seed-dir seed
```

Re-applies the master seeds to an existing schema. Idempotent, and seed-only —
there is nothing else for it to do.

---

## 4) `db verify` — the one you will use most

```bash
biofilter db verify --in ./bundles/20260914
biofilter db verify --in ./bundles/20260914 --no-hashes
biofilter db verify --in ./bundles/20260914 --schema
```

Validates a bundle against its own manifest, and **needs no database at all**.

| Tier | Checks |
|---|---|
| default | every declared file present, at the declared size |
| + SHA-256 | only where the manifest recorded a digest |
| `--schema` | that the tables carry the columns this build expects |

Two things worth knowing:

- **`--schema` exits non-zero on drift,** which is what makes it usable as a
  gate in a script or CI.
- **The hash tier is currently a no-op for bundles from `bundle build`**,
  which records size but not SHA-256. For those, `verify` is checking presence
  and size whether or not you pass `--no-hashes`.

Opening a bundle only *warns* when a table is missing columns, deliberately:
a bundle built from a subset of sources legitimately has fewer, and refusing
would make it unusable for the sources it does have. `--schema` turns that
warning into an error.

---

## 5) `db export` / `db import`

`bundle build` is how bundles are normally produced. `db export` writes one
from a database you already have, which is useful for a development snapshot.

```bash
biofilter db export --out ./exports/dev_bundle --format parquet
biofilter db import --in ./exports/dev_bundle --format parquet
```

| `export` | |
|---|---|
| `--out`, `--format` | destination and format |
| `--table`, `--exclude-table` | narrow the selection |
| `--chunksize` | rows per batch |
| `--schema-version` | stamp a version |
| `--no-checksums` | skip SHA-256 |
| `--include-partition-children` | emit partition children as separate files |

| `import` | |
|---|---|
| `--in`, `--format` | source and format |
| `--allow-missing-tables` | tolerate a partial bundle |
| `--no-rebuild-indexes`, `--no-reset-sequences` | skip post-load steps |

**Confirm the target before importing.** It writes into whatever `--db-uri`
resolves to.

---

## 6) `db backup` / `db restore`

Physical snapshot, engine-specific.

```bash
biofilter db backup  --out ./backups/dev.snapshot
biofilter db restore --in  ./backups/dev.snapshot
```

**`restore` overwrites.** Never run it automatically, and confirm the target
environment first.

---

## 7) `db ping`

```bash
biofilter db ping --db-uri "postgresql+psycopg2://user:pass@host:5432/bf_dev"
```

Reports engine, host, database and latency. Tests reachability only — it does
not check whether the Biofilter schema is present.

---

## 8) Typical flows

### Preparing to build a bundle

```bash
biofilter db create-db --db-uri sqlite:///$SCRATCH/bf_registry.sqlite
biofilter bundle plan  --db-uri sqlite:///$SCRATCH/bf_registry.sqlite --out plan.json
# edit plan.json, then:
biofilter bundle build --plan plan.json --out ./bundles/$(date +%Y%m%d)
```

### Gating a bundle before publishing

```bash
biofilter bundle info  ./bundles/20260914
biofilter db verify --in ./bundles/20260914 --schema || exit 1
biofilter --bundle ./bundles/20260914 report run \
  --report-name annotate_variant --input rs429358 --output /tmp/smoke.csv
```

The variant report is the smoke test that matters — a gene report exercises
only the core branch and proves nothing about the partitioned tables.

---

## 9) API usage

```python
from biofilter import Biofilter

bf = Biofilter(db_uri="sqlite:///biofilter_registry.sqlite")
bf.db.connect()
bf.db.create_db(db_uri="sqlite:///biofilter_registry.sqlite", overwrite=False)
```

Reading is a different constructor — `Biofilter(bundle="...")`. Reading takes
a directory, writing takes a URI, and the two are never the same argument.

---

## 10) Safety

| Command | Why care |
|---|---|
| `db restore` | overwrites the target |
| `db import` | writes into whatever `--db-uri` resolves to |
| `db create-db --overwrite` | replaces an existing database |

None of these should run unattended. And none of them can damage a published
bundle: the read path has no write capability, and a published bundle should
additionally be `chmod -R a-w`.

---

## 11) See also

- `docs/source/technical/building_bundles.md` — where bundles come from
- `docs/source/technical/database.md` — the same commands, user-facing
- `ag_etl_en.md` — running one data source at a time
- `notebooks/lpc__deploy.md` — the cluster deployment
