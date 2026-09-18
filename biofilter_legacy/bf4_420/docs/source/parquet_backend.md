# Parquet Backend (read-only)

Since **4.2.0**, Biofilter can read the knowledge base directly from a
**Parquet bundle** using DuckDB as the query engine — no database server, no
container, no per-user copy of the data.

This exists for environments where running PostgreSQL is not an option. The
motivating case is HPC: on a shared cluster users typically have no privileges
to run a database daemon, no persistent service host, and no reasonable way to
maintain a multi-hundred-gigabyte data directory per user.

---

## When to use it

| Backend | Use for | Writes |
|---|---|---|
| PostgreSQL | Production, ETL, multi-user with writes | yes |
| SQLite | Local development, small datasets | yes |
| **Parquet bundle** | **HPC, shared read-only snapshots, air-gapped analysis** | **no** |

Choose the Parquet backend when all of the following hold:

- you only need to **read** — run reports, no ETL and no migrations;
- the data can be distributed as a point-in-time snapshot;
- you want many users querying the same files concurrently without copies.

If you need to ingest data or apply migrations, use PostgreSQL or SQLite.

---

## Connecting

Point Biofilter at the bundle's `tables/` directory with the `parquet://` URI
scheme:

```bash
export BIOFILTER_DB_URI="parquet:///shared/bundles/bf4_2026_06/tables"

biofilter report run \
  --report-name annotation_master_gene \
  --input APOE \
  --output apoe.csv
```

`DATABASE_URL` works too, as does the `--db-uri` option for a single command:

```bash
biofilter --db-uri "parquet:///shared/bundles/bf4_2026_06/tables" \
  report run --report-name annotation_master_variant --input rs429358 --output out.csv
```

From Python:

```python
from biofilter import Biofilter

bf = Biofilter(db_uri="parquet:///shared/bundles/bf4_2026_06/tables")
df = bf.report.run("annotation_master_variant", input_data=["rs429358"])
```

Both `parquet://relative/path` and `parquet:///absolute/path` are accepted;
the path is expanded and resolved to an absolute path either way.

---

## How it works

1. The `parquet://` URI is translated internally to an in-memory DuckDB engine
   (`duckdb:///:memory:`).
2. On connect, Biofilter scans the directory and registers one SQL `VIEW` per
   `*.parquet` file, backed by DuckDB's `read_parquet()`.
3. A SQLAlchemy `StaticPool` keeps every session on the same connection, so all
   sessions share the in-memory catalog where those views live.
4. From that point on the ORM resolves normally.

**Reports require no changes.** They run through the same ORM layer used by
PostgreSQL and SQLite, so every report in the catalog works unmodified against
a bundle.

### Partitioned tables

Files whose name contains `_chr_` are **skipped** during view registration.

On PostgreSQL, `variant_masters` and `variant_molecular_effects` are
partitioned by chromosome, and an export writes both the consolidated parent
(`variant_molecular_effects.parquet`) and its 25 children
(`variant_molecular_effects_chr_1.parquet`, …). Registering both would make
every row appear twice, so only the consolidated parent is queried.

This means the consolidated file is **required**. A bundle containing only the
`_chr_*` children will connect successfully but the corresponding table will
not exist, and variant reports will fail.

---

(read-only-enforcement)=
## Read-only enforcement

The backend is read-only at two levels:

- **Storage** — DuckDB rejects any write against a `read_parquet` view.
- **Application** — the `Database.read_only` flag exposes the same information
  to Biofilter code.

Deployments typically add a third level by making the bundle directory
non-writable on disk (`chmod -R a-w`).

Consequences:

- `biofilter etl update` / `update-all` — **not supported**; run the ETL
  against PostgreSQL and export a new bundle.
- `biofilter db migrate` / `db upgrade` / `db create-db` — **not supported**.
- Refreshing the data means producing a **new bundle**, not modifying the
  current one.

---

## Producing a bundle

Export from any existing PostgreSQL or SQLite instance:

```bash
biofilter db export \
  --db-uri "postgresql+psycopg2://user:password@host:5432/biofilter_prod" \
  --out /shared/bundles/bf4_2026_06 \
  --format parquet
```

This writes:

```
bf4_2026_06/
├── manifest.json
└── tables/
    ├── entities.parquet
    ├── gene_masters.parquet
    └── ...
```

The `tables/` subdirectory is what `parquet://` points at.

> **Production-scale caveat.** On a full production database the partitioned
> variant tables make the single-command export impractical: exporting the
> consolidated parent forces PostgreSQL to UNION all 25 partitions on every
> chunk. The bundle has to be assembled in stages instead — export the
> partition children individually, then concatenate them outside the database.
> The full procedure is documented in the
> [LPC deployment guide](https://github.com/RitchieLab/biofilter/blob/main/notebooks/Templates/lpc__deploy.md).

---

## Performance

Queries stream from disk with column pruning and predicate pushdown, so memory
stays low even against billion-row tables. Measured on the production snapshot:

| Workload | Storage | Wall clock | Peak memory |
|---|---|---|---|
| 10,000 rsIDs against `variant_molecular_effects` (1.79 B rows) | local NVMe | 1.18 s | 89 MB |
| Same workload | shared GPFS | 15.52 s | 89 MB |
| End-to-end CLI report (`annotation_master_variant`, 3 rsIDs) | local NVMe | 1.22 s | — |

Shared network storage costs roughly an order of magnitude in wall clock and
still lands well inside interactive range.

---

## Troubleshooting

**`No *.parquet files found in <dir>`**
The URI points at the wrong directory. It must point at `tables/`, not at the
bundle root that holds `manifest.json`.

**`parquet:// directory not found: <dir>`**
The path does not exist or is not readable. Check the mount, and remember the
path is resolved relative to the process working directory when given without
a leading slash.

**A table is missing or a variant report fails**
The bundle is likely missing a consolidated parent for a partitioned table.
Check that `variant_molecular_effects.parquet` exists alongside the
`_chr_*` files — the children alone are not enough.

**A write command fails**
Expected. The backend is read-only; see [Read-only enforcement](#read-only-enforcement).

---

## See also

- [Connecting to a Database](getting_started/connecting_db.md) — all backend options
- [Database Operations](database.md) — export/import commands
- [Configuration](configuration.md) — how `db_uri` is resolved
