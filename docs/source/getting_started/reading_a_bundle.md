# Pointing Biofilter at a Bundle

A **bundle** is what Biofilter reads: a directory of parquet files plus a
`manifest.json` describing them. There is no database server to install,
start or connect to.

If someone has given you a bundle, this page is the whole setup. If you
need to build one, see [Building Bundles](../building_bundles.md) — it
takes about two days and 150 GB, so borrow one first if you can.

## Point at it

The URI goes to the bundle's `tables/` directory, not the bundle root:

```bash
export BIOFILTER_DB_URI="parquet:///shared/bundles/bf4_20260912/tables"

biofilter report list
```

Or per command:

```bash
biofilter --db-uri "parquet:///shared/bundles/bf4_20260912/tables" report list
```

Or in `.biofilter.toml`, if you use the same bundle every day:

```toml
[database]
db_uri = "parquet:///shared/bundles/bf4_20260912/tables"
```

The path must be absolute, which is why the scheme carries three
slashes: `parquet://` plus `/shared/...`.

## Check it worked

```bash
biofilter bundle info /shared/bundles/bf4_20260912
```

```
Bundle id:      e29a11604a326d2e
Biofilter:      4.3.0
Built:          2026-09-12T11:46:28+00:00
Tables:         114
  core        41 table(s)       5,869,266 rows       71.9 MB
  variant     73 table(s)   3,130,016,386 rows   21,226.3 MB
```

Reports work unchanged — the same code runs over parquet as over a
database, with DuckDB underneath.

## Two things worth knowing

**A bundle is read-only.** Refreshing the data means getting a newer
bundle, not updating this one. That is deliberate: a bundle is a
point-in-time snapshot, and the sources it was built from have moved on
since.

**Ids belong to one bundle.** `entities.id`, `variant_id` and the rest
are internal row identifiers, valid only inside the bundle that produced
them. They are not stable across bundles, and the drift is small enough
to be dangerous: id 11450 is APOE in one bundle and APOF — a different
gene in the same family — in another. Nothing errors; the answer is
simply about the wrong gene.

So pin the bundle, not the id. Every report result carries the bundle it
came from:

```python
df = bf.report.run("annotation_master_gene", input_data=["APOE"])
df.attrs["bundle_id"]     # 'e29a11604a326d2e'
```

Note that this does not survive a CSV export. If you are writing ids to a
file that will be read back later, write the bundle id beside them.

## If something is wrong with the bundle

```bash
biofilter db verify --in /shared/bundles/bf4_20260912 --no-hashes --schema
```

`--schema` also checks that the tables present carry the columns this
version of Biofilter expects. Opening a bundle only warns about that, so
a partial bundle stays usable; this turns it into an error you can gate
on.

## Next step

[Find a report](finding_reports.md), then [run it](running_reports.md).
