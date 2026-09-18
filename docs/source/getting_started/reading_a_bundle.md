# Pointing Biofilter at a Bundle

## What a bundle is

A **bundle** is a folder. Inside it are the knowledge base's data —
genes, proteins, pathways, diseases, variants and the relationships
between them — already gathered from their original sources, cleaned up
and written as parquet files. A `manifest.json` sits alongside them as
the dictionary: what each file holds, how many rows, and which version of
the data this is.

Biofilter both writes bundles and reads them. The data lives in the
folder, not in a server, so there is nothing to install, start or
connect to. To run a query you give Biofilter the path and it does the
rest.

A bundle is a **photograph**: it captures the sources exactly as they
were on the day it was built. Nothing inside it changes afterwards. When
the sources move on — a new Ensembl release, a new gnomAD callset — you
build a new bundle rather than update this one, and the old one stays
readable for anyone who needs to reproduce work done against it.

A full human-genome bundle is around **21 GB** and holds roughly three
billion rows across 114 files. You can keep it on a laptop, a shared
drive, or an HPC filesystem — anywhere you can read a folder.

If someone has given you one, this page is the whole setup. If you need
to build one yourself, see [Building Bundles](../technical/building_bundles.md) —
borrow one first if you can.

## Point at it

Give Biofilter the bundle folder. It finds the data and the manifest
inside:

```bash
biofilter --bundle /shared/bundles/bf4_20260912 report list
```

If you use the same bundle every day, set it once:

```bash
export BIOFILTER_BUNDLE="/shared/bundles/bf4_20260912"

biofilter report list
biofilter report run --report-name platform_data_statistics
```

Or in `.biofilter.toml`:

```toml
[database]
bundle = "/shared/bundles/bf4_20260912"
```

A relative path there is resolved against the file itself, not your
working directory, so it means the same thing from the project root and
from a notebook two levels down.

`--db-uri` exists for the cases that really are a database — a staging
SQLite during a build, or a development PostgreSQL. Reading a bundle is
not one of them. Passing both is an error rather than a guess about which
you meant.

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

Biofilter opens the folder, reads `manifest.json` to learn which files
make up each table, and queries them with DuckDB in the same process.
There is no server to start and nothing to import.

## The one thing to watch

**Ids belong to one bundle.** `entities.id`, `variant_id` and the rest
are internal row identifiers, valid only inside the bundle that produced
them. They are not stable across bundles, and the drift is small enough
to be dangerous: id 11450 is APOE in one bundle and APOF — a different
gene in the same family — in another. Nothing errors; the answer is
simply about the wrong gene.

So pin the bundle, not the id. Every report result carries the bundle it
came from:

```python
result = bf.report.run("annotate_gene", input_data=["APOE"])
result.provenance["bundle_id"]     # 'e29a11604a326d2e'
```

Saving a result keeps that record: `result.write("genes.csv")` also
writes `genes.csv.provenance.json` beside it. Writing `.parquet` instead
stores the provenance inside the file's own metadata, so it travels even
if the sidecar is lost.

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
