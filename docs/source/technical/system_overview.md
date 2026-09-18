# System Overview

Biofilter 4 is an entity-centric biological knowledge platform. It brings
genes, variants, proteins, pathways, diseases, ontology terms and chemicals
from many public sources into one model, and lets you query that model
without writing integration code for each source.

This page describes the system as a whole. Every section links to the
reference page that covers it in depth.

## The shape of the system

Biofilter has two halves, and they are separated by an artifact rather than
by an API.

One half **builds a bundle**: it downloads sources, normalizes them, and
writes a dated directory of parquet files. It is expensive, it runs a few
times a year, and it is the only code in the project that writes anything.

The other half **reads that bundle**: it opens the directory, runs SQL
against it, and returns tables. It is cheap, it runs constantly, and it has
no write path at all.

The two never run at the same time and share no engine, no connection and
no session. Everything else in this documentation is a detail of one side
or the other.

```{figure} ../_static/images/architecture.svg
:alt: A plan selects data sources; a core branch stages through a throwaway SQLite while a variant branch writes parquet directly; assembly publishes an immutable bundle; the read side opens that bundle with DuckDB and runs reports that return a table plus provenance.
:width: 100%
:name: fig-architecture

The whole system. Everything above the dark band produces the bundle; everything below it reads the bundle. The figure is an SVG — zoom in for the detail.
```

## The bundle

A bundle is a dated directory. It carries its data and the account of how
it was made:

| Path                | What it holds                                                                                    |
| ------------------- | ------------------------------------------------------------------------------------------------ |
| `tables/`           | One parquet file per table. Variant tables are split, one file per chromosome.                   |
| `manifest.json`     | Every file with its row count, byte size and branch. This is the catalogue the read side trusts. |
| `build_record.json` | What each run did, in the order it happened.                                                     |
| `bundle_plan.json`  | The plan the build was asked to cover.                                                           |

Two properties follow from this and shape how you work with it.

**Ids belong to one bundle.** `entities.id`, `variant_masters.variant_id`
and every other surrogate key are row identifiers valid only inside the
bundle that produced them. The drift between builds is small, which is
exactly what makes it dangerous: an id carried from one bundle into another
usually still resolves — to a different, biologically adjacent gene.
Nothing errors, and the answer is quietly about something else.

So pin the bundle, not the id. Every report result carries the `bundle_id`
it was computed against. Cross-domain links use natural keys instead:
`chromosome:position:ref:alt` for variants, HGNC ids and symbols for genes.

**A bundle is not rebuilt.** Sources move on — Ensembl publishes a new
release and stops serving the previous file, gnomAD versions its callsets
independently. Building the same plan later produces different data. When a
source changes you build a _new_ bundle, and the previous one stays as a
snapshot of a moment that can no longer be recreated. Reproducibility lives
in the retained artifact, which is why the manifest, the plan and the build
record travel inside it.

For the full treatment, see [Building Bundles](building_bundles.md).

## Building a bundle

Three commands:

```bash
biofilter bundle plan  --out bundle_plan.json
biofilter bundle build --plan bundle_plan.json
biofilter bundle info  ./biofilter_data/bundles/20260914
```

The **plan** lists every data source with an `include` flag, its DTP and
its version. Its order is the dependency declaration: sources run top to
bottom, and the core branch resolves entities against what ran before it,
so `hgnc` precedes `gene_ncbi`, which precedes `ensembl`.

The **build** runs in two branches, because the data has two shapes.

_The core branch_ covers 17 sources — genes, proteins, pathways, diseases,
GO, chemicals and the relationships between them. Together they are roughly
7 million rows and about 105 MB. They need transactions, because resolving
an entity means asking what earlier sources already created, so this branch
stages through a throwaway SQLite written through the ORM models.

_The variant branch_ covers 4 sources and billions of rows. Variant tables
carry no entity ids and no foreign keys; they link to genes through natural
keys. There is nothing to resolve, so there is nothing to stage: this
branch writes its final parquet directly, one file per chromosome, and has
no load step.

Both branches record themselves in the staging SQLite even though only one
stores data there. That ledger is what makes an interrupted build resumable
— finished sources are skipped on the next run — and it ships inside the
bundle as provenance, which is what the `platform_etl_status` and
`platform_etl_packages` reports read.

**Assembly is all or nothing.** Every planned source must have produced a
table, or nothing is published and the output directory is removed. A
bundle missing a table is indistinguishable from a complete one to whoever
reads it next.

See [ETL Operations](etl.md) for running a single source, and
[Bundle Requirements](bundle_requirements.md) for disk and time planning.

## Reading a bundle

A bundle is opened, not connected to:

```bash
biofilter --bundle ./biofilter_data/bundles/20260914 report list
biofilter --bundle ./biofilter_data/bundles/20260914 \
  report run --report-name annotate_gene --input TP53 --input BRCA1
```

```python
from biofilter import Biofilter

bf = Biofilter(bundle="./biofilter_data/bundles/20260914")

result = bf.report.run("annotate_gene", input_data=["TP53", "BRCA1"])
df = result.to_pandas()
result.provenance["bundle_id"]     # which build these rows came from
result.write("genes.csv")          # writes genes.csv.provenance.json beside it
```

`Bundle.open()` reads `manifest.json`, checks that every file it names
exists at the size it claims, starts an in-process DuckDB, and registers one
view per logical table — built from the manifest entries, not from a
directory scan. Each report execution gets its own cursor, so two reports
in one process cannot collide.

### What a report is

A report writes SQL and returns a table. It does not assemble rows in
Python, and it does not paste your input into a query: input arrives as a
registered relation and is joined, which is what keeps injection out and
what turns a ten-thousand-symbol filter into a hash join instead of a
literal list.

There are six report families:

| Family        | Count | What they answer                                                                                 |
| ------------- | ----- | ------------------------------------------------------------------------------------------------ |
| `annotate_*`  | 6     | What does the bundle know about these genes, variants, proteins, pathways, diseases, GO terms?   |
| `expand_*`    | 4     | What is connected to these — neighbours, relationships, variants in a gene, regulatory evidence? |
| `pair_*`      | 1     | How do these variants relate to each other?                                                      |
| `aggregate_*` | 1     | Roll a cohort's variants up to a summary.                                                        |
| `resolve_*`   | 1     | Which entity do these symbols actually mean?                                                     |
| `platform_*`  | 3     | What does this bundle contain, and how was it built?                                             |

Reports are discovered by being in the package, so adding one requires no
CLI change. Each ships with a guide in
`biofilter/modules/report/reports_explain/report_<name>.md`, which is what
`biofilter report explain --report-name <name>` prints.

### Absence is recorded, not hidden

Each report declares what it needs:

- **`requires`** — tables it cannot work without. Checked before the query
  runs, so a bundle built without GTEx says so in one line instead of
  failing somewhere inside the third join.
- **`optional`** — tables it uses when present and does without when
  absent. Their absence is not an error, which is the risk: the columns come
  back null, and a null because the source was never built looks exactly
  like a null answer.

So every result carries a `coverage` block in its provenance recording
which optional tables were missing and which chromosomes the bundle spans.
A result that is empty because the data was never built says so.

See [Reports](../reports.md) and the [Report Catalog](../report_catalog.md).

## The entity model

Every biological object the platform knows about is an `Entity`, with
`EntityAlias` for the many names it goes by and `EntityRelationship` for
how it connects to others. Domain detail hangs off that hub in master
tables — `GeneMaster`, `ProteinMaster`, `PathwayMaster`, `DiseaseMaster`,
`GOMaster`, `ChemicalMaster`.

This is what makes a cross-source question answerable in one query: a
symbol from one source and an accession from another resolve to the same
entity, so a report does not need to know which source contributed which
alias.

Variant data sits deliberately outside that hub. It is keyed by
`chromosome:position:ref:alt`, carries no entity id, and uses one table per
source with no cross-source joins — the fields each source contributes are
declared in that DTP's JSON config.

See [Entity Model and Omics Domains](../entity_and_omics.md) and the
[Database Schema](schema.md).

## Interfaces

The CLI has five command groups. Which side of the system they sit on tells
you what they need:

| Group    | Side  | For                                                     |
| -------- | ----- | ------------------------------------------------------- |
| `bundle` | build | plan a build, run it, inspect the result                |
| `etl`    | build | one source at a time: update, status, restart, rollback |
| `db`     | build | create, verify, export, import, backup                  |
| `report` | read  | list, explain, available-columns, example-input, run    |
| `config` | —     | show which settings actually resolved                   |

The Python API mirrors the split. `Biofilter(bundle="…")` gives you
`.report` over a read-only bundle; `Biofilter(db_uri="…")` takes a writable
SQLAlchemy URI and is what the ETL needs. Reading takes a directory,
writing takes a URI, and the two are never the same argument.

Configuration resolves in a fixed order — command flag, then environment
(`BIOFILTER_BUNDLE`, `DATABASE_URL`, `BIOFILTER_DB_URI`), then
`.biofilter.toml`. See [Configuration](configuration.md) and the
[CLI Reference](../cli_reference.md).

## Where files live

Under the data root (`biofilter_data/` by default):

| Path                  | Contents                       | Lifetime                                                                |
| --------------------- | ------------------------------ | ----------------------------------------------------------------------- |
| `downloads/`          | Raw source files as fetched    | Dropped as soon as their parquet exists, unless `--keep-raw`            |
| `processed/`          | Normalized parquet, per source | Variant output is kept — it _is_ the artifact; core output is transient |
| `staging/`            | The build's throwaway SQLite   | Deleted after assembly                                                  |
| `bundles/<YYYYMMDD>/` | Published bundles              | Permanent; archive these                                                |

Disk, not CPU, is the binding constraint on a full build: raw downloads
reach the terabyte range, which only fits because files are discarded per
chromosome as the build advances.

## Where to go next

| If you want to                    | Read                                                                                    |
| --------------------------------- | --------------------------------------------------------------------------------------- |
| Install and run your first report | [Getting Started](../getting_started/index.md)                                             |
| Build a bundle                    | [Building Bundles](building_bundles.md) · [Bundle Requirements](bundle_requirements.md) |
| Run a single source, or add one   | [ETL Operations](etl.md) · [Developer Extensions](developer_extensions.md)              |
| Find and run reports              | [Reports](../reports.md) · [Report Catalog](../report_catalog.md)                             |
| Understand the data model         | [Entity Model and Omics Domains](../entity_and_omics.md) · [Database Schema](schema.md)    |
| Look up a command                 | [CLI Reference](../cli_reference.md)                                                       |
| Fix something                     | [Troubleshooting](../troubleshooting.md)                                                   |
