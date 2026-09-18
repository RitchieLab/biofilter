# BF4 User Documentation



<!-- ===== SOURCE FILE: docs/source/cli_reference.md ===== -->

# CLI Reference

Every command, with the options that matter. The groups split by what they
do: `report` reads a bundle, `bundle` / `etl` / `db` build one, `config`
inspects settings.

## Global

```bash
biofilter [--bundle PATH] [--db-uri URI] [--debug] COMMAND ...
biofilter --version
```

| Option | For |
|---|---|
| `--bundle PATH` | The bundle to read. What `report` needs. |
| `--db-uri URI` | A writable SQLAlchemy URI. What `etl`, `db` and `bundle plan` need. |
| `--debug` | Debug logging. Accepted by most commands individually too. |

`--bundle` wins over `--db-uri`. Both fall back to the environment
(`BIOFILTER_BUNDLE`, `DATABASE_URL`, `BIOFILTER_DB_URI`) and then to
`.biofilter.toml`.

## report

Reads a bundle. Never writes.

| Command | Options |
|---|---|
| `report list` | `--verbose` |
| `report explain` | `--report-name` |
| `report example-input` | `--report-name` |
| `report available-columns` | `--report-name` |
| `report run` | `--report-name` · `--input` `--input-file` `--input-column` · `--param` `--params-json` `--params-file` `--params-template` · `--output` |
| `report refresh` | — rebuild the report index after adding one |

`--report-name` also accepts the shorter `--name`. Everything except `run`
works without a bundle.

```bash
biofilter --bundle <path> report run --report-name annotate_gene \
  --input TP53 --output genes.csv
```

See [Reports](reports.md) and the [Report Catalog](report_catalog.md).

## bundle

Builds a bundle from a plan.

| Command | Options |
|---|---|
| `bundle plan` | `--out` · `--all-sources` · `--force` |
| `bundle build` | `--plan` `--out` `--data-root` · `--restart` · `--keep-raw` `--keep-processed` · `--min-free-gb` · `--into` `--no-assemble` |
| `bundle info` | takes the bundle directory as an argument |

See [Building Bundles](technical/building_bundles.md).

## etl

Runs one data source at a time. Useful when developing a DTP or re-running
a single source; a full build goes through `bundle build`.

| Command | Options |
|---|---|
| `etl update` | `--data-source` `--source-system` · `--run-step` `--force-step` |
| `etl update-all` | `--data-source` `--source-system` · `--only-active/--all` · `--drop-files/--keep-files` · `--stop-on-error` |
| `etl status` | `--data-source` `--source-system` · `--only-active/--all` |
| `etl explain` | `--data-source` `--source-system` `--dtp-script` |
| `etl restart` | `--data-source` `--source-system` · `--delete-files` |
| `etl rollback` | `--data-source` `--source-system` · `--package-id` · `--delete-files` |
| `etl index` | `--group` · `--drop-only` `--no-drop-first` · `--no-read-mode` `--no-write-mode` |

`--data-source` and `--source-system` are repeatable. `--run-step` and
`--force-step` are too, so a full explicit run is `--run-step extract
--run-step transform --run-step load`.

**`restart` and `rollback` destroy work.** Neither should be run
automatically or without knowing what it will remove.

See [ETL Operations](technical/etl.md).

## db

| Command | Options |
|---|---|
| `db create-db` | `--db-uri` (required) · `--overwrite` |
| `db ping` | — reachability and latency only |
| `db upgrade` | `--seed-dir` · `--force` — re-applies seeds, idempotent |
| `db verify` | `--in` (required) · `--no-hashes` · `--schema` |
| `db export` | `--out` · `--format` · `--table` `--exclude-table` · `--chunksize` · `--schema-version` · `--no-checksums` · `--include-partition-children` |
| `db import` | `--in` · `--format` · `--allow-missing-tables` · `--no-rebuild-indexes` · `--no-reset-sequences` |
| `db backup` | `--out` |
| `db restore` | `--in` |

`db verify` needs no database — it validates a bundle against its own
manifest, and `--schema` makes it exit non-zero on drift, for CI.

**`restore` overwrites.** Confirm the target before running it.

See [Database Operations](technical/database.md).

## config

| Command | Options |
|---|---|
| `config show` | — prints what actually resolved, and from where |
| `config get SECTION.KEY` | `--path` |
| `config set SECTION.KEY VALUE` | `--path` |
| `config init` | `--path` · `--force` · `--db-uri` `--data-root` |

```bash
biofilter config show
biofilter config get database.bundle
```

See [Configuration](technical/configuration.md).



<!-- ===== SOURCE FILE: docs/source/entity_and_omics.md ===== -->

# The Entity Model

What Biofilter means by a gene, and why you can call it whatever you like.

## One concept, many names

Every biological object Biofilter knows is an **entity** — a single row that
stands for one concept. Around it sit the names it goes by and the things it
connects to:

| | |
|---|---|
| `entities` | The concept itself. One row per gene, protein, pathway, disease, GO term, chemical. |
| `entity_aliases` | Every name, symbol, synonym and external code that points at it. |
| `entity_relationships` | A typed, directed link between two entities. |
| `entity_relationship_types` | What a link means: `interacts_with`, `in_pathway`, `encodes`, `is_a`. |
| `entity_groups` | Which domain an entity belongs to. |

The practical effect is the one that matters to you: **you do not have to
know which name a source used.** `TP53`, `ENSG00000141510`, `HGNC:11998` and
`7157` all resolve to the same entity, so a gene list assembled from three
different papers works without being harmonized first.

In one full core build, that looked like this:

| | |
|---|---:|
| Entities | 203,393 |
| Aliases pointing at them | 912,316 |
| Relationships between them | 4,210,591 |

Roughly 4.5 names per concept. That ratio is the integration work the
platform is doing on your behalf.

Where the names come from, by source:

| Source | Aliases |
|---|---:|
| HGNC | 250,301 |
| MONDO | 155,752 |
| UniProt | 111,490 |
| ENTREZ | 70,220 |
| ENSEMBL | 45,863 |
| GO | 41,378 |
| NCBI | 26,224 |
| UCSC | 24,337 |
| MEDGEN | 21,660 |

One practical caution: `xref_source` is recorded as each source spelled it,
and the spellings are not normalized — `UniProt` and `Uniprot` both appear,
as separate values. Match case-insensitively if you filter on it.

## Following the links

Relationships are what make a cross-domain question answerable in one query:
gene → pathway → disease is a traversal, not a join you have to hand-write
per domain. The types actually present in a full build:

| Type | Links |
|---|---:|
| `interacts_with` | 3,950,396 |
| `in_pathway` | 180,646 |
| `is_a` | 46,580 |
| `encodes` | 20,255 |
| `part_of` | 6,511 |
| `Disease_has_disruption` | 6,203 |

`expand_entity_relationship` returns these rows directly;
`expand_entity_neighborhood` summarises them as degree per entity; and
`pair_variants` uses them to decide whether two genes share enough biology
to be worth reporting.

## Domain detail

The entity layer carries identity and connection. The specifics live in
master tables beside it — `gene_masters`, `protein_masters`,
`pathway_masters`, `disease_masters`, `go_masters`, `chemical_masters` —
each linked back to its entity. That is where you find a gene's locus type,
a protein's Pfam domains, a disease's cross-references.

The `annotate_*` reports are the read interface over this layer: give them
names, get back the master detail plus relationship counts.

## Variants are deliberately outside this

This is the exception worth knowing, because the rest of the model does not
predict it.

**Variants are not entities.** They carry no `entity_id`, appear in no
relationship, and the `Variants` entity group is empty in every bundle. A
variant is identified by `chromosome:position:ref:alt` and nothing else.

They reach genes through the symbol and HGNC id that VEP emitted — a string
match, not a link through the entity graph. So when `expand_gene_to_variant`
asks you to choose between `mapping=position` and `mapping=annotation`, that
is the reason: there is no stored edge saying a variant belongs to a gene,
so you have to say which question you mean.

This is a design decision, not a gap. Variants outnumber every other domain
by three orders of magnitude, and keeping them out of the entity graph is
what lets them be built, stored and queried independently of it.

## Which domains a bundle actually holds

Fourteen entity groups are defined. Far fewer are populated, because a group
is only filled by a source that was built. From the same build:

| Group | Entities |
|---|---:|
| Genes | 72,660 |
| Proteins | 53,296 |
| Gene Ontology | 38,092 |
| Diseases | 36,090 |
| Pathways | 3,255 |
| Chemicals | 0 — ChEBI was not included in this build |
| Variants | 0 — by design, see above |

Epigenomics, Transcriptomics, Metabolomics, Clinical Trials, Microbiome,
Phenotypes and Cell Types are defined and empty. They mark room the model
leaves for domains that have no source behind them yet, and no report will
return anything for them.

**Check before you assume.** The bundle you were given may not carry the
domain your question needs:

```bash
biofilter --bundle <path> report run --report-name platform_data_statistics
```

That reports entity counts by domain for the bundle in front of you, which
is the only authority on what it can answer. The numbers on this page come
from one build and are there to show the shape, not to be quoted.

## See also

- [Report Catalog](report_catalog.md) — which report reads which layer
- [Database Schema](technical/schema.md) — every table and column



<!-- ===== SOURCE FILE: docs/source/getting_started/finding_reports.md ===== -->

# Finding a Report

## What a report is

A **report** is a prepared question you can ask the data.

You give it an input — a list of gene symbols, an rsID, a disease name,
sometimes nothing at all — and it gives you back a table. Under the
covers it knows which files to open, how to resolve the names you typed
against the ones the sources use, and how to follow the links between
genes, proteins, pathways, diseases and variants. You do not write
queries and you do not need to know how the data is laid out.

```bash
biofilter --bundle /shared/bundles/bf4_20260912 \
  report run --report-name resolve_entity --input APOE --input TP53
```

Repeat `--input` for each value — it is not a comma-separated list.

Every report takes the same shape: a name, an input, optional parameters,
and a table out — to your screen, to a file, or straight into pandas if
you are working in Python.

Biofilter ships 16 of them: resolving names, annotating what you have,
expanding it to what is connected, and checking what the bundle actually
holds. Three ways to find the one you want.

## 1. Browse the catalog

The [Report Catalog](../report_catalog.md) is the full index, grouped by
purpose. Each entry gives you:

- A one-line description of what it does.
- A link to its **Explain Guide** — parameters, output columns, examples.
- A link to a **notebook tutorial** that runs end-to-end.

Use the catalog when you want to see everything available.

## 2. Ask the assistant

For questions in plain language — *"I have a list of genes from a GWAS,
which report shows what pathways they touch?"* — there is a GPT assistant
trained on BF4's reports and terminology:

**[BF4 Assistant](https://chatgpt.com/g/g-6887cf80355c8191ab3f88bbd8955e0d-biofilter-4-assistant)**

Its source — system prompt, FAQ, and a manifest of every report with its
inputs and use cases — lives in the repository's `assistent/` folder.

## 3. Ask Biofilter itself

If you already have it installed:

```bash
biofilter report list
```

And for any one of them:

```bash
biofilter report explain --report-name resolve_entity
```

That prints the full guide in your terminal — what it expects, what it
returns, and how to call it.

## Good places to start

**Start from names you have** — genes, variants, diseases, proteins.

| Report | Use it when |
| ------ | ----------- |
| `resolve_entity` | You have a list of names and want to know which ones Biofilter recognises |
| `annotate_gene` | You want everything known about a set of genes |
| `annotate_variant` | You have rsIDs or positions and want the full annotation |

**Expand to what is connected.**

| Report | Use it when |
| ------ | ----------- |
| `expand_gene_to_variant` | You have genes and want the variants in them, filtered by predicted damage |
| `expand_entity_neighborhood` | You have entities and want everything one hop away |
| `expand_variant_regulatory` | You want to know which genes a variant regulates, and in which tissue |

**Work with a cohort or a set.**

| Report | Use it when |
| ------ | ----------- |
| `aggregate_cohort_variants` | You have a cohort's variants and want them matched and binned |
| `pair_variants` | You need candidate variant pairs whose genes share biology |

And one worth running once on any bundle you have just been handed:

| Report | Use it when |
| ------ | ----------- |
| `platform_data_statistics` | You want to know what is actually in this bundle |

It reports entity counts by domain, variant counts by chromosome, and what
each data source contributed — which is what decides whether your question
is answerable at all before you spend time on it.

## Next step

Picked one? [Run your first report](running_reports.md).



<!-- ===== SOURCE FILE: docs/source/getting_started/index.md ===== -->

# Getting Started

Biofilter 4 (BF4) resolves biological entities — genes, proteins,
pathways, diseases, variants — tracks the relationships between them, and
exposes all of it through ready-to-use reports.

What you read is a **bundle**: a directory of parquet files with a
manifest describing them. No database server, no import step. Point
Biofilter at a bundle and run reports.

## Choose your path

### Someone gave me a bundle

This is the common case, and it takes minutes.

1. [Install Biofilter](installing.md) — pip or Docker.
2. [Point at the bundle](reading_a_bundle.md) — one URI, no setup.
3. [Find a report](finding_reports.md) that fits your question.
4. [Run it](running_reports.md) — CLI or Python.

### I need to build a bundle

Only if no one has one for the data you need. The full human genome
means 1.5 TB of downloads, processed and discarded as the build goes, so
plan for **150 GB of working space**.

1. [Install Biofilter](installing.md) — from source if you will change DTPs.
2. [Build a bundle](../technical/building_bundles.md) — `bundle plan`, then `bundle build`.
3. [Run a report](running_reports.md) against what you built.

[What it costs](../technical/bundle_requirements.md) has the measured figures for
disk, memory and runtime before you start.

## What you'll need

- **Python 3.10+**, or **Docker** if you prefer containers.
- **A bundle** — a path you can read, local or on a shared filesystem.

## One thing to carry with you

Ids inside a bundle — `entities.id`, `variant_id` — are internal to that
bundle. They are not stable across bundles, and a stale one still
resolves: to a different gene, without an error. Pin the bundle, not the
id. [Reading a bundle](reading_a_bundle.md) explains how results carry
their origin.

## Where this guide stops

Once you can run a report, the rest goes deeper:

- [Report catalog](../report_catalog.md) — every report, with tutorials.
- [Building bundles](../technical/building_bundles.md) — the plan/build/inspect flow.
- [What a build costs](../technical/bundle_requirements.md) — measured disk, time, memory.
- [The Read Path](../technical/read_path.md) — how views are registered, and what a query costs.
- [Data sources and ingestion](../technical/etl.md) — where the data comes from, and how it gets in.
- [Configuration](../technical/configuration.md) — `.biofilter.toml` options.
- [Troubleshooting](../troubleshooting.md) — common errors.

Running on the Penn LPC? The cluster-specific quickstart and the
maintainer's deployment guide live in the repository at
`notebooks/lpc__quickstart.md` and `notebooks/lpc__deploy.md`.



<!-- ===== SOURCE FILE: docs/source/getting_started/installing.md ===== -->

# Installing Biofilter

Three installation methods, in order of simplicity. Pick **one**.

## Which one should I use?

| Method     | Best for                                           | Requires                |
| ---------- | -------------------------------------------------- | ----------------------- |
| **pip**    | Most users — running reports, notebooks, scripting | Python 3.10+            |
| **Docker** | Avoiding any Python setup, reproducible CI runs    | Docker                  |
| **Source** | Contributors, debugging, modifying BF4 itself      | Python 3.10+ and Poetry |

## pip (recommended)

```bash
pip install biofilter
biofilter --help
```

That's it — `biofilter` is now available as a CLI command and the `biofilter` Python package is importable.

To verify:

```bash
biofilter --help
python -c "from biofilter import Biofilter; print('OK')"
```

## Docker

One image, published to two registries. Pull it rather than building:

```bash
docker pull ricoandre/biofilter:latest
```

The image carries no data. It expects two mounts:

| Mount | Mode | Holds |
|---|---|---|
| `/bundle` | read-only | the bundle directory, the one with `manifest.json` |
| `/workspace` | writable | where `--output` writes |

`BIOFILTER_BUNDLE` already defaults to `/bundle` inside the image, so a
normal run names no paths beyond the mounts:

```bash
docker run --rm \
  -v /shared/bundles/20260914:/bundle:ro \
  -v "$(pwd)/out:/workspace" \
  --user "$(id -u):$(id -g)" \
  ricoandre/biofilter:latest \
  report run --report-name annotate_gene --input TP53 --output /workspace/genes.csv
```

Three things worth knowing:

- **Mount the bundle root**, not its `tables/` subdirectory.
- **`--output` writes inside the container.** Point it at the mounted
  `/workspace` or the file leaves with the container.
- **`--user "$(id -u):$(id -g)"`** makes the output yours. Without it the
  files belong to the image's own user.

An interactive shell:

```bash
docker run --rm -it \
  -v /shared/bundles/20260914:/bundle:ro \
  -v "$(pwd):/workspace" \
  --entrypoint /bin/bash \
  ricoandre/biofilter:latest
```

To build it yourself from a checkout:

```bash
docker build -t biofilter:latest -f docker/Dockerfile .
```

### On a cluster (Apptainer/Singularity)

The same image. `--bind` replaces `-v`, and output ownership takes care of
itself because the container runs as you:

```bash
apptainer pull bf4.sif docker://ghcr.io/ritchielab/biofilter-hpc:latest

apptainer run \
  --bind /shared/bundles/20260914:/bundle:ro \
  --bind ~/bf4_output:/workspace \
  bf4.sif \
  report run --report-name annotate_gene --input APOE --output /workspace/apoe.csv
```

The GHCR name `biofilter-hpc` predates the merge of what used to be two
images; it is the same image as Docker Hub's.

## From source

For contributors or anyone modifying BF4 itself.

```bash
git clone https://github.com/RitchieLab/biofilter.git
cd biofilter
poetry install
poetry run biofilter --help
```

## Next step

Once installed, [point Biofilter at a bundle](reading_a_bundle.md) — one URI, no server to set up.



<!-- ===== SOURCE FILE: docs/source/getting_started/reading_a_bundle.md ===== -->

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



<!-- ===== SOURCE FILE: docs/source/getting_started/running_reports.md ===== -->

# Running Your First Report

Two ways to run any report: from the command line or from Python. Both
produce the same result. Pick whichever fits your workflow.

## CLI — quickest path

List what's available:

```bash
biofilter report list
```

Run a report and print the result to the terminal:

```bash
biofilter report run --report-name annotate_gene --input TP53
```

Save it to a file. The format follows the extension — `.csv` or
`.parquet`:

```bash
biofilter report run --report-name annotate_gene \
  --input TP53 --input BRCA1 \
  --output genes.csv
```

Repeat `--input` for each value. For lists too long for a command line,
put one value per line in a file:

```bash
biofilter report run \
  --report-name annotate_gene \
  --input-file ./genes.txt
```

Options are separate from input, and go through `--param KEY=VALUE`:

```bash
biofilter report run \
  --report-name resolve_entity \
  --input BRCA1 --input TP53 \
  --param match_mode=exact
```

To see what a report accepts:

```bash
biofilter report explain --report-name resolve_entity
biofilter report run --report-name resolve_entity --params-template
```

## Python — best for notebooks and scripts

```python
from biofilter import Biofilter

bf = Biofilter(bundle="/shared/bundles/bf4_20260912")

result = bf.report.run(
    "resolve_entity",
    input_data=["BRCA1", "TP53", "APOE"],
    match_mode="exact",
)

df = result.to_pandas()
print(f"{result.num_rows} rows")
df.head()
```

`bf.report.run()` returns a **result**, not a bare DataFrame. Call
`.to_pandas()` when you want to continue in pandas; the extra layer is
what carries the record of where the rows came from:

```python
result.provenance["bundle_id"]   # which build produced these rows
result.provenance["coverage"]    # what this bundle did not have
result.write("entities.csv")     # also writes entities.csv.provenance.json
```

`Biofilter()` with no argument falls back to `BIOFILTER_BUNDLE` or to
`.biofilter.toml`, so in a configured environment the constructor can stay
empty.

## A complete first example

Install, point, run:

```bash
# Install
pip install biofilter

# Point at a bundle
export BIOFILTER_BUNDLE="/shared/bundles/bf4_20260912"

# See what is in it
biofilter report run --report-name platform_data_statistics \
  --output bundle_contents.csv

# Ask it something
biofilter report run --report-name annotate_gene \
  --input APOE --input TP53 \
  --output genes.csv
```

Open `bundle_contents.csv` first. It tells you which domains and which
chromosomes this bundle actually carries — worth knowing before you
conclude that an empty result means an empty answer.

## Before you trust a result

An empty or partial table has more than one cause, and they are not
interchangeable:

- **A row with a `not_found` status** means the name did not resolve in
  this bundle. `resolve_entity` will tell you what it did match.
- **A `no_variants` status** means the input resolved and nothing met your
  criteria — a real negative.
- **A column that is entirely null** may mean the source was never built.
  `result.provenance["coverage"]` lists what the bundle was missing.

## Next steps

- Browse the [Report Catalog](../report_catalog.md) for what else you can ask.
- Each report has a worked notebook at `notebooks/templates/reports__<name>.ipynb` — copy one and adapt it.
- For every CLI option, see the [CLI Reference](../cli_reference.md).
- For how reports work in general, see [Reports](../reports.md).



<!-- ===== SOURCE FILE: docs/source/index.md ===== -->

# Biofilter Documentation

Biofilter 4 brings genes, variants, proteins, pathways, diseases, ontology
terms and chemicals from many public sources into one model, and lets you
query that model through ready-to-use reports.

What you read is a **bundle**: a directory of parquet files with a manifest
describing them. No database server, no import step. Point Biofilter at a
bundle and run reports.

## Where to start

**You were given a bundle and want answers from it.**
Go to [Getting Started](getting_started/index.md). It takes minutes, and
you will not need the technical section at all.

**You want to know which analyses exist.**
The [Report Catalog](report_catalog.md) lists every report and the question
it answers.

**You build bundles, or extend Biofilter.**
Go to [Technical Reference](technical/index.md).

```{toctree}
:maxdepth: 2
:caption: Getting Started

getting_started/index
getting_started/installing
getting_started/reading_a_bundle
getting_started/finding_reports
getting_started/running_reports
```

```{toctree}
:maxdepth: 2
:caption: Running Analyses

report_catalog
reports
entity_and_omics
cli_reference
troubleshooting
```

```{toctree}
:maxdepth: 2
:caption: Technical Reference

technical/index
```



<!-- ===== SOURCE FILE: docs/source/report_catalog.md ===== -->

# Report Catalog

Every analysis Biofilter can run, organized by the question it answers.
There are 17 reports. Each one takes a list of things you already have —
gene symbols, rsIDs, disease names, a cohort's variants — and returns a
table.

For how reports work in general (parameters, input channels, output
formats), see [Reports](reports.md).

## Find your question

| You have | You want | Report |
|---|---|---|
| Gene symbols or ids | Everything the bundle knows about them | [`annotate_gene`](#annotate-what-you-already-have) |
| Gene symbols | The variants in those genes, filtered by predicted damage | [`expand_gene_to_variant`](#from-genes-to-variants) |
| rsIDs or `chr:pos:ref:alt` | Full annotation, one row per transcript | [`annotate_variant`](#annotate-what-you-already-have) |
| Variants | Which genes they regulate, in which tissue | [`expand_variant_regulatory`](#from-variants-outward) |
| Variants | Plausible interacting pairs, with the biology that links them | [`pair_variants`](#pairs-to-test) |
| Genes | Their variants, then the pairs among those | `expand_gene_to_variant` then [`pair_variants`](#pairs-to-test) |
| Genes | Which of them are related, and by what | [`pair_genes`](#pairs-to-test) |
| Genes, and your own gene-to-anything list | The pairs your list implies | [`pair_genes`](#pairs-to-test) |
| A cohort's variants | Which ones the bundle knows, binned by biology | [`aggregate_cohort_variants`](#a-whole-cohort) |
| Names that are not matching | What they actually resolve to, and where they conflict | [`resolve_entity`](#annotate-what-you-already-have) |
| Any entity list | Its one-hop neighbourhood, or the relationship rows themselves | [`expand_entity_*`](#follow-the-entity-network) |
| Disease, pathway, GO or protein names | Annotation for that domain | [`annotate_*`](#annotate-what-you-already-have) |
| A bundle | What is in it, and how it was built | [`platform_*`](#about-the-bundle-itself) |

## Running a report

```bash
biofilter --bundle /path/to/bundle report run \
  --report-name annotate_gene \
  --input TP53 --input BRCA1 \
  --output genes.csv
```

```python
from biofilter import Biofilter

bf = Biofilter(bundle="/path/to/bundle")
result = bf.report.run("annotate_gene", input_data=["TP53", "BRCA1"])
df = result.to_pandas()
```

Three things worth knowing before you start:

- **`biofilter report explain --report-name <name>`** prints the report's
  full guide — every parameter, every column, and how to read the result.
  It is the authoritative reference; this page is the index.
- **A worked notebook** ships for each report at
  `notebooks/templates/reports__<name>.ipynb`.
- **Options go through `--param KEY=VALUE`**, separately from `--input`.
  Values are coerced: `true`/`false`, numbers, and JSON. A list is written
  as JSON — `--param impact_filter='["HIGH","MODERATE"]'`. Use
  `--params-template` to print every option a report accepts.

## Annotate what you already have

You have identifiers. You want what is known about them.

| Report | Takes | Returns |
|---|---|---|
| `annotate_gene` | Gene symbols, HGNC or Ensembl ids, aliases | Canonical ids, gene metadata, build-38 coordinates, relationship counts by related domain, and optionally how many variants fall in the gene's range |
| `annotate_variant` | rsIDs, `chr:pos`, or `chr:pos:ref:alt` | Identity, gnomAD joint frequencies, in-silico predictions, and one row per transcript the variant was annotated against |
| `annotate_protein` | Accessions, names, aliases | Canonical accession, function, location, tissue expression, isoform resolution, Pfam domains by type |
| `annotate_disease` | Disease names or aliases | Canonical ids, label and description, disease groups, cross-references by source, and the genes ClinGen links to the disease |
| `annotate_pathway` | Pathway names or ids | Canonical id and description, which source contributed it, relationship counts by domain |
| `annotate_go` | GO terms or aliases | GO id, name and namespace, parent and child counts by relation type, relationship counts by domain |
| `resolve_entity` | Any list of names | What each name resolves to, with conflict and status flags |

`resolve_entity` is the one to reach for when another report returns
`not_found` and you want to know why. It supports `match_mode=exact`
(default), `like` for substrings, and `fuzzy` for Jaro-Winkler similarity
above a threshold.

## From genes to variants

**`expand_gene_to_variant`** — the variants that belong to a list of genes.

This is the report for the common screening question: *given these genes,
which variants in them are plausibly damaging and rare enough to matter?*

First you choose what "belongs to" means, because the two answers differ:

| `mapping` | A variant belongs to a gene when… |
|---|---|
| `position` | Its coordinate falls inside the gene's build-38 range |
| `annotation` | VEP associated it with that gene |

Then you filter. Every filter below is optional and they compose:

| Filter | Options |
|---|---|
| `impact_filter` | `HIGH`, `MODERATE`, `LOW`, `MODIFIER` |
| `consequence_type_filter` | VEP consequence names |
| `lof_confidence_filter` | LOFTEE `HC`, `LC` |
| `af_min`, `af_max` | gnomAD joint allele frequency bounds |
| `cadd_phred_min`, `sift_score_max`, `polyphen_score_min` | In-silico predictor thresholds |
| `alphamissense_score_min` | AlphaMissense score |
| `alphamissense_classification` | `likely_pathogenic`, `likely_benign`, `ambiguous` |

A rare, high-impact, likely-pathogenic screen over two genes:

```bash
biofilter --bundle /path/to/bundle report run \
  --report-name expand_gene_to_variant \
  --input BRCA1 --input CHEK2 \
  --param mapping=annotation \
  --param impact_filter=HIGH \
  --param af_max=0.01 \
  --param alphamissense_classification=likely_pathogenic \
  --output candidates.csv
```

Two defaults to be aware of. `most_severe_only` is `true`, so you get one
row per gene and variant keeping the worst consequence — set it to `false`
when you want every transcript. And `max_variants_per_gene` is `5000`; when
a gene is capped, the `variants_available` column reports its pre-cap total,
so a truncated row admits that it is truncated.

## From variants outward

| Report | Answers |
|---|---|
| `expand_variant_regulatory` | Which genes does this variant regulate, and in which tissue? One row per variant × tissue × regulated gene, with effect size and p-value. Takes gene symbols, rsIDs or positions. |

## Pairs to test

Two reports generate candidate pairs, and choosing between them is
choosing **where the link between a variant and a gene comes from**.

| Report | Answers |
|---|---|
| `pair_variants` | Which of these variants plausibly interact? Places each input variant on its genes **by coordinate**, connects those genes through shared pathways, diseases or proteins, and returns the pairs among the variants you named. |
| `pair_genes` | Which of these genes are related, and by what — and, given a gene-to-item list of your own, the item pairs those gene pairs imply. Performs **no** variant-to-gene mapping. |

Use `pair_variants` when the variant belongs to the gene it sits inside.
That is true of a coding variant.

**`pair_variants` takes variants only.** It used to accept gene names and
expand each into the variants inside it, keeping 100 of a gene's ~4,000
by allele frequency without showing you which. Run
`expand_gene_to_variant` first, look at the list, filter it, and pair
that — one visible step instead of one invisible one. `membership` and
`max_variants_per_gene` left with the expansion, and passing either is an
error rather than a silently different answer.

Use `pair_genes` when it does not. A regulatory variant sits in one gene
and acts on another: of the 11,532,453 variant × gene links in this bundle
that carry both kinds of evidence, **91.5% name a gene other than the one
the variant sits in**. If your evidence for the attachment comes from
outside Biofilter — a colocalization, a fine-mapping, a curated
assignment — `pair_variants` cannot use it. It re-derives membership from
coordinates and drops what disagrees, with no error.

`pair_genes` never derives it:

```bash
biofilter --bundle /path/to/bundle report run \
  --report-name pair_genes \
  --input-file my_genes.txt \
  --param gene_identifier=ensembl \
  --param mapping_file=variant_to_gene.tsv \
  --param max_group_size=300 \
  --param min_group_sources=2 \
  --output item_pairs.csv
```

**Say how your genes are identified.** `gene_identifier` is `alias` by
default, which searches every alias — symbols, synonyms, HGNC, Ensembl,
Entrez. Name a code system and the search narrows to it; pass
`entity_id` and it goes to the bundle's own key instead.

This matters more than it looks. 174,410 gene aliases in the bundle are
bare numbers, because that is what an Entrez id is, and **14,335 of those
are also the entity id of a different gene** — Entrez `2` is A2M, entity
`2` is A1BG-AS1. Nothing can tell them apart by looking, so a list from
one source should say which source it came from. It applies to the
mapping as well as the input: one decision about one thing.

The mapping is two columns, gene then item, and the item is **never
read** — which is what lets the same report pair positions, rsIDs, probe
ids or exposures. Biofilter is build 38 and managing build is yours;
because nothing interprets the item, build-37 positions pass through
correctly.

It also owns three rules that are easy to get wrong alone: pairs are
unordered, deduplication is global rather than per gene pair, and an item
attached to both genes does not pair with itself. On one real run the
deduplication alone was 4.3% of the answer.

Both are hypothesis generators, not tests. The pairs are candidates whose
genes share biology, and the supporting columns are there so you can judge
each one — `group_support_count` is a weight for ranking, never a p-value.

## Follow the entity network

| Report | Answers |
|---|---|
| `expand_entity_neighborhood` | What is one hop away from these entities? Takes a mixed list — genes, diseases, proteins — with optional `gene:` style hints, and reports degree overall and by neighbour type. |
| `expand_entity_relationship` | The relationship rows themselves: every link where an input appears on either side, with the related entity named. `scope` controls whether the other side must also be in your input list. |

Use the first to explore, the second to extract.

## A whole cohort

**`aggregate_cohort_variants`** — your cohort's variants, matched against
the bundle and optionally rolled up into biological bins.

It answers three things at once: which of your variants Biofilter knows,
where they sit, and what each sample carries per bin. Binning is optional —
without it you get the match and the placement.

It returns **two tables**: the bins, and the `(variant, bin)` mapping that
says what each bin is made of.

```python
bins = bf.report.run("aggregate_cohort_variants",
                     cohort_file="cohort.vcf.gz", output_grain="bins")

bins.table                            # one row per (bin, sample)
bins.extra_tables["variant_to_bin"]   # what each bin is made of
```

It also writes a `plink --extract` list as an artifact. And it is the report
whose `provenance["warnings"]` most often has something in it — chromosomes
the bundle cannot place, a `maf_cutoff` below what the cohort can observe,
samples with no phenotype. Read them.

## About the bundle itself

| Report | Answers |
|---|---|
| `platform_data_statistics` | What does this bundle hold? Identity, table sizes on disk, entity counts by domain, variant counts by chromosome, relationship counts by group pair, and what each source contributed. |
| `platform_etl_status` | One row per data source: the latest good extract, transform and load, whether each stage ran on the previous one's output, and whether anything is known to be wrong. |
| `platform_etl_packages` | The raw record behind the status: one row per ETL package, with stage, timing, row counts and the hash it carried forward. |

Run `platform_data_statistics` first on any bundle you did not build
yourself. It tells you which chromosomes and which sources are actually in
there, which is what decides whether your question is answerable at all.

It returns **three tables**. The long list of measurements holds most of the
report, but two sections lose the part you would sort by, so they travel as
tables of their own:

```python
stats = bf.report.run("platform_data_statistics")

stats.table                       # the long measurements
stats.extra_tables["storage"]     # table, branch, rows, bytes, files
stats.extra_tables["variants"]    # table, chromosome, rows
```

In the long shape a size is `"3.4 MB"` and a chromosome is a string, so
sorting gives 1, 10, 11, 2. In these two they are integers.

## Reading a result honestly

An empty or partial result has more than one cause, and they are not
interchangeable.

**Per-row status.** Reports that resolve input keep the inputs that
produced nothing, with a status saying why — `not_found` (the name did not
resolve in this bundle), `no_location` (it resolved, but there are no
coordinates for it), `no_variants` (it resolved and nothing met your
criteria). A shorter table is not the same as a negative answer.

**Coverage.** Every result carries a `coverage` block in its provenance
recording which optional tables the bundle did not have and which
chromosomes it spans. If a bundle was built without AlphaMissense, an
AlphaMissense filter silently matches nothing — coverage is where that is
written down.

```python
result.provenance["coverage"]
result.provenance["bundle_id"]
```

**Warnings.** A report that copes with a problem rather than failing records
it, and `provenance["warnings"]` is always present — so an empty list means
nothing went wrong, not that nobody checked.

```python
result.provenance["warnings"]
```

**Bundle identity.** Entity and variant ids are valid only inside the
bundle that produced them. `result.write("out.csv")` saves
`out.csv.provenance.json` beside the file so the result stays traceable to
its build. Pin the bundle, not the id.

**Keeping the whole thing.** `write()` exports one table and flattens what a
spreadsheet cannot hold. For a result you mean to come back to — especially
one of the multi-table reports above — `save()` writes a directory that loses
nothing, and `load()` reads it back:

```python
result.save("./results/cohort_2026_09")
```

See [Reports](reports.md#saving-a-result) for both.



<!-- ===== SOURCE FILE: docs/source/reports.md ===== -->

# Reports

A report takes a list of things you have and returns a table. This page is
how to find one, run it, and read what comes back. For which report answers
which question, see the [Report Catalog](report_catalog.md).

## Every report needs a bundle

Reports read a bundle and nothing else. Point at one in any of these ways —
the first that is set wins:

```bash
biofilter --bundle /path/to/bundles/20260914 report run ...   # flag
export BIOFILTER_BUNDLE=/path/to/bundles/20260914             # environment
```

```toml
# .biofilter.toml — relative to this file, not your working directory
[database]
bundle = "./biofilter_data/bundles/20260914"
```

Discovery is the exception. `report list`, `explain`, `example-input` and
`available-columns` ask about the installed package, not about data, so they
work with no bundle at all.

## Find a report

```bash
biofilter report list
biofilter report list --verbose          # descriptions and module names
```

Then ask a specific report what it does:

```bash
biofilter report explain --report-name expand_gene_to_variant
biofilter report example-input --report-name expand_gene_to_variant
biofilter report available-columns --report-name expand_gene_to_variant
```

`explain` prints the report's full guide — parameters, columns, and how to
read the result. It is the authoritative reference for any single report.

`report refresh` rebuilds the index after you add a report. You will not
need it otherwise.

## Run one

```bash
biofilter --bundle <path> report run --report-name annotate_gene \
  --input TP53 --input BRCA1
```

Write the result to a file with `--output`. The format follows the
extension — `.csv`, or `.parquet` / `.pq`:

```bash
biofilter --bundle <path> report run --report-name annotate_gene \
  --input-file ./my_genes.txt \
  --output genes.parquet
```

From Python:

```python
from biofilter import Biofilter

bf = Biofilter(bundle="/path/to/bundles/20260914")
result = bf.report.run("annotate_gene", input_data=["TP53", "BRCA1"])

df = result.to_pandas()
result.write("genes.csv")
```

## Two channels: input and options

They are separate on purpose, and mixing them is an error rather than a
guess.

**Input** — the records you are asking about. One channel at a time:

```bash
--input TP53 --input BRCA1                      # repeatable
--input-file ./genes.txt                        # one value per line
--input-file ./cohort.csv --input-column symbol # a column of a CSV
```

**Options** — everything that changes behaviour: filters, modes, thresholds.

```bash
biofilter --bundle <path> report run --report-name expand_gene_to_variant \
  --input BRCA1 \
  --param mapping=annotation \
  --param impact_filter=HIGH \
  --param af_max=0.01
```

Values are coerced: `true` / `false`, numbers, and JSON. A list is JSON —
`--param impact_filter='["HIGH","MODERATE"]'`. A leading `@` reads the value
from a file, and `@@` escapes a literal `@`:

```bash
--param consequence_type_filter=@./consequences.txt
```

For anything longer, pass the whole option set at once:

```bash
--params-json '{"mapping":"annotation","af_max":0.01}'
--params-file ./params.yaml          # .json, .yml or .yaml
```

`--params-template` prints the options a report accepts, filled with its own
example values, which is the fastest way to see what is available:

```bash
biofilter report run --report-name expand_gene_to_variant --params-template
```

## What comes back

`bf.report.run()` returns a **result**, not a bare DataFrame. The extra layer
is what carries everything the rows alone cannot say.

```python
result = bf.report.run("annotate_gene", input_data=["TP53", "BRCA1"])

result.num_rows          # rows in the main table
result.columns           # its column names
result.to_pandas()       # a DataFrame, provenance on .attrs
```

### What it holds

| | |
|---|---|
| `result.table` | the main table, as Arrow |
| `result.extra_tables` | further tables, by name — see below |
| `result.tables` | all of them, main one first |
| `result.provenance` | where the rows came from and what happened |
| `result.artifacts` | extra files the report wrote, if any |

### The provenance

```python
result.provenance["report"]             # which report
result.provenance["bundle_id"]          # which build these rows came from
result.provenance["bundle_root"]        # and where it was
result.provenance["params"]             # what was asked
result.provenance["rows"]               # how many came back
result.provenance["generated_at"]       # when
result.provenance["coverage"]           # what the bundle did not have
result.provenance["version_mismatch"]   # None unless built by another release
result.provenance["warnings"]           # what the report coped with, in order
```

`warnings` is always present, so an empty list means "nothing went wrong"
rather than "nobody recorded whether anything did". A report that quietly
works around a problem writes it here, where it reaches whoever opens the
result next month — who never has the log.

### More than one table

Some answers are genuinely two shapes. `platform_data_statistics` returns one
long list of metrics and, beside it, the storage and variant breakdowns that
list cannot hold. `aggregate_cohort_variants` returns the bins and, beside
them, what went into each.

```python
result.tables.keys()              # 'result', then the rest
result.extra_tables["variants"]
```

They are tables rather than files on purpose: a second table stays part of
the result and gets checked with it, where a CSV written off to the side
becomes something the result only names.

## Saving a result

Two verbs, because they answer different questions.

### `write()` — export it

For getting the numbers somewhere else: a spreadsheet, a collaborator, a
plotting script.

```python
result.write("genes.csv")        # also writes genes.csv.provenance.json
result.write("genes.parquet")    # provenance inside the file's metadata too
```

One table — the main one. Nested columns are flattened to JSON strings so a
spreadsheet can hold them, which is **lossy on purpose**. Extra tables and
artifacts are not included.

### `save()` / `load()` — keep it whole

For coming back to it later, or handing the whole answer to someone else.

```python
from biofilter.modules.report.result import ReportResult

result.save("./results/apoe_screen")

later = ReportResult.load("./results/apoe_screen")
later.provenance["bundle_id"]
later.extra_tables
```

`save()` writes a **directory**, not a file: every table as parquet, plus a
`manifest.json`. Nothing is flattened and nothing is left behind. It refuses
to write over a directory that already holds something unless you pass
`overwrite=True`.

The layout is deliberately a bundle's, which means a saved result is not only
reloadable — it is **queryable**:

```python
from biofilter.modules.report.bundle import Bundle

with Bundle.open("./results/apoe_screen") as saved:
    saved.con.execute("SELECT count(*) FROM result").fetchone()
```

### Does the source bundle still exist?

`load()` adds one field the original result did not have:

```python
later.provenance["source_bundle"]
# {'bundle_id': '39c56b50adeb1dc5',
#  'bundle_root': '/path/to/bundles/20260914',
#  'still_present': True,
#  'means': 'The bundle that produced this is where it was, ...'}
```

A saved result is self-contained and does not go looking for its bundle. This
field is there so that "can I go back to the source?" has an answer, rather
than being discovered by opening a path that is gone.

If `still_present` is `False`, the rows are unchanged and still belong to the
build `bundle_id` names. What you lose is the ability to resolve the ids in
them to anything else.

## Two habits worth forming

- **Check `coverage` before trusting a null.** It lists the optional tables
  the bundle lacked and the chromosomes it spans. A column that is null
  because a source was never built looks exactly like one that is null
  because the answer is null.
- **Keep the `bundle_id` with the result.** Entity and variant ids are valid
  only inside the bundle that produced them, so an id without its build is
  not a fact. `write()` puts it in the sidecar and `save()` puts it in the
  manifest — what neither can do is follow a column of ids you pasted into a
  spreadsheet.

## Guides and notebooks

Each report ships two pieces of documentation besides this page:

| | |
|---|---|
| The guide | `biofilter/modules/report/reports_explain/report_<name>.md`, printed by `report explain` |
| A notebook | `notebooks/templates/reports__<name>.ipynb`, a worked example against a real bundle |



<!-- ===== SOURCE FILE: docs/source/technical/building_bundles.md ===== -->

# Building Bundles

A bundle is what Biofilter produces and what it reads: a directory of
parquet files plus a manifest, built once and never changed. This page is
how one gets made.

## Why the build has two branches

Two facts about the data decide the shape of the build.

**The relational core is small and needs transactions.** Entities, aliases,
relationships, genes, proteins, pathways, diseases, GO and chemicals come to
about 7 million rows and 105 MB. They also resolve against each other —
creating an entity means asking what earlier sources already created — so
they need a transactional store while they are being assembled.

**Variant data is large and needs none of that.** It runs to roughly
2 billion rows. `variant_masters` carries no `entity_id` and no foreign
keys; variants link to genes through natural keys that VEP emitted, so
there is nothing to resolve. At about 4 bytes per row in parquet the whole
variant set is 15.6 GB — the same table in a row store costs about 255 bytes
per row, which puts it in the hundreds of gigabytes.

So the build splits. The **core branch** stages through a throwaway SQLite,
written through the ORM models. The **variant branch** writes its final
parquet directly, one file per chromosome, with no relational hop and no
load step. The branches do not depend on each other.

## Before the first build

`bundle plan` reads the list of data sources from a database, so you need
one before you can write a plan:

```bash
biofilter db create-db --db-uri sqlite:///biofilter_data/registry.sqlite
```

This creates the schema and applies the JSON seeds, including the data
source registry the plan is generated from. It is a registry, not a data
store — the build creates its own staging database and never writes here.

## The three commands

```bash
biofilter bundle plan  --out bundle_plan.json --db-uri sqlite:///biofilter_data/registry.sqlite
biofilter bundle build --plan bundle_plan.json
biofilter bundle info  ./biofilter_data/bundles/20260914
```

### plan

Writes the recipe. Every data source appears with an `include` flag, its
DTP and version, and the path of that DTP's field or tissue config, split
into the two branches.

Edit the flags to choose what the build covers. `--all-sources` enables
every source rather than only the ones currently flagged active — note that
this covers the whole genome, and the gnomAD download alone is about 1.5 TB.

**Order matters.** Sources run in the order they appear, and that order is
the dependency declaration — the core branch resolves entities against what
earlier sources created, so `hgnc` precedes `gene_ncbi`, which precedes
`ensembl`. Reordering the list reorders the build.

The plan is authoritative for one build. The `active` flag in the registry
only seeds a new plan's defaults, and each DTP's own JSON config still
governs what is selected *within* a source — which INFO fields, which GTEx
tissues.

Writing over an existing plan needs `--force`, because a plan may be the
only record of how a published bundle was made.

### build

```bash
biofilter bundle build \
  --plan bundle_plan.json \
  --data-root biofilter_data \
  --out ./bundles/20260914
```

Creates `<data-root>/staging/bundle_staging.sqlite`, runs every included
source against it, reclaims disk as it goes, and assembles only once all of
them have succeeded.

| Option | Default | Does |
|---|---|---|
| `--plan` | `bundle_plan.json` | The plan to build from |
| `--data-root` | `biofilter_data` | Where raw, processed and staging live |
| `--out` | `<data-root>/bundles/<YYYYMMDD>` | Where to write the bundle |
| `--restart` | off | Discard the staging database and start over |
| `--keep-raw` | off | Keep downloads after their output exists |
| `--min-free-gb` | `100` | Refuse to start a source below this much free space |
| `--into` | — | Fold this run's variant output into an existing bundle |
| `--keep-processed` | off | Copy the variant parquet in rather than moving it |
| `--no-assemble` | off | Run the sources but do not publish |

**Resume is the default.** An interrupted build re-runs only what is
pending: finished sources are skipped, both because their steps are recorded
in the staging ledger and because the output they left is still there.
`--restart` discards the staging database and starts over — needed when a
source that loaded partially has to be excluded, since resuming would leave
its rows in place.

**Disk is the binding constraint.** A full gnomAD download is about 1.53 TB
while the largest single chromosome is about 126 GB, which only fits because
raw files are dropped as soon as their parquet exists. What gets dropped
differs per branch: the variant branch keeps its parquet — it is the
artifact — and drops the raw VCFs; the core branch drops both once its rows
are in the staging database. `--keep-raw` disables this. `--min-free-gb`
stops a source from starting into a disk that cannot hold it; one gnomAD
chromosome needs up to 67 GB of raw before its parquet exists.

Sources run one at a time. That is a requirement, not a simplification:
running chromosomes concurrently multiplies the peak disk footprint, and
downloads gain nothing from concurrency — four parallel range streams
measured 66.6 MB/s against 64.2 MB/s for one, because the local link
saturates.

**Nothing is published unless every source succeeded.** A bundle missing a
table is indistinguishable from a complete one to whoever reads it. A failed
build stays resumable with its finished work intact.

### Building a genome in stages

A full variant branch is days of work and more raw input than most machines
hold at once. `--into` lets it be done in stages, each one folding its
chromosomes into a bundle that already exists:

```bash
# first stage publishes the bundle, core branch included
biofilter bundle build --plan plan_chr1_4.json --out ./bundles/20260914

# later stages fold their variant output in
biofilter bundle build --plan plan_chr5_8.json --into ./bundles/20260914
```

The core is not touched after the first assembly — it came out of the
staging database then and has not changed. Only the variant branch is
folded in.

Three things to know about a staged build:

- **The bundle id changes every time.** It is derived from content, and the
  content grew. That is the honest outcome and also the cost: a result
  already stamped with the old id now names a bundle that no longer exists.
  `build_record.json` gains a `merges` entry per fold, recording
  `bundle_id_before` and `bundle_id_after`, so the sequence can be read
  back.
- **It refuses to replace a file the bundle already has.** Re-folding the
  same chromosome would silently double its rows, and deciding which copy is
  right is not something the build can know.
- **The guards still apply.** A planned source that produced nothing stops
  the merge, and nothing is moved until that check passes — a collision
  found halfway would leave the bundle holding part of a run.

Two options exist for the same workflow. `--no-assemble` runs the sources
without publishing, for when assembling early would produce a bundle holding
only what has run so far. `--keep-processed` copies the variant parquet into
the bundle instead of moving it, so `processed/` still holds it afterwards —
useful for building a chromosome-subset bundle to develop against without
consuming the files the eventual full bundle needs.

### info

Prints what a bundle declares about itself, which after its sources have
moved on is the only surviving account of what it holds:

```
Bundle id:      39c56b50adeb1dc5
Biofilter:      4.3.0
Schema:         4.3.0
Built:          2026-09-14T21:28:42+00:00
Built from:     sqlite
Tables:         103
  core        30 table(s)       5,869,254 rows       71.9 MB
  variant     73 table(s)   1,313,355,486 rows   10,057.3 MB
Plan:           bundle_plan.json
Build record:   build_record.json
```

## Reading what you built

```bash
biofilter --bundle ./bundles/20260914 report list
```

```python
from biofilter import Biofilter

bf = Biofilter(bundle="./bundles/20260914")
result = bf.report.run("annotate_gene", input_data=["TP53"])

result.provenance["bundle_id"]   # which build these rows came from
result.write("genes.csv")        # writes genes.csv.provenance.json beside it
```

Opening a bundle warns if a table it carries is missing columns this build
expects, naming them. It does not refuse: a bundle built from a subset of
the sources legitimately has fewer tables, and refusing would make it
unusable for the ones it does have. For a strict check suitable for gating:

```bash
biofilter db verify --in ./bundles/20260914 --schema
```

See [The Read Path](read_path.md) for what happens between opening a
directory and getting rows back.

## Two properties to know about

### Ids are internal to one bundle

`entities.id`, `variant_masters.variant_id` and every other surrogate are
row identifiers **valid only inside the bundle that produced them**. They
are not stable across builds and no attempt is made to make them so.

This matters because the drift is small. Comparing two environments, `APOE`
was 11448 in one and 11450 in the other — and 11450 in the first is `APOF`,
a different gene in the same family. Nothing errors; the answer is simply
wrong.

Pin the bundle, not the id. Every report result carries `bundle_id` in its
provenance, and writing a result saves that provenance beside the file, so
an orphaned id can be recognised as one.

Cross-domain links use natural keys instead — `chromosome:position:ref:alt`
for variants, `HGNC_ID` and gene symbols for genes.

### A bundle cannot be rebuilt

Sources move on. Ensembl publishes a new release and the previous file stops
being served; gnomAD versions its callsets independently. Building the same
plan a year later produces different data.

That is expected rather than a defect. When a source changes you build a
**new** bundle, and the old one remains a snapshot of a moment that can no
longer be recreated. Reproducibility lives in the retained artifact, not in
the ability to rebuild it — which is why bundles are archived, and why the
manifest, the plan and the build record travel inside the bundle. They are
the only account of it that survives.



<!-- ===== SOURCE FILE: docs/source/technical/bundle_requirements.md ===== -->

# What Building a Bundle Costs

Measured on a full human-genome build completed 2026-09-12: 66 data
sources, the gnomAD v4 joint and VEP callsets for all 24 chromosomes,
plus AlphaMissense, GTEx and GWAS. Chemicals (ChEBI) were excluded.

These are figures from that run, not estimates, except where marked.

## Disk

| | |
| --- | --- |
| **Peak usage** | **~70 GB** |
| Raw downloaded and discarded | ~1.57 TB |
| Final bundle | 21 GB |
| Staging database | 0.9 GB |

The peak is **not** the total downloaded. Raw files are deleted as soon
as the parquet that supersedes them exists, so what has to fit at once is
the largest single source plus what has already been produced:

```
gnomad_joint_chr1 raw        67 GB   ← the largest single download
parquets produced so far      2 GB
staging database              1 GB
                            ──────
                             70 GB
```

That peak occurs early, on chromosome 1. Later sources are smaller while
the accumulated parquet grows, and the two never coincide badly: the
second-highest point is assembly, at about 43 GB.

Without the discard the same build needs the full 1.57 TB. This is why
`bundle build` reclaims per source and why it refuses to start one below
a free-space floor (`--min-free-gb`, default 100 GB).

### What to provision

| | |
| --- | --- |
| **Working space during a build** | **150 GB free** |
| **Storage per retained bundle** | **30 GB** |

150 GB is about twice the measured peak. It also keeps the default
`--min-free-gb` floor of 100 GB workable: the floor is checked before
each source starts, and between sources the space comes back, because raw
is discarded while only parquet accumulates. At the last source there is
still around 129 GB free.

Provisioning exactly 100 GB would not work — the build would refuse to
start, since free space at the peak drops below its own floor.

30 GB per bundle rather than the 21 GB this one occupies: a later gnomAD
release or adding ChEBI moves that number, and bundles are retained
rather than rebuilt, so archive storage is *N* × 30 GB for however many
you keep.

## Time

Wall-clock across the run was 64.5 hours, but that includes gaps between
stages. Summing the steps themselves:

| step | hours | what dominates |
| ---- | ----- | -------------- |
| extract | 6.5 | network: 1.57 TB at ~64 MB/s |
| transform | 40.3 | parsing VCF and writing parquet |
| **load** | **2.3** | only the core branch touches SQLite |
| **total** | **49.1** | |

**Plan for about two days**, serial. The build runs one source at a time
by design: concurrency multiplies the peak disk footprint that the
discard exists to control, and downloads gain nothing from it — four
parallel range streams measured 66.6 MB/s against 64.2 MB/s for one,
because the local link saturates.

Transform is the target for anything faster, not download or load.

## Memory

**Provision 32 GB.**

That is close to what was validated rather than an extrapolation: the
machine that ran this build has 36 GB and showed no pressure. The peak
itself was **not measured**, so 32 GB is a safe recommendation, not a
derived requirement — a smaller machine may well be enough, and that is
worth instrumenting on a future build.

What is known about the shape: transforms stream in batches of 250,000
rows and hold one writer per chromosome, so memory does not scale with
chromosome size. The exception is the rsID map's deduplication pass,
which hands a whole chromosome's file to DuckDB — on chr1 that is a
larger working set than anything else in the build.

## What you get

```
bundle/
  manifest.json          the dictionary BF4 reads
  bundle_plan.json       which sources were included, at which versions
  build_record.json      per-step timings, source URLs, content hashes
  tables/
    entities.parquet             ┐
    gene_masters.parquet         │ core: 41 files, one per table
    entity_relationships.parquet ┘
    variant_masters/
      variant_masters_chr1.parquet … chr24.parquet
    variant_molecular_effects/
    variant_rsid/
    variant_alphamissense/
    variant_gtex/
    variant_gwas.parquet
```

From the measured build:

| | |
| --- | --- |
| Files | 114 parquet |
| Rows | 3,135,885,652 |
| Size | 21 GB |
| Core | 41 tables, 5.87 M rows, 72 MB |
| Variant | 73 tables, 3.13 B rows, 21.2 GB |

`manifest.json` is what makes the directory a bundle rather than a pile
of files. It names every table, its row count and size, which branch
produced it, and carries a `bundle_id` derived from that content — so the
id can be recomputed to check the bundle has not changed.

`bundle_plan.json` and `build_record.json` travel with it because **a
bundle cannot be rebuilt**. Ensembl publishes a new release and the file
the build read stops being served; gnomAD versions its callsets
independently. Running the same plan a year later produces different
data. These files are the only surviving account of what a given bundle
holds, which is why they sit inside it.

## Reading it

No import, no database:

```bash
biofilter --bundle /path/to/bundle report list
```

```python
from biofilter import Biofilter

bf = Biofilter(bundle="/path/to/bundle")
result = bf.report.run("platform_data_statistics")

print(result.provenance["bundle_id"])
```

## One number worth keeping in mind

The same data is 21 GB as parquet and would be several hundred GB in
PostgreSQL — `variant_molecular_effects` alone costs 255 bytes per row
there against 4 in parquet. That ratio is why the build produces a bundle
instead of loading a database, and it is what makes a genome fit on a
laptop.



<!-- ===== SOURCE FILE: docs/source/technical/configuration.md ===== -->

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

`config init` accepts `--db-uri` and `--data-root` to pre-fill the
template.

## Typical keys

| Key | For |
|---|---|
| `database.bundle` | Where reports read from. A directory. |
| `database.db_uri` | Where the ETL and `bundle plan` write. A SQLAlchemy URI. |
| `etl.data_root` | Where raw, processed and staging live during a build. |

## Accepted `database.db_uri` values

| Scheme | Example | Writes |
|---|---|---|
| SQLite | `sqlite:///biofilter_dev.db` | yes |
| PostgreSQL | `postgresql+psycopg2://user:pass@host:5432/biofilter_dev` | yes |
| Parquet bundle | `parquet:///path/to/bundle` | no |

The `parquet://` scheme is a shorthand the database layer understands for
"read this bundle". Prefer `database.bundle` for reading: it is the same
target expressed as a directory, it resolves relative to this file, and it
is what `--bundle` sets. See [The Read Path](read_path.md).

## Tips

- Prefer `--db-uri` in CI or one-off commands.
- Prefer `DATABASE_URL` in containers and orchestrators.
- Prefer `.biofilter.toml` for local development defaults.



<!-- ===== SOURCE FILE: docs/source/technical/index.md ===== -->

# Technical Reference

How Biofilter is built and how it works internally. You need this section
if you **build bundles**, **add a data source or a report**, or need to
know exactly what the data means at the table level.

If you were given a bundle and want to run analyses on it, you do not need
anything here — start at [Getting Started](../getting_started/index.md).

```{toctree}
:maxdepth: 1

system_overview
building_bundles
bundle_requirements
etl
database
schema
read_path
developer_extensions
configuration
```

## What is where

| Page | Answers |
|---|---|
| [System Overview](system_overview.md) | How the whole system fits together, in one diagram |
| [Building Bundles](building_bundles.md) | `plan` → `build` → `info`, and what each step guarantees |
| [Bundle Requirements](bundle_requirements.md) | Disk, time and network before you start a build |
| [ETL Operations](etl.md) | Running one source: update, status, restart, rollback |
| [Database Operations](database.md) | Create, verify, export, import, backup |
| [Database Schema](schema.md) | Every table and column, with the ER diagram |
| [The Read Path](read_path.md) | How a bundle becomes queryable, and what is checked on the way |
| [Developer Extensions](developer_extensions.md) | Adding a DTP or a report |
| [Configuration](configuration.md) | Settings, precedence, environment variables |



<!-- ===== SOURCE FILE: docs/source/technical/read_path.md ===== -->

# The Read Path

Reports do not connect to a database. They open a directory.

This page is the mechanism between `Bundle.open()` and a result row: how a
bundle becomes queryable, what is checked on the way, and what the read
layer deliberately cannot do. For the practical setup — pointing Biofilter
at a bundle and running something — see
[Pointing Biofilter at a bundle](../getting_started/reading_a_bundle.md).

## Opening a bundle

```python
from biofilter.modules.report.bundle import Bundle

bundle = Bundle.open("/path/to/bundles/20260914")

bundle.bundle_id                      # which build this is
bundle.tables["variant_masters"].rows # what it declares
bundle.has("variant_gtex")            # whether a source made it in
```

Six things happen, in this order:

1. **Read `manifest.json`.** If it is not there, this is not a bundle and
   the error says so — including the most common cause, which is pointing at
   the `tables/` subdirectory instead of the bundle root.
2. **Check the manifest version.** This release reads `manifest_version: 2`.
   A bundle declaring anything else is refused rather than guessed at.
3. **Resolve logical tables** from the manifest entries.
4. **Verify the files** — every one the manifest declares is present, at the
   size it recorded.
5. **`duckdb.connect()`** — in the same process. No server, no port, no
   daemon.
6. **Register one view per logical table.**

What is *not* created is as much the point: no `Engine`, no `sessionmaker`,
no connection pool, no `Session`, no ORM. The read layer has none of that
machinery because it needs none of it.

Most code reaches this through the facade rather than directly:

```python
from biofilter import Biofilter

bf = Biofilter(bundle="/path/to/bundles/20260914")
result = bf.report.run("annotate_gene", input_data=["TP53"])
```

## The manifest is the catalogue

Views are built from what the manifest declares, not from what is on disk.

Each manifest entry names a file. A single-file table names only itself; a
partitioned table declares one entry per file, each carrying a `table` field
naming the parent it belongs to. Entries are grouped by that field, and each
group becomes one view:

```sql
CREATE OR REPLACE VIEW variant_masters AS
SELECT * FROM read_parquet([ ...the files the manifest names... ],
                           union_by_name = true);
```

So `variant_masters` is one queryable name whether it is one file or
twenty-five, and no filename convention has to be interpreted to work that
out.

This matters more than it looks. Inferring table membership from filenames
is what once let an empty parent file shadow 177 million rows of real data —
the same defect, independently, in two code paths. Reading the manifest
removes that whole class of error rather than patching its symptoms.

## Verification is tiered

Hashing 21 GB on every `report run` is not an option, so the checks are
split by cost:

| Tier | Checks | Cost | When |
|---|---|---|---|
| Open | Every declared file is present, at the declared size | ~100 `stat()` calls | Every `Bundle.open()`, by default |
| `db verify` | Adds SHA-256 re-computation | Reads every byte | On demand |
| `db verify --schema` | Adds column-level comparison against the models | A metadata read | Gating, CI |

```bash
biofilter db verify --in ./bundles/20260914
biofilter db verify --in ./bundles/20260914 --schema
```

Two notes on the middle tier. `--no-hashes` skips the re-computation. And
the check only runs where the manifest actually recorded a digest — bundles
produced by `bundle build` currently record size but not SHA-256, so for
those the hash tier is a no-op and `verify` is checking presence and size.

The `--schema` tier exists because opening a bundle only *warns* when a
table is missing columns this build expects. That is deliberate: a bundle
built from a subset of sources legitimately has fewer tables, and refusing
to open it would make it useless for the sources it does have. `--schema`
turns the warning into an error, for when you need a gate.

## One cursor per report

Each report execution gets `bundle.cursor()` — its own connection over the
same database.

Views are catalog objects, so every cursor sees them. Temp tables and
registered relations are connection-scoped, so they do not leak between
cursors. That is what gives one report execution a private scratch space,
and it is why two reports running in the same process cannot collide on a
temp table name.

Report input arrives the same way: registered as a relation on the cursor
and joined, never interpolated into the SQL. That is both what keeps
injection out and what turns a ten-thousand-value filter into a hash join
instead of a literal list.

## Tuning

```python
bundle = Bundle.open(path, threads=6, memory_limit="12GB")
```

`memory_limit` is a declared ceiling, and DuckDB respects it by spilling
rather than by failing. The read path also sets
`preserve_insertion_order = false`: results are assembled by joins and
ordered explicitly when order matters, so letting DuckDB drop that guarantee
is a straight memory saving on large scans.

## What to expect

A production-scale question, measured on a 21 GB bundle with 114 declared
tables (macOS, 6 threads, `memory_limit = 12GB`):

> Which of a cohort's 711,836 variants map to a protein-coding gene?
> Against `variant_masters` (177,520,333 rows) and
> `variant_molecular_effects` (2,238,929,441 rows).

| Step | Time |
|---|---:|
| Parse input | 0.2 s |
| Coding genes | 0.0 s |
| Match `variant_masters` (177 M) | 2.5 s |
| Join `variant_molecular_effects` (2.2 B) | 8.9 s |
| Coding filter and aggregate | 0.1 s |
| **Total** | **11.9 s** |

Peak resident memory was 1.87 GB against the declared 12 GB ceiling. Queries
stream from disk with column pruning and predicate pushdown, so memory
tracks the shape of the result rather than the size of the table.

Shared network storage costs roughly an order of magnitude in wall clock
against local NVMe and still lands inside interactive range.

## Writing is not possible

Not "blocked" — absent. The read layer opens parquet and has no write path
to disrespect, so there is no flag to set and no mode to get wrong.

Refreshing data means producing a **new** bundle; see
[Building Bundles](building_bundles.md). Deployments that want the guarantee
visible on disk as well typically make the bundle directory non-writable
(`chmod -R a-w`).

## Querying a bundle without Biofilter

A bundle is parquet files and a JSON catalogue, so anything that reads
parquet can read it:

```sql
-- single-file table
SELECT * FROM read_parquet('/path/to/bundle/tables/gene_masters.parquet')
LIMIT 5;

-- partitioned table: pass the files together
SELECT chromosome, count(*)
FROM read_parquet('/path/to/bundle/tables/variant_masters/*.parquet',
                  union_by_name = true)
GROUP BY 1 ORDER BY 1;
```

One caution: read `manifest.json` to learn which files make up a table
rather than trusting a glob. The manifest is the only authority on that, and
the bugs that come from guessing are silent ones.

## Two parquet readers, and which is which

There are two in the codebase, and they are not interchangeable:

| | `Bundle` | `Database` with a `parquet://` URI |
|---|---|---|
| Where | `modules/report/bundle.py` | `modules/db/database.py` |
| Builds views from | `manifest.json` | A directory scan |
| Stack | DuckDB directly | SQLAlchemy over an in-memory DuckDB |
| Used by | Every report | `db verify --schema`, `bf.db` helpers |

Reports never touch the second one. `Biofilter(bundle=...)` stores the path
as a `parquet://` URI internally, but the report component resolves it back
to a directory and calls `Bundle.open()` on it.

## Troubleshooting

**`No manifest.json in <path>`**
The path is not a bundle root. Point at the directory that holds
`manifest.json`, not at its `tables/` subdirectory.

**`<name> declares manifest_version N; this Biofilter reads [2]`**
The bundle was written by a newer Biofilter. Update the package — the bundle
is fine and needs no migration.

**`<name> does not match its manifest`**
A declared file is missing or the wrong size. The message names the files.
An incomplete copy is the usual cause; re-sync the directory.

**`<name> does not carry: <tables>`**
The bundle was built without the sources that report needs. Run
`platform_data_statistics` to see what it does have.

**A column is all null**
Check `result.provenance["coverage"]`. It records which optional tables the
bundle did not have — a column that is null because a source was never built
looks exactly like one that is null because the answer is null.

## See also

- [Pointing Biofilter at a bundle](../getting_started/reading_a_bundle.md) — practical setup
- [Building Bundles](building_bundles.md) — where bundles come from
- [Database Operations](database.md) — `verify`, `export`, `import`
- [Configuration](configuration.md) — how the bundle path is resolved



<!-- ===== SOURCE FILE: docs/source/troubleshooting.md ===== -->

# Troubleshooting

## Report Not Found

- Run `biofilter report list`.
- Use `--report-name` with one of the listed names.

## Input Conflict in `report run`

If you pass `--input`/`--input-file`, do not also pass input keys through params (`input_data`, `items`, `input_path`).

## Explain Page Not Found

- Check if guide exists at `biofilter/modules/report/reports_explain/report_<module>.md`.
- If missing, Biofilter will fall back to class `explain()`.

## Bundle Will Not Open

`No manifest.json in <path>` — the path is not a bundle root. Point at the
directory that holds `manifest.json`, not at its `tables/` subdirectory.

`<name> declares manifest_version N; this Biofilter reads [2]` — the bundle
was written by a newer Biofilter. Update the package; the bundle needs no
migration.

`<name> does not match its manifest` — a declared file is missing or is the
wrong size, and the message names which. An incomplete copy is the usual
cause; re-sync the directory and check with:

```bash
biofilter db verify --in ./bundles/<YYYYMMDD>
```

## A Report Says the Bundle Does Not Carry Something

`<name> does not carry: <tables>` — the bundle was built without the sources
that report needs. See what it does have:

```bash
biofilter --bundle ./bundles/<YYYYMMDD> report run \
  --report-name platform_data_statistics
```

## A Column Is All Null

Check `result.provenance["coverage"]`. It records which optional tables the
bundle lacked. A column that is null because the source was never built looks
exactly like one that is null because the answer is null, and only coverage
tells them apart.

## A Write Command Fails Against a Bundle

Expected. A bundle is read-only and the read layer has no write path at all.
Producing new data means building a new bundle — see
[Building Bundles](technical/building_bundles.md). There are no in-place
migrations: a schema change produces a new bundle, not an upgraded one.

To check an existing bundle against the schema this build expects:

```bash
biofilter db verify --in ./bundles/<YYYYMMDD> --schema
```

## ETL Batch Resume

If `etl update-all` was interrupted, run it again. Successful data sources are skipped.

## Report Output Not Found (Docker)

`--output` writes inside the container. Mount a host directory and write
to that path, or the file leaves with the container:

```bash
docker run --rm \
  -v /shared/bundles/20260914:/bundle:ro \
  -v "$(pwd)/out:/workspace" \
  ricoandre/biofilter:latest \
  report run --report-name platform_etl_status --output /workspace/etl_status.csv
```

## Output Files Owned by the Wrong User (Docker)

The image runs as its own user, so files it writes belong to that uid. Add
`--user "$(id -u):$(id -g)"` to get your own. Under Apptainer this does not
arise — the container runs as the invoking user.

## Warning: Bundle Built by a Different Biofilter

```
UserWarning: <bundle> was built by Biofilter 4.2.0; this is 4.3.0.
```

The bundle opens and most reports behave. The warning exists because a
column that changed meaning between releases will not announce itself.
Check the bundle against this install:

```bash
biofilter db verify --in /path/to/bundle --schema
```

Every result records this under `provenance["version_mismatch"]`, so a
result produced across a version boundary can be recognised later.
