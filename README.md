# Biofilter 4

**Biofilter 4** is a persistent, entity-centric biological knowledge platform.
It brings genes, variants, proteins, pathways, diseases, ontology terms and
chemicals from many curated sources into one model, and lets you query that
model through ready-to-use reports.

📚 **Documentation:** https://biofilter.readthedocs.io/en/latest/

---

## What you actually work with

A **bundle**: a dated directory of Parquet files with a `manifest.json`
describing them.

```
20260914/
├── manifest.json        the catalogue: every file, its rows, its size
├── bundle_plan.json     which sources went in, at which versions
├── build_record.json    what each build run did, in order
└── tables/              the data
```

Biofilter opens that directory and queries it with DuckDB **in your own
process** — no database server, no import step, no per-user copy. One bundle
on shared storage serves any number of concurrent readers, and it is read-only
by construction.

A bundle is never updated in place. When sources move on, a **new** bundle is
built, and the old one stays readable for anyone reproducing work against it.

---

## 🚀 Quick Start

```bash
pip install biofilter
```

Point at a bundle someone gave you, and ask it something:

```bash
export BIOFILTER_BUNDLE=/path/to/bundles/20260914

# What is in this bundle?
biofilter report run --report-name platform_data_statistics --output contents.csv

# Annotate a gene list
biofilter report run --report-name annotate_gene \
  --input TP53 --input BRCA1 \
  --output genes.csv
```

`--input` repeats — there is no comma-separated form. For long lists use
`--input-file genes.txt`.

From Python:

```python
from biofilter import Biofilter

bf = Biofilter(bundle="/path/to/bundles/20260914")

result = bf.report.run("annotate_gene", input_data=["TP53", "BRCA1"])

df = result.to_pandas()
result.provenance["bundle_id"]   # which build these rows came from
result.write("genes.csv")        # writes genes.csv.provenance.json beside it
```

👉 Full walkthrough: [Getting Started](https://biofilter.readthedocs.io/en/latest/getting_started/index.html)

### Don't have a bundle yet?

Ask whoever maintains one for your group — that is how most people get one.

Building your own is a maintainer task rather than a setup step. See
[Building Bundles](https://biofilter.readthedocs.io/en/latest/technical/building_bundles.html)
for the procedure, and
[what it costs](https://biofilter.readthedocs.io/en/latest/technical/bundle_requirements.html)
for the disk, time and memory it takes.

---

## Two sides, one artifact

Biofilter has two halves, separated by the bundle rather than by an API.

| | Builds a bundle | Reads a bundle |
|---|---|---|
| Who | whoever maintains the data | everyone else |
| How often | a few times a year | constantly |
| Engine | SQLAlchemy models, staging SQLite | DuckDB, in process |
| Can write | yes — it is the only code that can | no, structurally |
| Commands | `bundle`, `etl`, `db` | `report` |

The build runs in **two branches**. The core — genes, proteins, pathways,
diseases, GO, chemicals and their relationships — is about 7 million rows and
needs transactions, because resolving an entity means asking what earlier
sources created. The variant branch is billions of rows with no entity ids and
no foreign keys, so there is nothing to resolve and nothing to stage: it writes
its final Parquet directly.

📖 [System Overview](https://biofilter.readthedocs.io/en/latest/technical/system_overview.html)
 · [The Read Path](https://biofilter.readthedocs.io/en/latest/technical/read_path.html)

---

## 🧬 Reports

A report takes a list of things you have and returns a table. Seventeen of
them, in six families:

| Family | What they answer |
|---|---|
| `resolve_*` | Which of my names does Biofilter recognise? |
| `annotate_*` | What is known about these genes, variants, proteins, pathways, diseases, GO terms? |
| `expand_*` | What is connected to them — neighbours, relationships, variants in a gene, regulatory evidence? |
| `pair_*` | Which of these genes or variants share biology, and on what support? |
| `aggregate_*` | Roll a cohort's variants up to a summary. |
| `platform_*` | What does this bundle contain, and how was it built? |

```bash
biofilter report list --verbose
biofilter report explain --report-name expand_gene_to_variant
```

📋 [Report Catalog](https://biofilter.readthedocs.io/en/latest/report_catalog.html)
— organized by the question you arrive with.

### 📓 Notebook tutorials

One per report, runnable against a bundle:

| | |
|---|---|
| [`reports__101`](notebooks/templates/reports__101.ipynb) | **start here** — how reports work |
| [`reports__resolve_entity`](notebooks/templates/reports__resolve_entity.ipynb) | start here for a new input list |
| [`reports__annotate_gene`](notebooks/templates/reports__annotate_gene.ipynb) | annotating a gene list |
| [`reports__expand_gene_to_variant`](notebooks/templates/reports__expand_gene_to_variant.ipynb) | variants in a gene, filtered by predicted damage |
| [`reports__aggregate_cohort_variants`](notebooks/templates/reports__aggregate_cohort_variants.ipynb) | a cohort, matched and binned |
| [`reports__TEMPLATE`](notebooks/templates/reports__TEMPLATE.ipynb) | copy this to write a new report |

All of them: [`notebooks/templates/`](notebooks/templates/)

---

## Reading a result honestly

A short table is not the same as a negative answer, and this is the part users
do not think to ask about.

```python
result.provenance["coverage"]         # optional tables the bundle lacked
result.provenance["warnings"]         # what the report coped with
result.provenance["bundle_id"]        # which build produced these rows
```

- **A `not_found` status** means the name did not resolve; **`no_variants`**
  means it resolved and nothing matched — a real negative.
- **A column that is entirely null** may mean the source was never built into
  this bundle. `coverage` is where that is written down.
- **Ids belong to one bundle.** They are not stable across builds, and the
  drift is small enough to be dangerous: a stale id still resolves, to a
  different gene, with no error. Pin the bundle, not the id.

Some reports answer in more than one shape — `result.extra_tables` holds the
rest. `result.save(dir)` keeps everything; `result.write(path)` exports one
table and is lossy on purpose.

📖 [Reports](https://biofilter.readthedocs.io/en/latest/reports.html)

---

## 🐳 Containers

One image, published to Docker Hub and to GHCR. It carries no data: bind the
bundle read-only at `/bundle` and a writable `/workspace` for output.

```bash
docker run --rm \
  -v /path/to/bundles/20260914:/bundle:ro \
  -v "$PWD/out:/workspace" \
  --user "$(id -u):$(id -g)" \
  ricoandre/biofilter:latest \
  report run --report-name annotate_gene --input TP53 --output /workspace/genes.csv
```

On a cluster, the same image under Apptainer:

```bash
apptainer pull bf4.sif docker://ghcr.io/ritchielab/biofilter-hpc:latest

apptainer run \
  --bind /project/shared/bundles/20260914:/bundle:ro \
  --bind ~/out:/workspace \
  bf4.sif \
  report run --report-name annotate_gene --input APOE --output /workspace/apoe.csv
```

`--output` writes inside the container, so it has to point at `/workspace`.

📖 [docker/README.md](docker/README.md) · Penn LPC users:
[lpc__quickstart.md](notebooks/lpc__quickstart.md)

---

## Repository structure

```text
biofilter/
├── api/cli/               # Click CLI: bundle, config, db, etl, report
├── core/components/       # db, etl, report, settings
├── modules/
│   ├── bundle/            # the build: plan, run, assemble
│   ├── db/                # SQLAlchemy models, JSON seeds
│   ├── etl/               # ETL framework and 21 DTPs
│   ├── io/                # Parquet export
│   └── report/            # Bundle reader, ReportManager, 17 reports
└── biofilter.py           # Python API facade

docs/source/               # Sphinx docs, split by audience
│                          #   root + getting_started/ — the scientist
└── technical/             #   building bundles, internals

biofilter_agents/          # Operational guides (LLM-ready)
assistent/                 # GPT assistant kit
notebooks/                 # templates/ + the Penn LPC guides
adr/                       # Architecture decisions
docker/                    # Image and container docs
tests/                     # unit/, integration/, contract
```

---

## 🤖 Resources

- **[Biofilter 4 Assistant](https://chatgpt.com/g/g-6887cf80355c8191ab3f88bbd8955e0d-biofilter-4-assistant)**
  — conversational help picking and running reports
- **[Report Catalog](https://biofilter.readthedocs.io/en/latest/report_catalog.html)**
  — every report, by the question it answers
- **[CLI Reference](https://biofilter.readthedocs.io/en/latest/cli_reference.html)**
  — every command and option
- **[Troubleshooting](https://biofilter.readthedocs.io/en/latest/troubleshooting.html)**
  — the errors people actually hit

---

## Status

- **Current version:** 4.3.0
- **Schema:** entity-centric, versioned (4.3.x)
- **What you read:** a Parquet bundle, via DuckDB, read-only
- **What builds it:** `bundle plan` → `bundle build`, through a throwaway SQLite
- **Stability:** actively evolving; APIs and schema may change between minor releases

There are no migrations. A schema change produces a new bundle rather than an
in-place upgrade, which is why a published bundle carries the plan and the
build record that made it.

---

## Contributing

Contributions, feedback and design discussions are welcome.

- Follow the existing patterns — entities, DTPs, reports.
- Keep provenance and reproducibility first-class: if a result can mislead,
  the result should say so.
- A report is four artifacts and they ship together: the module, its explain
  guide, a notebook, and tests.
- Prefer ORM logic on the write path; the read path is SQL over DuckDB.

📖 [Developer Extensions](https://biofilter.readthedocs.io/en/latest/technical/developer_extensions.html)

---

## License

MIT License. See [LICENSE](LICENSE).

---

## Acknowledgements

Biofilter builds on years of development and scientific usage across multiple
generations of the framework. Biofilter 4 continues that work, redesigned for
modern data volumes, richer biological relationships, and long-term
sustainability.
