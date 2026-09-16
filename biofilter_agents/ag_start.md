# AG Start - Biofilter Setup and First Run (CLI/API)

Practical onboarding for someone who has been given a **bundle** and wants an
answer out of it.

This guide covers:
- installation (`pip install biofilter`, Docker, or source)
- pointing Biofilter at a bundle
- confirming the setup
- running a first report and reading what comes back

It does **not** cover building a bundle or running the ETL. Both are
maintainer tasks — a full build needs about 150 GB of working space and two
days — see `ag_db_en.md` and `ag_etl_en.md`, or ask whoever maintains the
bundle you were given.

---

## 1) Quick Outcome

By the end you will be able to:
- run `biofilter --help`
- point Biofilter at a bundle
- confirm which bundle is in effect and what it contains
- run a report and export a CSV
- tell an empty result from a missing source

---

## 2) What you are pointing at

A **bundle** is a dated directory of parquet files plus a `manifest.json`
describing them:

```
20260914/
├── manifest.json        the catalogue: every file, its rows, its size
├── bundle_plan.json     which sources went in, at which versions
├── build_record.json    what each build run did, in order
└── tables/              the parquet
```

Biofilter opens the directory and queries it with DuckDB in the same process.
No server, no import step, no per-user copy — one bundle on shared storage
serves any number of concurrent readers.

A bundle is read-only and is never updated in place. Refreshing data means a
**new** bundle.

---

## 3) Installation

### 3.1 Option A - PyPI

```bash
pip install biofilter
biofilter --help
```

Requires Python 3.10+.

### 3.2 Option B - Container

One image, two mounts: `/bundle` read-only, `/workspace` writable.

```bash
docker run --rm \
  -v /path/to/bundles/20260914:/bundle:ro \
  -v "$PWD/out:/workspace" \
  --user "$(id -u):$(id -g)" \
  ricoandre/biofilter:latest \
  report run --report-name annotate_gene --input TP53 --output /workspace/genes.csv
```

Under Apptainer, `--bind` replaces `-v` and output ownership takes care of
itself:

```bash
apptainer pull bf4.sif docker://ghcr.io/ritchielab/biofilter-hpc:latest
```

### 3.3 Option C - Source

For contributors:

```bash
git clone https://github.com/RitchieLab/biofilter.git
cd biofilter
poetry install
poetry run biofilter --help
```

### 3.4 On a managed cluster

The environment may already be prepared. On the Penn LPC:

```bash
source /project/hall_shared/hall_shared.sh
module load biofilter/4.3.0
```

The module puts the CLI on `PATH` and sets `BIOFILTER_BUNDLE`, so you pass no
path at all. See `notebooks/lpc__quickstart.md`.

---

## 4) Point at the bundle

Three ways, highest precedence first:

```bash
biofilter --bundle /path/to/bundles/20260914 report list   # one command
export BIOFILTER_BUNDLE=/path/to/bundles/20260914          # the session
```

```toml
# .biofilter.toml at your project root
[database]
bundle = "./biofilter_data/bundles/20260914"
```

A relative path in the TOML resolves against **that file**, not your working
directory, so it means the same thing from the project root and from a
notebook two levels down.

Two rules worth stating once:

- **Point at the bundle root**, the directory holding `manifest.json`, never
  at its `tables/` subdirectory.
- **`--db-uri` is for writing.** Reports read bundles. Passing `--bundle` and
  `--db-uri` together is an error rather than a guess about which you meant.

### 4.1 Full resolution order

1. `--bundle`
2. `--db-uri`
3. `BIOFILTER_BUNDLE`
4. `DATABASE_URL` / `BIOFILTER_DB_URI`
5. `.biofilter.toml` — `[database] bundle`, then `[database] db_uri`

---

## 5) Confirm the setup

```bash
biofilter --version
biofilter config show                 # what actually resolved, and from where
biofilter bundle info /path/to/bundle # what the bundle says about itself
```

`bundle info` prints its id, the versions that built it, when, and its tables
by branch.

To check a bundle against this install:

```bash
biofilter db verify --in /path/to/bundle --schema
```

`--schema` exits non-zero on drift, so it works as a gate in a script.

---

## 6) First report

```bash
biofilter report list --verbose
```

Start with what the bundle holds — worth doing on any bundle you have just
been handed:

```bash
biofilter report run --report-name platform_data_statistics \
  --output bundle_contents.csv
```

Then ask it something:

```bash
biofilter report run --report-name annotate_gene \
  --input TP53 --input BRCA1 \
  --output genes.csv
```

`--input` repeats; there is no comma-separated form. For long lists use
`--input-file genes.txt`, one value per line.

See `ag_report_en.md` for the full report workflow.

---

## 7) Read the result honestly

An empty or partial table has several causes and they are not
interchangeable.

| What you see | What it means |
|---|---|
| `not_found` | the name did not resolve in this bundle |
| `no_location` | it resolved, but there are no coordinates for it |
| `no_variants` | it resolved and nothing met your criteria — a real negative |
| a column entirely null | the source may never have been built into this bundle |

For the last one, the provenance is where it is written down:

```python
result.provenance["coverage"]     # optional tables the bundle lacked
result.provenance["bundle_id"]    # which build produced these rows
```

**Ids belong to one bundle.** `entities.id`, `variant_id` and the rest are
valid only inside the bundle that produced them. The drift between builds is
small, which is what makes it dangerous — a stale id still resolves, to a
different gene, with no error. Pin the bundle, not the id.

Saving a result keeps that record: `--output genes.csv` also writes
`genes.csv.provenance.json` beside it.

---

## 8) API / Notebook Quickstart

```python
from biofilter import Biofilter

bf = Biofilter(bundle="/path/to/bundles/20260914")

result = bf.report.run("annotate_gene", input_data=["TP53", "BRCA1"])

df = result.to_pandas()
print(result.num_rows, "rows")

result.provenance["bundle_id"]
result.provenance["coverage"]
result.write("genes.csv")          # writes genes.csv.provenance.json too
```

`bf.report.run()` returns a result object rather than a bare DataFrame. The
extra layer is what carries the record of where the rows came from; call
`.to_pandas()` when you want to continue in pandas.

`Biofilter()` with no argument falls back to `BIOFILTER_BUNDLE` or
`.biofilter.toml`, so in a configured environment the constructor stays empty.

A worked notebook ships for every report at
`notebooks/templates/reports__<name>.ipynb`.

---

## 9) If something is wrong

| Symptom | Check |
|---|---|
| `No manifest.json in <path>` | you pointed at `tables/`, or the path is not a bundle |
| `<name> does not carry: <tables>` | the bundle was built without those sources — run `platform_data_statistics` |
| `declares manifest_version N` | the bundle is newer than this install; update the package |
| warning that the bundle was built by another release | it still works; run `db verify --schema` to check |
| output file missing, in a container | `--output` wrote inside it — point at the mounted `/workspace` |

More detail on any failure:

```bash
biofilter --debug report run --report-name <name> --input <value>
```

---

## 10) Where to go next

- `ag_report_en.md` — the full report workflow
- `docs/source/report_catalog.md` — every report, by the question it answers
- `notebooks/lpc__quickstart.md` — the Penn LPC specifics
- `ag_db_en.md`, `ag_etl_en.md` — maintainer territory: building a bundle
