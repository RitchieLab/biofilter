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
