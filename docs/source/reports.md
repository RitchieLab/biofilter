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

A result is a table plus a record of how it was produced.

```python
result.num_rows
result.columns
result.to_pandas()

result.provenance["bundle_id"]   # which build these rows came from
result.provenance["params"]      # what was asked
result.provenance["coverage"]    # what the bundle did not have
```

`result.write("out.csv")` saves `out.csv.provenance.json` beside it. Writing
parquet instead keeps the provenance inside the file's own metadata, so it
travels even if the sidecar is lost.

Two habits worth forming:

- **Check `coverage` before trusting a null.** It lists the optional tables
  the bundle lacked and the chromosomes it spans. A column that is null
  because a source was never built looks exactly like one that is null
  because the answer is null.
- **Keep the `bundle_id` with the result.** Entity and variant ids are valid
  only inside the bundle that produced them, so an id without its build is
  not a fact.

## Guides and notebooks

Each report ships two pieces of documentation besides this page:

| | |
|---|---|
| The guide | `biofilter/modules/report/reports_explain/report_<name>.md`, printed by `report explain` |
| A notebook | `notebooks/templates/reports__<name>.ipynb`, a worked example against a real bundle |
