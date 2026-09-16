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
