# Running Your First Report

Two ways to run any report: from the command line (CLI) or from Python (notebook or script). Both produce the same output. Pick whichever fits your workflow.

## CLI — quickest path

List what's available:

```bash
biofilter report list
```

Run a report and print the result to the terminal:

```bash
biofilter report run --report-name etl_status
```

Save the output to a CSV file:

```bash
biofilter report run --report-name etl_status --output etl_status.csv
```

Pass parameters with `--param KEY=VALUE`:

```bash
biofilter report run \
  --report-name entity_filter \
  --input "BRCA1" \
  --input "TP53" \
  --param match_mode=exact
```

For input lists too long for the command line, use `--input-file`:

```bash
biofilter report run \
  --report-name entity_filter \
  --input-file ./genes.txt
```

To see what parameters a report accepts:

```bash
biofilter report explain --report-name entity_filter
```

## Python API — best for notebooks and scripts

```python
from biofilter import Biofilter

bf = Biofilter()  # picks up DB from .biofilter.toml or DATABASE_URL

df = bf.report.run(
    "entity_filter",
    input_data=["BRCA1", "TP53", "APOE"],
    match_mode="exact",
)

print(f"{len(df)} rows")
df.head()
```

Every report returns a pandas `DataFrame`, so you can chain it with the rest of your analysis without saving to disk first.

## A complete first example

Here's a full session — install, connect, run:

```bash
# Install
pip install biofilter

# Configure
export DATABASE_URL="postgresql+psycopg2://user:pass@host:5432/biofilter_prod"

# Run
biofilter report list
biofilter report run --report-name etl_status --output etl_status.csv
```

Open `etl_status.csv` in your favorite tool and you'll see the current state of every data source in the database.

## Next steps

- Browse the [Report Catalog](../report_catalog.md) for what else you can do.
- Each report has a notebook tutorial in [`notebooks/Templates/`](https://github.com/RitchieLab/biofilter/tree/biofilter3r/notebooks/Templates) — copy one and adapt it.
- For deeper CLI options, see the [CLI Reference](../cli_reference.md).
- For Python API patterns, see [Reports](../reports.md).
