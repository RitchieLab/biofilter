# template — example report

A working report with nothing to it but the shape every other report
should follow. Copy this file and `report_template.py`, rename both, and
replace the query.

## What it does

Takes a list of gene symbols and reports, for each one, whether the
bundle carries a gene entity for it.

```bash
biofilter --bundle /path/to/bundle report run \
    --report-name template \
    --input TP53 --input BRCA1 \
    --output genes.csv
```

## Parameters

| parameter | required | meaning |
| --- | --- | --- |
| `input_data` | yes | gene symbols, via `--input` or `--input-file` |

## Columns

| column | meaning |
| --- | --- |
| `input_value` | the symbol as given |
| `entity_id` | the BF4 entity, or null when unmatched |
| `symbol` | the symbol as the bundle spells it |
| `found` | whether the bundle carries this gene |

Ids are scoped to the bundle that produced them. The `bundle_id` in the
`.provenance.json` written beside the result is what makes an id from
another build recognisable as one.

## What to copy from it

**Declare what you read.** `requires = ("gene_masters",)` turns a bundle
built without a source into one sentence, checked before the report
starts.

**Register input, never interpolate it.** `register_input()` puts the
user's values in an Arrow relation to JOIN against. That is what keeps
injection out, and what makes a 700,000-value input a hash join rather
than a SQL literal the size of a phone book.

**Match on the normalised column.** `register_input` adds
`<column>_norm`, already lowercased, so a case-insensitive match is a
plain equality. Wrapping `lower()` around a *bundle* column instead
defeats the parquet statistics — measured at 5.5x on a narrow range, and
worse as the scan widens.

**One statement.** Let DuckDB plan the whole question. A query per input
merged in Python is what the previous layer did, and it cost 113x in
time and 5x in memory on the materialisation step alone.

**Keep what you could not resolve.** The LEFT JOIN keeps unmatched
inputs with `found = false`. A report that silently drops them tells the
user nothing about the difference between "absent" and "not asked for".
