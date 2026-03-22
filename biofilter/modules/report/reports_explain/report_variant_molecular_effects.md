# Report Tutorial: `variant_molecular_effects`

## Purpose

Given genomic regions, returns overlapping variants from `variant_masters` and consequence annotations from `variant_molecular_effects` with resolved labels (consequence/group/category/impact/biotype).

## Report Name

`variant_molecular_effects`

## Parameters (API)

- Input:
  - `items`: `list[str] | list[dict]` (optional)
  - `input_data`: alias for `items` (optional)
  - `input_path`: text file with one region per line (optional)
- Behavior:
  - `range_up`: `int` (default `0`)
  - `range_down`: `int` (default `0`)
  - `emit_not_found_rows`: `bool` (default `True`)
  - `include_variant_only_rows`: `bool` (default `True`)
  - `limit_variants_per_input`: `int` (default `1000`)
  - `effect_query_chunk_size`: `int` (default `2000`)

## Input Formats

- String: `chr:start:end` (for example `chr1:55516888:55516888`)
- Dict:
  - `{"chromosome": "1", "start": 123, "end": 456}`
  - `{"chr": "X", "pos_start": 123, "pos_end": 456}`

## Examples

API:

```python
df = bf.report.run(
    "variant_molecular_effects",
    items=[
        "chr1:55516888:55516888",
        {"chromosome": "7", "start": 55019017, "end": 55019017},
    ],
    range_up=50,
    range_down=50,
    include_variant_only_rows=True,
)
```

From file:

```python
df = bf.report.run(
    "variant_molecular_effects",
    input_path="./regions.txt",
    limit_variants_per_input=500,
)
```

CLI:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name variant_molecular_effects --input chr1:55516888:55516888 --param range_up=50 --param range_down=50
```

With input file:

```bash
biofilter --db-uri sqlite:///biofilter_dev.db report run --report-name variant_molecular_effects --input-file ./regions.txt --param limit_variants_per_input=500
```

## Demo Tips

- Use a short curated region list for interactive demos.
- Show `Status`, `Variant Key`, `Gene Symbol`, `Consequence`, `Impact`, `Biotype`.
