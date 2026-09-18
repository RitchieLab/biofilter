# platform_data_statistics

What this bundle holds: how much, of what, and how big.

```bash
biofilter report run --report-name platform_data_statistics --output stats.csv
```

> Called the same thing before 4.3.0. A platform report — it describes
> the bundle, not the biology in it, and takes no input beyond which
> sections to compute.

## One row per measurement

Heterogeneous statistics do not fit a wide table, so this one is long:

| column | meaning |
| --- | --- |
| `section` | which group of measurements |
| `metric` | what is being measured |
| `dimension_1`, `dimension_2` | what it is measured *by* |
| `value_number` | the number, when there is one |
| `value_text` | the value, when it is not a number |
| `as_of` | when the measured thing happened, not when the report ran |
| `note` | anything that needs saying about the row |

A wide table would have to change shape every time a section is added.

## Sections

| section | what it answers | cost |
| --- | --- | --- |
| `bundle` | which build is this, and how big overall | free |
| `storage` | rows, bytes and file count per table | free |
| `entities` | how many of each kind of thing | a scan |
| `variants` | how many variants per chromosome, per variant table | a scan |
| `relationships` | links by group pair, and by type | a scan |
| `sources` | what each data source contributed, and when | a scan |

```bash
--param sections=bundle --param sections=storage
```

**`bundle` and `storage` cost nothing.** They read `manifest.json`, which
already records rows and bytes per file. The sizes of a 21 GB bundle come
out of a few hundred lines of JSON — measured at 0.00s against the full
one, where the complete report takes 2.8 seconds over 3.2 billion rows.

**`variants` is grouped from the data, not from filenames.** The manifest
counts rows per *file*, and a file happening to be one chromosome is a
convention of the current build rather than a guarantee. Grouping by the
`chromosome` column is affordable because it has row-group statistics —
2.2 billion rows group in about a second.

## Reading the result

**`tables_without_rows` is the one to watch.** A declared table with no
rows is a source that was planned and did not land. It gets a number of
its own rather than being buried in the per-table list, because it is the
measurement most likely to mean something is wrong.

**`storage` sums a partitioned table across its files.** `note` says how
many, so a table spread over 25 files reads as one row.

**`sources` lists every data source, including ones that never ran** —
those have a null `value_text` and no `as_of`. `platform_etl_status` is
where to go for why.

**`as_of` is about the data, not the report.** When a source was last
loaded, for instance. When the *report* ran is in the provenance sidecar.

## The two tables beside the long one

The long shape holds most of this report faithfully. Two sections it
cannot, and in both cases what it loses is the part you would sort by —
so those travel as tables of their own:

```python
stats = bf.report.run("platform_data_statistics")

stats.table                        # the long measurements, unchanged
stats.extra_tables["storage"]      # table, branch, rows, bytes, files
stats.extra_tables["variants"]     # table, chromosome, rows
```

`storage.bytes` is an integer. In the long shape a table's size survives
twice and neither is usable: `value_text` rounds it to `"3.4 MB"` and
`note` buries the figure in `"1 file(s), 3416028 bytes"`.

`variants.chromosome` is an integer. In the long shape it is a string in
`dimension_2`, so sorting gives 1, 10, 11, 2.

The other four sections keep the long shape and lose nothing by it.
`relationships` alone carries two metrics of different shapes, which is
why "one table per section" is not a thing this report could have.
