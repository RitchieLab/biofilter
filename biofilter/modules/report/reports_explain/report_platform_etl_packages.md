# platform_etl_packages

Every ETL package that went into this bundle. One row per package, which
is one stage of one run.

```bash
biofilter report run --report-name platform_etl_packages \
    --param operation_type=load --output packages.csv
```

> Called `etl_packages` before 4.3.0. A platform report — it describes
> the bundle, not the biology in it, and takes no input.

Where `platform_etl_status` summarises one row per data source, this is
the unaggregated record behind it. Use it when the summary says something
surprising and you want to see the runs themselves.

## Parameters

| parameter | default | meaning |
| --- | --- | --- |
| `only_active` | `false` | restrict to active sources and systems |
| `source_system` | none | one or more systems, e.g. `HGNC` |
| `data_sources` | none | one or more sources, e.g. `hgnc` |
| `operation_type` | none | `extract`, `transform`, or `load` |

## Columns

| column | meaning |
| --- | --- |
| `package_id` | the package, and the order things ran in |
| `source_system`, `data_source`, `data_type` | what it was for |
| `operation_type` | which stage this package is |
| `status` | the package's own outcome |
| `extract_*`, `transform_*`, `load_*` | status, timings, rows and hash per stage |
| `note`, `stats` | whatever the DTP recorded |
| `created_at`, `version_tag` | when, and against which release |

## Reading the result

**One stage per row.** A source that ran fully has three packages:
extract, transform, load. Only the columns of that stage are filled — an
extract package has `extract_status` and `extract_hash` and nulls in the
rest.

**The hash travels.** The digest in a extract package's `extract_hash`
reappears as the next package's `transform_hash`, and then as
`load_hash`. That is what `platform_etl_status` calls alignment.

**Failures stay in the record.** A source that failed and was retried
keeps both packages. `platform_etl_status` reports the latest *good*
stage, so it can say `ok` while a failure sits here — its `latest_error`
is how the two connect.
