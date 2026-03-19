# ETL Operations

ETL is how Biofilter ingests, normalizes, and versions knowledge from external sources.

## Main Commands

Update selected sources:

```bash
biofilter etl update --data-source hgnc
```

Resumable batch update:

```bash
biofilter etl update-all
biofilter etl update-all --source-system NCBI
biofilter etl update-all --drop-files
```

Status overview:

```bash
biofilter etl status
biofilter etl status --source-system NCBI --only-active
```

Restart with rollback + rerun:

```bash
biofilter etl restart --data-source gnomad_chr22
```

Rollback only:

```bash
biofilter etl rollback --package-id 123
biofilter etl rollback --data-source gnomad_chr22 --delete-files
```

## Monitoring Pair

- `biofilter etl status` for quick operational view.
- `biofilter report run --report-name etl_packages` for detailed audit.

## File Lifecycle (Raw and Processed)

By default, BF4 uses:
- download path: `./downloads`
- processed path: `./processed`

For each data source, ETL stages typically use:
- raw files: `<download_path>/<source_system>/<data_source>/...`
- processed outputs: `<processed_path>/<source_system>/<data_source>/...`

You will commonly see parquet files in the processed stage (e.g., `master_data.parquet`, relationship datasets).

`etl update-all --drop-files` can remove raw/processed directories after successful load for each data source.

## ETL Package Tracking

Each ETL run writes package metadata into the database, including:
- operation type (`extract`, `transform`, `load`, `rollback`)
- step status and timestamps
- hash linkage to support skip/up-to-date behavior
- error messages in package stats when failures happen

This is the foundation for resumable updates and for ETL audit reports.
