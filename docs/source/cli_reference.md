# CLI Reference

## Global

```bash
biofilter [--db-uri URI] [--debug] COMMAND ...
```

Groups:
- `config`
- `db`
- `etl`
- `report`

## Config

- `biofilter config show`
- `biofilter config get SECTION.KEY`
- `biofilter config set SECTION.KEY VALUE`
- `biofilter config init --path .`

## DB

- `biofilter db create-db`
- `biofilter db migrate`
- `biofilter db upgrade`
- `biofilter db backup`
- `biofilter db restore`
- `biofilter db export`
- `biofilter db import`

## ETL

- `biofilter etl update`
- `biofilter etl update-all`
- `biofilter etl status`
- `biofilter etl restart`
- `biofilter etl rollback`
- `biofilter etl index`

## Report

- `biofilter report list`
- `biofilter report explain --report-name <name>`
- `biofilter report example-input --report-name <name>`
- `biofilter report available-columns --report-name <name>`
- `biofilter report run --report-name <name> [options]`

Key `report run` options:
- `--input`, `--input-file`, `--input-column`
- `--param`, `--params-json`, `--params-file`
- `--params-template`
- `--output`
