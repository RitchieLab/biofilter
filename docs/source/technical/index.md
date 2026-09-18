# Technical Reference

How Biofilter is built and how it works internally. You need this section
if you **build bundles**, **add a data source or a report**, or need to
know exactly what the data means at the table level.

If you were given a bundle and want to run analyses on it, you do not need
anything here — start at [Getting Started](../getting_started/index.md).

```{toctree}
:maxdepth: 1

system_overview
building_bundles
bundle_requirements
etl
database
schema
read_path
developer_extensions
configuration
```

## What is where

| Page | Answers |
|---|---|
| [System Overview](system_overview.md) | How the whole system fits together, in one diagram |
| [Building Bundles](building_bundles.md) | `plan` → `build` → `info`, and what each step guarantees |
| [Bundle Requirements](bundle_requirements.md) | Disk, time and network before you start a build |
| [ETL Operations](etl.md) | Running one source: update, status, restart, rollback |
| [Database Operations](database.md) | Create, verify, export, import, backup |
| [Database Schema](schema.md) | Every table and column, with the ER diagram |
| [The Read Path](read_path.md) | How a bundle becomes queryable, and what is checked on the way |
| [Developer Extensions](developer_extensions.md) | Adding a DTP or a report |
| [Configuration](configuration.md) | Settings, precedence, environment variables |
