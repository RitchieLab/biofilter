# Getting Started

Biofilter 4 (BF4) resolves biological entities — genes, proteins,
pathways, diseases, variants — tracks the relationships between them, and
exposes all of it through ready-to-use reports.

What you read is a **bundle**: a directory of parquet files with a
manifest describing them. No database server, no import step. Point
Biofilter at a bundle and run reports.

## Choose your path

### Someone gave me a bundle

This is the common case, and it takes minutes.

1. [Install Biofilter](installing.md) — pip or Docker.
2. [Point at the bundle](reading_a_bundle.md) — one URI, no setup.
3. [Find a report](finding_reports.md) that fits your question.
4. [Run it](running_reports.md) — CLI or Python.

### I need to build a bundle

Only if no one has one for the data you need. Budget **two days and
150 GB** — the full human genome means 1.5 TB of downloads, processed and
discarded as it goes.

1. [Install Biofilter](installing.md) — from source if you will change DTPs.
2. [Build a bundle](../building_bundles.md) — `bundle plan`, then `bundle build`.
3. [Run a report](running_reports.md) against what you built.

[What it costs](../bundle_requirements.md) has the measured figures for
disk, memory and time before you start.

## What you'll need

- **Python 3.10+**, or **Docker** if you prefer containers.
- **A bundle** — a path you can read, local or on a shared filesystem.
- Nothing else. No database to provision, no credentials to request.

## One thing to carry with you

Ids inside a bundle — `entities.id`, `variant_id` — are internal to that
bundle. They are not stable across bundles, and a stale one still
resolves: to a different gene, without an error. Pin the bundle, not the
id. [Reading a bundle](reading_a_bundle.md) explains how results carry
their origin.

## Where this guide stops

Once you can run a report, the rest goes deeper:

- [Report catalog](../report_catalog.md) — every report, with tutorials.
- [Building bundles](../building_bundles.md) — the plan/build/inspect flow.
- [What a build costs](../bundle_requirements.md) — measured disk, time, memory.
- [Parquet backend](../parquet_backend.md) — how views are registered, performance.
- [ETL](../etl.md) — data sources, DTPs, the two branches.
- [Configuration](../configuration.md) — `.biofilter.toml` options.
- [Troubleshooting](../troubleshooting.md) — common errors.
