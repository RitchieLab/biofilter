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

Only if no one has one for the data you need. The full human genome
means 1.5 TB of downloads, processed and discarded as the build goes, so
plan for **150 GB of working space**.

1. [Install Biofilter](installing.md) — from source if you will change DTPs.
2. [Build a bundle](../technical/building_bundles.md) — `bundle plan`, then `bundle build`.
3. [Run a report](running_reports.md) against what you built.

[What it costs](../technical/bundle_requirements.md) has the measured figures for
disk, memory and runtime before you start.

## What you'll need

- **Python 3.10+**, or **Docker** if you prefer containers.
- **A bundle** — a path you can read, local or on a shared filesystem.

## One thing to carry with you

Ids inside a bundle — `entities.id`, `variant_id` — are internal to that
bundle. They are not stable across bundles, and a stale one still
resolves: to a different gene, without an error. Pin the bundle, not the
id. [Reading a bundle](reading_a_bundle.md) explains how results carry
their origin.

## Where this guide stops

Once you can run a report, the rest goes deeper:

- [Report catalog](../report_catalog.md) — every report, with tutorials.
- [Building bundles](../technical/building_bundles.md) — the plan/build/inspect flow.
- [What a build costs](../technical/bundle_requirements.md) — measured disk, time, memory.
- [The Read Path](../technical/read_path.md) — how views are registered, and what a query costs.
- [Data sources and ingestion](../technical/etl.md) — where the data comes from, and how it gets in.
- [Configuration](../technical/configuration.md) — `.biofilter.toml` options.
- [Troubleshooting](../troubleshooting.md) — common errors.

Running on the Penn LPC? The cluster-specific quickstart and the
maintainer's deployment guide live in the repository at
`notebooks/lpc__quickstart.md` and `notebooks/lpc__deploy.md`.
