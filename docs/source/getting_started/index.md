# Getting Started

Biofilter 4 (BF4) is a biological knowledge platform that resolves entities (genes, proteins, pathways, diseases, variants), tracks their relationships, and exposes them through ready-to-use reports.

This section walks you through your first run end-to-end. Pick the path that matches your situation and follow it in order.

## Choose your path

### I just want to run reports against a database that already exists

You have access to a Biofilter database, you don't need to do any data ingestion yourself.

1. [Install Biofilter](installing.md) — pick **pip** (recommended) or **Docker**.
2. [Connect to the database](connecting_db.md) — read **Option A: connect to an existing database**.
3. [Find a report that fits your need](finding_reports.md) — the catalog and the GPT assistant help here.
4. [Run your first report](running_reports.md) — CLI and Python API examples.

### I'm setting up my own Biofilter from scratch

You want a local database (SQLite for testing or PostgreSQL for production), populated by running the ETL yourself.

1. [Install Biofilter](installing.md) — pick **pip** or **source** if you'll contribute back.
2. [Connect to the database](connecting_db.md) — read **Option B: bootstrap a new database**.
3. Run the ETL pipeline (covered in **Option B** of the same page).
4. [Run your first report](running_reports.md) once the ETL completes.

## What you'll need

- **Python 3.10+** for pip-based installation, or **Docker** if you prefer containers.
- A **database connection string** if you're connecting to an existing instance — get this from whoever administrates it.
- Roughly **1 TB of disk space** if you're bootstrapping your own local DB with the full data.

## Where this guide stops

This Getting Started track is intentionally minimal. Once you can run a report, the rest of the documentation goes deeper:

- [Report catalog](../report_catalog.md) — every available report with descriptions and tutorials.
- [Configuration](../configuration.md) — full options for `.biofilter.toml`.
- [Database](../database.md) — schema, migrations, backup/restore.
- [ETL](../etl.md) — managing data sources, ETL packages, and rollbacks.
- [System overview](../system_overview.md) — architecture and design rationale.
- [Troubleshooting](../troubleshooting.md) — common errors and fixes.
