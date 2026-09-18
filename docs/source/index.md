# Biofilter Documentation

Biofilter 4 brings genes, variants, proteins, pathways, diseases, ontology
terms and chemicals from many public sources into one model, and lets you
query that model through ready-to-use reports.

What you read is a **bundle**: a directory of parquet files with a manifest
describing them. No database server, no import step. Point Biofilter at a
bundle and run reports.

## Where to start

**You were given a bundle and want answers from it.**
Go to [Getting Started](getting_started/index.md). It takes minutes, and
you will not need the technical section at all.

**You want to know which analyses exist.**
The [Report Catalog](report_catalog.md) lists every report and the question
it answers.

**You build bundles, or extend Biofilter.**
Go to [Technical Reference](technical/index.md).

```{toctree}
:maxdepth: 2
:caption: Getting Started

getting_started/index
getting_started/installing
getting_started/reading_a_bundle
getting_started/finding_reports
getting_started/running_reports
```

```{toctree}
:maxdepth: 2
:caption: Running Analyses

report_catalog
reports
entity_and_omics
cli_reference
troubleshooting
```

```{toctree}
:maxdepth: 2
:caption: Technical Reference

technical/index
```
