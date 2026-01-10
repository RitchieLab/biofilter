# Reports

Reports are the primary user-facing interface of **Biofilter 4**.
They provide predefined, reusable query modules designed to answer common
biological questions over an integrated and curated knowledge base.

Reports abstract the complexity of the underlying data model and expose
standardized query logic that supports **Annotation**, **Filtering**, and
**Modeling** workflows. For most users, reports represent the main interaction
layer with Biofilter 4.

---

## What Are Reports?

A **report** is a named query module implemented within Biofilter that
encapsulates:

- validated query logic,
- biologically meaningful relationships,
- and a consistent output structure.

Reports are designed to be:

- reusable across projects,
- parameter-driven,
- reproducible,
- independent of downstream analysis tooling.

This allows users to focus on biological questions rather than database
structure or join logic.

---

## Supported Analysis Modes

Biofilter 4 reports are conceptually aligned with three major analysis modes:

### Annotation
Reports that enrich user-provided inputs with biological context, such as:

- gene → variant annotation,
- variant → gene or pathway mapping,
- position-based biological annotation.

### Filtering
Reports that reduce input datasets based on biological criteria, for example:

- filtering variants by gene membership,
- restricting results to curated pathways or interaction networks.

### Modeling
Reports that generate biologically informed models or relationships, such as:

- gene–gene or variant–variant interaction candidates,
- network-derived feature sets for downstream statistical modeling.

> **Note**  
> While not all report types are fully implemented yet, the reporting framework
> is designed to support all three modes consistently.

---

## Prerequisites: Knowledge Base Availability

Before running any report, ensure that the Biofilter database:

- is accessible (local or remote),
- is properly configured,
- and has been populated with biological knowledge.

A newly created database contains only structural metadata and seed data.
Reports will return empty results if no curated data has been ingested.

See **Running ETL Pipelines** for details on populating a knowledge base.

---

## Local and Remote Database Support

Reports can be executed against:

- a shared central knowledge base, or
- a local Biofilter database.

The database may be hosted locally or remotely (for example, a cloud-hosted
PostgreSQL instance). The execution model is identical in all cases; only the
database connection differs.

---

## Initializing Biofilter for Reports

Before running a report, initialize a Biofilter instance connected to the target
database:

```python
from biofilter import Biofilter

bf = Biofilter()
````

If a database URI is defined in `.biofilter.toml`, Biofilter automatically
connects to it. Otherwise, a database URI can be provided explicitly:

```python
bf = Biofilter("sqlite:///path/to/biofilter.db")
```

---

## Discovering Available Reports

Reports are exposed through a high-level API designed to be self-describing and
easy to explore interactively.

To list all available reports:

```python
bf.report.list()
```

This is the recommended starting point when exploring the reporting capabilities
of Biofilter 4.

---

## Running an Example Report

Many reports provide a built-in example input that can be executed immediately:

```python
df = bf.report.run_example("gene_to_snp")
```

This is useful to validate that:

* the database is populated,
* the connection is working,
* and the report produces expected results.

---

## Understanding Report Inputs and Behavior

Reports expose metadata describing their expected inputs and behavior:

```python
bf.report.example_input("gene_to_snp")
bf.report.explain("gene_to_snp")
```

* `example_input()` shows a minimal input payload template.
* `explain()` describes what the report does, its core logic, and expected
  outputs.

---

## Discovering Available Output Columns

Reports often support multiple output fields. To inspect which columns are
available:

```python
bf.report.available_columns("gene_to_snp")
```

This allows users to tailor results for different use cases, such as compact
annotation tables, downstream filtering, or modeling workflows.

---

## Running a Report with Custom Parameters

Reports are executed using `bf.report.run()` by specifying the report name,
input data, and optional parameters.

Example:

```python
df = bf.report.run(
    "gene_to_snp",
    input_data=["ENSG00000143801"],
    window_bp=0,
    output_columns=[
        "HGNC Symbol",
        "SNP Chr (23:X/24:Y)",
        "SNP Pos (Build 38)",
        "Ref Allele",
        "Alt Allele",
    ],
)
```

Selecting output columns helps:

* keep tables compact,
* standardize outputs across projects,
* reduce unnecessary data movement.

---

## Example Notebooks

The following notebooks serve as reference templates and practical examples for
working with reports:

* 📓 **Reports 101** (`reports__101.ipynb`)
  Introduction to discovering, running, and inspecting reports.

* 📓 **Gene-to-SNP Annotation** (`reports__annotation_gene_to_snp.ipynb`)
  Example of an annotation-focused report.

* 📓 **Reports and ETL Metadata** (`reports__etl.ipynb`)
  Demonstrates reports that interact with ETL provenance and metadata.

These notebooks are available in the Biofilter GitHub repository and are intended
to be executable and extensible.

---

## Reports vs Queries

Reports are designed for:

* standardized workflows,
* reproducible analyses,
* shared logic across teams and projects.

For exploratory, low-level, or highly customized access patterns, Biofilter also
exposes a **query-oriented API**, which is covered in the next section.

---

## Future CLI Support

Biofilter 4 is actively evolving toward full CLI support for reports.
Future releases will allow reports to be executed directly from the command line,
enabling:

* batch execution,
* pipeline integration,
* HPC-friendly workflows.

The CLI and API will share the same report definitions and execution engine.

---

## Summary

Reports are the primary mechanism for interacting with Biofilter 4 knowledge.
They provide a stable, reusable, and biologically meaningful interface for
annotation, filtering, and modeling workflows—whether executed locally or
against a remote knowledge base.

