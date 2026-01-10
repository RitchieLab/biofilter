# Overview

Biofilter 4 is a persistent biological knowledge platform designed to support
gene-centric analysis workflows through a unified, extensible data model and
multiple user interfaces.

Instead of treating biological knowledge as transient input files processed
independently by each analysis, Biofilter 4 provides a **shared, versioned
knowledge layer** that can be queried, updated, and reused across projects,
studies, and computational environments.

## Key Characteristics

- **Persistent biological knowledge base**
- **Unified and normalized data schema**
- **Gene-centric and entity-based modeling**
- **Multiple interaction layers** (CLI, Python API, SQL, notebooks)
- **Designed for extensibility and long-term evolution**

Biofilter 4 is not tied to a single interface or workflow. It can be used
interactively, programmatically, or as part of larger automated pipelines.

---

## Evolution of Biofilter: From v2 to v4

## Biofilter 2

- Widely used and consolidated
- Implemented in Python 2
- Depended on the LOKI system for database management
- Batch-oriented and file-driven workflows
- Proven scientific impact

Biofilter 2 established the conceptual foundations of gene-centric annotation
and filtering and was extensively used in production analyses.

---

## Biofilter 3

- Native Python 3 implementation
- Biofilter and LOKI consolidated into a single software stack
- Rebuild of the codebase
- Preserved the original Biofilter 2 data schema
- Focused on modernization and maintainability

Biofilter 3 represented a technical consolidation step, ensuring continuity
while enabling long-term support.

---

## Biofilter 4

- Redesigned data schema
- Explicit modeling of biological entities and relationships
- Built to support new data types and higher data volumes
- Designed for extensibility, incremental updates, and interactive querying

Biofilter 4 was redesigned to address new demands in data scale, data diversity,
and user interaction. The new architecture emphasizes **persistent knowledge**,
**explicit relationships**, and **flexible interfaces** that support both
exploratory and production-level workflows.

---

## Who Is Biofilter 4 For?

Biofilter 4 is designed for users who need:

## Annotation

To annotate genes or variants using curated biological knowledge that can be
reused across multiple analyses and projects, instead of repeatedly applying
file-based annotation pipelines.

## Filtering

To filter biological entities based on integrated knowledge (e.g. genes,
variants, pathways, relationships) using flexible, query-driven criteria rather
than static rule sets.

## Modeling

To support gene-centric modeling workflows by providing structured
relationships between biological entities that can be directly consumed by
downstream statistical or machine learning analyses.

## A Unified and Normalized Knowledge Source

To work with a single, consistent representation of biological knowledge that
integrates multiple data sources into a shared, versioned schema.

## Iterative and Exploratory Analysis

To ask new biological questions over the same knowledge base without rebuilding
annotations or re-running large pipelines.

## Multiple Interaction Styles

To access biological knowledge via Python, SQL, command-line tools, or notebooks,
depending on the analysis context.
