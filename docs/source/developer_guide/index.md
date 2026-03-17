# Developer Guide

The Developer Guide is intended for contributors, maintainers, and advanced
users who want to understand, extend, or operate Biofilter 4 at a deeper
technical level.

While the **User Guide** focuses on how to *use* Biofilter 4 (reports,
configuration, and workflows), this guide explains **how Biofilter works
internally** and how new components can be safely added or extended.

This guide covers the internal architecture, database design, ETL framework,
and development best practices that underpin Biofilter 4.

---

Who should read this guide?

This guide is intended for:

- developers extending Biofilter with new domains, reports, or ETL pipelines,
- data engineers responsible for ingestion, updates, and performance,
- maintainers operating Biofilter in shared, HPC, or production environments,
- advanced users who need to understand internal data modeling choices.

If your goal is to *run reports or analyses*, start with the **User
Guide** instead.

---

What this guide covers

The Developer Guide is organized around the core subsystems of Biofilter 4:

- **System architecture and design principles**
- **Database schema and modeling strategy**
- **ETL framework internals and DTP development**
- **Report system internals and authoring**
- **Indexing and performance considerations**
- **Schema evolution and migrations (Alembic)**

Together, these sections explain how Biofilter 4 implements a persistent,
entity-centric biological knowledge platform.

---

```{toctree}
:maxdepth: 2
:caption: Developer Guide

1_architecture
2_database_models/index
3_etl_internals/index
4_reports/index
6_project_structure
