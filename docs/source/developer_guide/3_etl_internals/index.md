# ETL

This section describes the internal ETL architecture of Biofilter 4, focusing on
how ingestion is orchestrated, how **Data Transformation Packages (DTPs)** are
structured, and how biological knowledge is normalized, persisted, and
versioned.

Unlike the User Guide, this section is intended for developers extending,
maintaining, or creating new ingestion pipelines.

```{toctree}
:maxdepth: 1
:caption: ETL Development

3_0_etl
3_1_write_dtp
3_2_indexes
3_3_entity_registration
