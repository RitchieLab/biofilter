# Model: ETL

> 🧠 Audience: Developers, Data Engineers, Curators
> 
> 
> 📌 Focus: How data sources and ETL processes are managed and tracked in Biofilter3R
> 

---

## 🔁 What Is ETL in Biofilter3R?

Biofilter3R integrates multiple biological data sources through a modular **ETL pipeline**. This pipeline is supported by a set of schema models that allow:

- Defining and organizing **source systems and datasets**
- Tracking **each execution** of ETL pipelines (extract, transform, load)
- Monitoring **statuses**, **errors**, and **timestamps**
- Logging actions for **debugging**, **curation**, and **auditing**

The ETL models are designed to be **performance-optimized**, with minimal dependencies and maximum traceability.

---

## 📚 ETL Schema Overview

| Model | Description |
| --- | --- |
| `SourceSystem` | Originating organization (e.g., NCBI, Ensembl, UniProt) |
| `DataSource` | Specific dataset or endpoint (e.g., dbSNP, HGNC, Ensembl Genes) |
| `ETLProcess` | Execution log of a complete ETL run (with status and timestamps) |
| `ETLLog` | Granular, per-step log entries (insert, skip, error, etc.) |

---

## 🔹 `SourceSystem`

Represents a **data provider or organization**, grouping multiple datasets under one origin.

| Field | Description |
| --- | --- |
| `name` | System name (e.g., `"NCBI"`, `"UniProt"`) |
| `description` | Optional description of the provider |
| `homepage` | URL or reference to source's main site |
| `active` | Whether this system is currently in use |

---

## 🔹 `DataSource`

Defines a **specific dataset**, version, and format. This is the primary anchor for ETL and data provenance.

| Field | Description |
| --- | --- |
| `name` | Short name (e.g., `"dbSNP"`, `"HGNC"`) |
| `source_system_id` | Links to the parent `SourceSystem` |
| `dtp_version` | Version of the DTP script used |
| `schema_version` | Compatible version of Biofilter3R schema |
| `format` | File or interface type (`CSV`, `JSON`, `API`, etc.) |
| `grch_version` | Genome reference used (`GRCh38`, `GRCh37`, etc.) |
| `dtp_script` | Script responsible for ingesting this data |
| `last_status` | Last known ETL result: `success`, `failed`, etc. |
| `last_update` | Timestamp of most recent successful load |
| `active` | Indicates if this source is active for processing |

> 🧠 Each DataSource is usually associated with a specific DTP implementation.
> 

---

## 🔹 `ETLProcess`

Every run of a DTP script is tracked here. The model is designed to:

- Monitor **ETL stages independently** (extract, transform, load)
- Record **start and end timestamps**
- Store **individual status enums** for each phase
- Capture **file integrity hashes** for traceability

| Field | Description |
| --- | --- |
| `data_source_id` | Links to `DataSource` |
| `global_status` | Overall result of the ETL run |
| `extract_*` | Start, end, and status of the extract phase |
| `transform_*` | Same for the transform phase |
| `load_*` | Same for the load phase |
| `raw_data_hash` | Optional: checksum of raw data |
| `process_data_hash` | Optional: checksum of transformed data |

> ✅ Status enums include: pending, running, completed, failed, not_applicable
> 

This model allows operators to **audit**, **restart**, and **track** every data integration process.

---

## 🔹 `ETLLog`

Stores **fine-grained log entries** for each step within a given ETL process.

| Field | Description |
| --- | --- |
| `etl_process_id` | ETL run to which this log belongs |
| `phase` | One of: `extract`, `transform`, `load` |
| `action` | What occurred: `insert`, `update`, `skip`, `error` |
| `message` | Optional detail or traceback |
| `timestamp` | When the action occurred |

> 🔍 These logs are useful for debugging ETL errors or reviewing ingestion behavior.
> 

---

## ⚙️ Design Principles

- ⚡ **Performance-first**: FKs and ORM `relationship()`s are disabled to speed up ingestion
- 🧩 **Modular**: Each component (source, dataset, run, log) is handled independently
- 📊 **Auditable**: Every ETL run and its outcomes are stored, including timestamps and status flags
- 🔒 **Controlled**: No record is overwritten without tracking its ingestion history

---

## 🧠 Summary

- ETL models allow Biofilter3R to organize and track the ingestion of omics datasets
- `SourceSystem` and `DataSource` define the origin and scope of each dataset
- `ETLProcess` tracks the status of each ETL run in structured phases
- `ETLLog` provides detailed action-by-action insights
- The schema supports scalability, transparency, and error recovery

> 👉 To learn how to execute, track, and debug ETL processes, visit:
> 
> 
> User Guide > ETL Workflows
>