# Model: Curation & Conflicts

> 🧠 Audience: Developers, Data Curators, Bioinformaticians
> 
> 
> 📌 Focus: Conflict tracking and resolution in Biofilter3R
> 

---

## 🧬 Why Conflict Tracking?

In large-scale biological data integration, it is common to encounter **conflicting records** across sources. For example:

- Two genes share the same Entrez ID but different HGNC symbols
- A protein appears under different identifiers in two datasets
- An entity already exists in the database with a partially overlapping identifier set

The **CurationConflict** model in Biofilter3R allows us to:

- **Detect and log conflicts** during data ingestion
- **Track resolution status** (manual or automated)
- **Document the reasoning** behind each resolution
- Maintain **data integrity** without silently discarding or overwriting records

---

## 📚 Conflict Schema Overview

| Model | Description |
| --- | --- |
| `CurationConflict` | Logs each conflict with metadata, proposed resolution, and notes |
| `ConflictStatus` | Enum: current status of the conflict (`pending`, `resolved`) |
| `ConflictResolution` | Enum: how the conflict was handled (`merge`, `delete`, `keep_both`) |

---

## 🔹 `CurationConflict` Model

| Field | Description |
| --- | --- |
| `entity_type` | Domain of the conflict (e.g., `"gene"`, `"protein"`) |
| `identifier` | The new incoming identifier causing the conflict |
| `existing_identifier` | The already existing identifier in the database |
| `entity_id` | If applicable, the ID of the affected entity |
| `data_source_id` | Source from which the conflict originated |
| `status` | `pending` or `resolved` |
| `resolution` | Decision (`merge`, `delete`, or `keep_both`) |
| `description` | Description of the issue |
| `notes` | Optional curator comments or audit notes |

---

## ⚖️ Conflict Lifecycle

### 1. **Detection**

Conflicts are detected during the ETL `load()` phase when attempting to ingest a record that overlaps (by identifier) with an existing entity, but presents **inconsistent values**.

> Example: Two genes with the same entrez_id but different hgnc_id
> 

---

### 2. **Logging**

A new entry is created in the `curation_conflicts` table, with `status = pending`. These records **do not interrupt the ETL**, but the conflicting data is not inserted until reviewed.

---

### 3. **Resolution**

Curators or automated scripts can review and resolve conflicts by updating the `resolution` field:

| Resolution | Meaning |
| --- | --- |
| `merge` | The new entity is merged into the existing one; identifiers may be consolidated |
| `delete` | The incoming entity is discarded |
| `keep_both` | Both entities are retained; identifiers are assumed not conflicting |

Once resolved, the `status` is set to `resolved`, and the conflict is no longer blocking.

> ✅ Conflict-aware logic ensures data consistency while enabling flexibility during ingestion.
> 

---

## 🧠 Design Principles

- **Decoupled from domain models**: Conflicts apply generically to any `entity_type`
- **Human-readable**: Identifier strings (e.g., `"HGNC:1234"`) allow for easy review
- **Extensible**: Can be expanded with additional metadata (e.g., timestamps, evidence, automatic flags)

---

## 🧪 Example Entries

| ID | entity_type | identifier | existing_identifier | status | resolution | notes |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | gene | HGNC:40594 | HGNC:58098 | pending | — | Same Entrez ID |
| 2 | gene | ENSG00000100001 | HGNC:GENE1 | resolved | keep_both | Curated manually |
| 3 | protein | P12345-2 | P12345 | resolved | merge | Isoform was redundant |

---

### 🧪 How Conflicts Are Handled During ETL

The **ETL pipeline in Biofilter3R is fully equipped to detect, log, and—when possible—automatically handle data conflicts**.

- Known patterns (e.g., duplicated identifiers with matching data) are auto-resolved.
- The system applies defined rules (e.g., preferring HGNC over NCBI) to handle common inconsistencies.
- Only when a **new or ambiguous conflict is detected**, the system flags it with `status = pending`.

At this point, **user intervention is required** to review the conflict and choose a resolution strategy—whether to `merge`, `delete`, or `keep_both`.

> ⚠️ If no resolution is defined, the conflicting data will not be ingested until explicitly handled.
> 

This workflow ensures high-quality curation while **minimizing manual workload**, reserving curator attention for genuinely complex cases.

---

## 🧠 Summary

- Conflicts are tracked using `CurationConflict` to ensure safe and transparent data integration
- The system supports detection, audit, and resolution of conflicts across domains
- Resolutions are logged and classified to support traceability and reproducibility
- The schema is general-purpose and can be extended for future curation workflows