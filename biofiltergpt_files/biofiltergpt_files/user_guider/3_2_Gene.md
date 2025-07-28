# Model: Gene

> 🧠 Audience: Developers, Analysts, Biologists
> 
> 
> 📌 Focus: Master gene structure and associated models in Biofilter3R
> 

---

## 🧬 What Is a Gene in Biofilter3R?

The **Gene model** in Biofilter3R is responsible for organizing and storing standardized gene-level information derived from curated sources like HGNC, Ensembl, and NCBI. Each gene record is linked to a master `Entity`, and may contain identifiers, groupings, genomic locations, and functional classifications.

> 🧠 By using a separate Gene model (instead of storing everything in Entity), we optimize queries, structure gene-specific metadata, and allow future gene-centric expansions without impacting other domains.
> 

---

## 📚 Gene Schema Overview

The full representation of a gene in Biofilter3R includes:

| Model | Purpose |
| --- | --- |
| `Gene` | Main gene record (linked to `Entity`) with identifiers |
| `GeneLocation` | Stores genomic coordinates and cytogenetic info |
| `GeneGroup` | Organizes genes into functional/structural categories |
| `GeneGroupMembership` | Handles many-to-many relation between genes and groups |
| `LocusGroup` | Functional annotation (e.g., protein-coding, RNA, pseudogene) |
| `LocusType` | Additional gene type annotation |
| `GenomicRegion` | Named cytogenetic bands (e.g., “12p13.31”) |
| `OmicStatus` | Flags for completeness, confidence, or import status |

Each of these is explained below.

---

## 🧬 Main Model: `Gene`

| Field | Description |
| --- | --- |
| `entity_id` | Foreign key to the main `Entity` table |
| `hgnc_id` | HGNC ID (if available) |
| `entrez_id` | NCBI Gene ID (if available) |
| `ensembl_id` | Ensembl Gene ID (if available) |
| `hgnc_status` | Status from HGNC ("Approved", "Withdrawn", “Gene from NCBI”, etc.) |
| `data_source_id` | Origin of the data (ETL-tracked) |
| `omic_status_id` | Linked to `OmicStatus` for import/control status |
| `locus_group_id` | Functional classification (e.g., “protein-coding”) |
| `locus_type_id` | Specific gene type (e.g., “RNA gene”, “pseudogene”) |

> 🔗 All Gene entries link back to an Entity, enabling name-based lookup, relationship tracking, and unified cross-domain analysis.
> 

### 🧬 Gene Data Curation Strategy

The **master gene list** in Biofilter3R is initialized using the curated dataset from **HGNC (HUGO Gene Nomenclature Committee)**, which provides authoritative gene symbols and annotations for human genes. After this primary ingestion, the system is **supplemented with genes from NCBI/Entrez** that are **not present in HGNC**, ensuring broader genomic coverage.

To maintain traceability and clarity:

- Genes originating from HGNC have their `hgnc_status` field populated with HGNC-defined values (e.g., `"Approved"`, `"Symbol Withdrawn"`).
- Genes that exist **only in Entrez** are added with the `hgnc_status` set to `"Gene from NCBI"`.

All genes, regardless of source, receive a unique `Entity` ID, allowing them to participate fully in the Biofilter3R relationship system.

> 🔎 This approach enables users to distinguish between fully curated HGNC genes and additional NCBI-only genes, allowing for transparent and comprehensive gene queries across biological domains.
> 

---

## 🧬 Genomic Positioning: `GeneLocation`

Some genes have multiple **genomic locations** (e.g., alternate assemblies, isoforms). These are stored in `GeneLocation`.

| Field | Description |
| --- | --- |
| `chromosome` | Chromosome number (as string, e.g., "12", "X") |
| `start`/`end` | Genomic coordinates |
| `strand` | DNA strand orientation ("+" or "-") |
| `assembly` | Assembly version (default is `"GRCh38"`) |
| `region_id` | Optional cytogenetic band from `GenomicRegion` |

Locations are attached to the `Gene` via a one-to-many relationship.

---

## 🧬 Gene Classification

Biofilter3R provides multiple mechanisms to annotate and group genes:

### 🔹 `LocusGroup` and `LocusType`

| Model | Description |
| --- | --- |
| `LocusGroup` | Broad group (e.g., "protein-coding gene") |
| `LocusType` | More specific type (e.g., "ncRNA", "tRNA") |

These are linked to `Gene` via foreign keys and support classification in external annotations or pipelines.

### 🔹 `GeneGroup` and `GeneGroupMembership`

Functional categories like:

- Transcription Factors
- Kinases
- Cell Cycle Genes

…can be stored in `GeneGroup`. Since one gene may belong to multiple groups, the intermediate table `GeneGroupMembership` is used to manage this **many-to-many relationship**.

---

## 🧬 Cytogenetic Bands: `GenomicRegion`

This optional model allows associating genes with **named cytogenetic regions**, such as `"12p13.31"` or `"1q21.2"`. These regions are often used in clinical and cytogenetic contexts.

Each region includes:

| Field | Description |
| --- | --- |
| `label` | Name (e.g., “17q21.31”) |
| `chromosome` | Chromosome part |
| `start/end` | Approximate coordinates (if known) |

These are linked to `GeneLocation` entries.

---

## 🧬 Status Tracking: `OmicStatus`

The field `omic_status_id` in the Gene table links to the `OmicStatus` model, which may be used to track:

- Completion status (e.g., partially imported, deprecated)
- Quality control flags
- ETL processing tags

While not mandatory, this field can help workflows identify which genes are ready for downstream analysis.

---

## 💡 Design Choices

- **ORM relationships are defined**, enabling intuitive Python usage (e.g., `gene.groups`, `group.genes`)
- **Identifiers are optional** to allow ingestion of unannotated or partial entries
- **Multiple locations** per gene are supported
- **No enforced cascade deletes**, favoring manual curation and auditability

This structure provides flexibility for both **research-oriented pipelines** and **production-grade analysis**.

---

## 🧠 Summary

- The `Gene` model centralizes gene-specific metadata, identifiers, and classification
- Genomic locations and functional groupings are modular and extensible
- Biofilter3R supports incomplete genes, multiple assemblies, and ontology integration
- Queries can filter genes by name, position, status, or classification
- The model is designed for scalability, supporting hundreds of thousands of records efficiently