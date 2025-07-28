# Model: Protein

> 🧠 Audience: Developers, Bioinformaticians, Structural Biologists
> 
> 
> 📌 Focus: How proteins and their domains, isoforms, and metadata are modeled
> 

---

## 🧬 What Is a Protein in Biofilter3R

The **Protein model group** in Biofilter3R is responsible for representing **canonical proteins**, their **isoforms**, **functional domains (Pfam)**, and supporting annotations like function, localization, and expression.

Each protein is mapped to a **master entry** and optionally linked to an `Entity`, which allows it to participate in the unified omics system.

---

## 📚 Protein Schema Overview

| Model | Description |
| --- | --- |
| `ProteinMaster` | Canonical protein metadata (function, location, expression) |
| `ProteinEntity` | Protein instance linked to `Entity` and possibly to isoforms |
| `ProteinPfam` | Pfam domain metadata (family, clan, type) |
| `ProteinPfamLink` | Many-to-many link between proteins and Pfam domains |

Each model plays a unique role in capturing the biological and structural properties of proteins.

---

### 🧬 Canonical vs Isoform: Design Rationale

To properly represent **protein isoforms** — biologically distinct forms that arise from alternative splicing or translation — Biofilter3R adopts a design where **each isoform is treated as a separate `Entity`**.

While the **`ProteinMaster`** table stores the **canonical metadata** (function, location, expression), it does not attempt to split or duplicate this information across isoforms. Instead:

- Both canonical and isoform proteins share the same entry in `ProteinMaster`
- The **`ProteinEntity`** table links each isoform (or canonical form) to a unique `Entity`, and optionally includes an `isoform_accession`

This structure allows:

- Treating isoforms as **independent biological units** in downstream relationships (e.g., pathway inclusion, interaction networks)
- Maintaining clean and efficient metadata storage in `ProteinMaster`

> ⚠️ This design differs from the Gene model, where no isoform variation exists, and the Entity ID is stored directly within the main Gene table.
> 

This distinction is important for understanding **how queries are built** and how **relationships between entities** behave depending on the biological object (e.g., gene vs. protein).

---

## 🔹 `ProteinMaster`

This is the **canonical record** for a protein.

| Field | Description |
| --- | --- |
| `protein_id` | Unique accession (e.g., UniProt ID) |
| `function` | Free-text biological function |
| `location` | Subcellular or cellular location |
| `tissue_expression` | Expression notes across tissues |
| `pseudogene_note` | Annotation if the protein is from a pseudogene |
| `data_source_id` | Tracks ETL provenance |

This table serves as the anchor for all protein-centric data — including domain links and isoform tracking.

---

## 🔹 `ProteinEntity`

Links a protein instance to a master `Entity`. This allows:

- Integration with the broader omics system
- Tracking of **canonical proteins** and **isoforms**

| Field | Description |
| --- | --- |
| `entity_id` | Refers to `Entity` (gene-product relationship) |
| `protein_master_id` | Canonical protein (FK to `ProteinMaster`) |
| `is_isoform` | Boolean flag (`True` if this is an isoform) |
| `isoform_accession` | Optional isoform ID (e.g., `P04637-2`) |
| `data_source_id` | Source of the linkage |

> 💡 Each ProteinEntity instance corresponds to a biologically distinct version — either canonical or isoform — and can have a unique Entity, name, and relations.
> 

---

## 🔹 `ProteinPfam`

Captures domain-level annotations from **Pfam** and other sources.

| Field | Description |
| --- | --- |
| `pfam_acc` | Pfam accession ID (e.g., `PF00067`) |
| `pfam_id` | Pfam short name (e.g., `p450`) |
| `description` | Brief description |
| `long_description` | Extended functional description |
| `type` | Domain type (`Domain`, `Family`, `Repeat`, etc.) |
| `clan_acc` | Clan accession, if available |
| `clan_name` | Clan name |
| `source_database` | Source (e.g., Pfam, InterPro, Prosite) |

---

## 🔹 `ProteinPfamLink`

A **many-to-many relationship** between proteins and domains.

| Field | Description |
| --- | --- |
| `protein_master_id` | Linked protein |
| `pfam_id` | Linked Pfam domain |
| `data_source_id` | Origin of the annotation |

This allows tracking of which protein contains which domains, enabling structural and functional mapping.

> 🧠 Example use case: “Find all kinases that contain a SH2 domain”
> 

---

## 🧠 Design Philosophy

- **Canonical first**: Each `ProteinMaster` is the reference point for links and metadata
- **Flexible isoform model**: Isoforms are supported via `ProteinEntity` and don't duplicate master entries
- **Modular domain system**: Uses independent tables for domain descriptions and links
- **Fully traceable**: All relationships record `data_source_id` for provenance

---

## 🔁 Biological Use Cases

This model group enables powerful queries like:

- List all protein isoforms for a given gene
- Find all proteins containing a specific Pfam domain
- Retrieve canonical proteins associated with a pathway
- Map structural domains to specific protein functions

All of this is achieved while keeping the schema modular, scalable, and aligned with the broader entity system.

---

## 🧠 Summary

- Proteins are stored in `ProteinMaster` and linked to the omics ecosystem via `ProteinEntity`
- Isoforms are flagged and stored alongside canonical proteins
- Domains (Pfam) are stored in a dedicated model and linked to proteins via `ProteinPfamLink`
- The model supports both functional and structural annotations
- Fully extensible for other domain databases (e.g., SMART, Prosite, InterPro)