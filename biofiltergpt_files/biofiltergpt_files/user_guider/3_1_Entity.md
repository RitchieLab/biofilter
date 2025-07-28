# Model: Entity

> 🧠 Audience: Developers, Analysts
> 
> 
> 📌 Focus: Core entity system and how all biological data is linked
> 

---

## 🧩 What Is an Entity?

At the heart of Biofilter3R lies the **Entity** model — a unified abstraction that represents **any biological object**: genes, proteins, variants, pathways, ontologies, and more.

Every object ingested into Biofilter3R is first represented as an **Entity** and assigned a unique `entity_id`. This structure enables a **central reference point** to which:

- Names (aliases, symbols, identifiers)
- Relationships (gene→pathway, protein→protein, etc.)
- Specialized data (e.g., gene-specific tables)

...can be attached consistently across different data types.

---

## 🧱 Core Models

Biofilter3R uses four tightly related models to implement its entity system:

| Model | Description |
| --- | --- |
| `Entity` | Core table. Stores object identity, status, and conflict flags |
| `EntityName` | Stores aliases and alternative identifiers for each Entity |
| `EntityGroup` | Optional grouping mechanism (e.g., Gene, Protein, Variant) |
| `EntityRelationship` | Represents directed relationships between two Entities |
| `EntityRelationshipType` | Defines semantic meaning of each relationship (e.g., "part_of") |

---

## 🔹 `Entity`

Represents the **master object**. It contains minimal fields for performance, but tracks two key statuses:

- `has_conflict`: Indicates whether the Entity is involved in a curation conflict
- `is_deactive`: Marks the Entity as inactive (due to merge, deletion, or manual curation)

These two fields define the **state of the entity**:

| Situation | `has_conflict` | `is_deactive` | Usage Guidance |
| --- | --- | --- | --- |
| ✅ Normal entity | False or None | None | Safe to use |
| ⚠️ Pending conflict | True | None | Use with caution; may be merged/removed |
| 🚫 Resolved with delete | True | True | Ignore in queries and ingestions |
| 🔀 Resolved with merge | True | True | Transfer aliases, ignore this entity |
| 🧹 Manually deprecated | None | True | Marked obsolete by manual review |

> 💡 Best practice: filter by is_deactive IS NULL to work only with active entities.
> 

---

## 🔹 `EntityName`

Every Entity may have multiple names — including symbols, aliases, IDs from external databases, and synonyms. These are stored in `EntityName`.

| Field | Description |
| --- | --- |
| `name` | The alias or identifier (e.g., "TP53", "P04637") |
| `is_primary` | Marks the preferred name (nullable boolean) |
| `data_source_id` | Tracks the origin of the name (e.g., HGNC) |

The presence of this table enables:

- Fuzzy search and cross-matching by name
- Display of preferred vs. secondary names
- Tracing names to their source systems

---

## 🔹 `EntityGroup`

Optional helper model used to **group entities** by type (e.g., Gene, Protein, Pathway). Currently not enforced in logic, but available for classification and UI filtering.

| Field | Description |
| --- | --- |
| `name` | Group name (e.g., "Gene") |
| `description` | Free text optional |

---

## 🔹 `EntityRelationship` and `EntityRelationshipType`

These models define **directed relationships between entities**, such as:

- Gene → Pathway
- Variant → Gene
- Protein → Protein (PPI)

| Field | Description |
| --- | --- |
| `entity_1_id` | Origin Entity (`from`) |
| `entity_2_id` | Target Entity (`to`) |
| `relationship_type_id` | Semantic meaning (`"encodes"`, `"interacts_with"`) |
| `data_source_id` | Source of the relationship |

The types of relationships are stored in `EntityRelationshipType`, including:

| Code | Description |
| --- | --- |
| `is_a` | Subclass of |
| `part_of` | Part of structure |
| `regulates` | Regulatory link |

---

## ⚙️ Design Considerations

To maximize speed and flexibility at scale, this module follows a **lean ORM strategy**:

- **No ForeignKey constraints**: All fields are stored as plain integers. This improves ingestion performance and avoids cascade complexity during high-volume inserts.
- **No `relationship()` ORM bindings**: This simplifies the models and reduces memory usage in large queries.
- **Minimal metadata**: Audit fields (`created_at`, `updated_at`) were intentionally removed in this first version.

This design allows the system to ingest **millions of omics records** quickly, while keeping the schema intuitive and expandable.

---

## 🧠 Summary

- The **Entity** model is the foundation of the Biofilter3R schema.
- All domain-specific objects (Gene, Protein, Variant...) are linked to a unique `Entity`.
- Names and relationships are managed via `EntityName` and `EntityRelationship`.
- The design prioritizes performance, extensibility, and clarity.
- Use `is_deactive` to exclude merged or deprecated entities in your queries.

> 🔍 Continue to explore other schema modules like Models > Gene to see how specific domains connect to the entity system.
>