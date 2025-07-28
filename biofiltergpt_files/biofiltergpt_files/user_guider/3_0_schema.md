# Schema

# 📖 What Is the Schema?

---

The **Biofilter3R schema** defines how biological and omics-related data are organized, represented, and related in the database. It was designed to balance **biological complexity** with **technical clarity**, allowing for:

- Standardized entity representation
- Traceability to source systems
- Easy access and extensibility across new domains

The schema is implemented using **SQLAlchemy**, a powerful Object Relational Mapper (ORM) that allows us to define **Python classes ("Models")** that map directly to database tables. This means:

- You interact with the data via Python classes and attributes
- Data is accessed **lazily**: queries only execute when needed
- All relationships are **navigable via class attributes**

> 🔗 For more on SQLAlchemy, visit the official documentation.
> 

---

## 🏗️ What Are "Models"?

In Biofilter3R, **every database table is represented by a Python class**, which we refer to as a **Model**. These models define the structure, constraints, and relationships of the data.

Each model lives in a dedicated Python module inside `biofilter/db/models/`, organized by domain. This modular architecture allows us to maintain clarity while enabling future growth.

---

## 🧩 Model Modules

The following model groups organize the system’s tables according to functionality and domain:

| File | Purpose |
| --- | --- |
| `config_models.py` | Models for internal configuration, settings, and control parameters |
| `curation_models.py` | Handles conflict tracking, curation resolutions, and ETL audits |
| `entity_models.py` | Core models for all **biological entities** and relationships |
| `etl_models.py` | Models related to data ingestion, logs, and provenance tracking |
| `genes_models.py` | Gene-specific models for efficient querying and biological enrichment |
| `go_models.py` | Models for **Gene Ontology (GO)** terms and their structure |
| `pathways_models.py` | Pathway entities and hierarchical relationships |
| `protein_models.py` | Models for proteins, isoforms, Pfam domains, and protein relationships |
| `variants_models.py` | Structures for genetic variants and gene-variant relationships |

Each of these files may contain several models that share a conceptual space. For example, `entity_models.py` contains `Entity`, `EntityName`, and `EntityRelation`, forming the core backbone of the Biofilter3R schema.

---

## 🔍 Model Metadata: `models_info.json`

To support development and exploration, Biofilter3R includes a helper file:

```json
<<models_info.json>>
"Gene": {
  "path": "biofilter.db.models.genes_models.Gene",
  "description": "Gene table with HGNC, Entrez, and Ensembl identifiers, linked to master Entity."
}
...
```

This file contains:

- Descriptions of each model
- Their full class paths (e.g., `"biofilter.db.models.genes_models.Gene"`)
- Annotations for use in query interfaces and documentation

This metadata will be explored in detail in the **Query Interface** section of the User Guide.

---

## ⚙️ Why SQLAlchemy?

Using SQLAlchemy brings several advantages:

- Abstracts direct SQL usage while maintaining performance
- Supports multiple backends (SQLite, PostgreSQL, etc.)
- Allows complex logic via **Pythonic relationships and mixins**
- Enables **lazy loading** and modular queries, scaling with large omics datasets

This makes it easier to extend the database, write reusable queries, and build interfaces — whether in Python notebooks, scripts, or interactive APIs.

---

## 🌱 Expandable by Design

A key goal of the Biofilter3R schema is **extensibility**. The system was designed to:

- Add new domains (e.g., metabolites, diseases, clinical attributes)
- Define new relationships between existing or new entities
- Preserve modularity and avoid breaking existing workflows

If a team wants to integrate a new domain — say, microbiome or chemical exposures — it can create a new `models/mydomain_models.py` file and follow the same architectural patterns.

> 🔧 Each new domain should:
> 
> - Define its own `Entity` link (via `entity_id`)
> - Use `EntityName` for aliases and synonyms
> - Optionally relate to other entities using `EntityRelation`

---

## 📌 Summary

- Biofilter3R uses SQLAlchemy to map Python classes ("Models") to database tables
- Models are organized into modular files by domain or purpose
- The schema is designed to be **navigable, lazy-loaded**, and **extensible**
- A helper file (`models_info.json`) supports documentation and querying
- New domains can be integrated without interfering with core logic

---

[Model: Entity](https://www.notion.so/Model-Entity-23ae7f9c0f238021ae5ff8c7f5823449?pvs=21)

[Model: Gene](https://www.notion.so/Model-Gene-23ae7f9c0f23803bb386da8556a544c2?pvs=21)

[Model: Protein](https://www.notion.so/Model-Protein-23ae7f9c0f23807b8717eea928d3a488?pvs=21)

[Model: Pathway](https://www.notion.so/Model-Pathway-23ae7f9c0f2380329940de1dc63b429c?pvs=21)

[Model: Variant](https://www.notion.so/Model-Variant-23ae7f9c0f238045a392c90e8be0842c?pvs=21)

[Model: Gene Ontology](https://www.notion.so/Model-Gene-Ontology-23ae7f9c0f23802c8793c26698623269?pvs=21)

[Model: ETL](https://www.notion.so/Model-ETL-23ae7f9c0f2380489ee2edac9b89cd9d?pvs=21)

[Model: Curation & Conflicts](https://www.notion.so/Model-Curation-Conflicts-23ae7f9c0f23805cbe63daac77a966fe?pvs=21)

[Model: Config & Metadata](https://www.notion.so/Model-Config-Metadata-23ae7f9c0f2380fa99f5f8ccc65dccfa?pvs=21)