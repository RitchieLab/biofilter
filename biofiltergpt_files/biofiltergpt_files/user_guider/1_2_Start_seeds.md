# Start a New Project - Seeds

> 📁 Subpage of: Start a New Biofilter3R Project
> 
> 
> 🧠 Audience: Users initializing a fresh Biofilter3R instance
> 

---

## 🧭 What Are Seeds?

In Biofilter3R, **seeds** are curated files that initialize key reference data required for the system to operate correctly.

They populate essential tables such as:

- Source Systems
- Data Sources and ETL Processes
- Entity Relationship Types
- Omic Status Definitions
- Genome Assemblies and Metadata

These values provide stable defaults and controlled vocabularies used throughout the system.

---

## 🚀 Do I Need to Run Anything?

### **No!!**

As a user, you do **not** need to run any commands or worry about loading the seeds manually.

They are automatically processed and loaded when you start a new Biofilter3R project using:

```python
bf.create_new_project("sqlite:///your.db")
```

The system detects empty tables and loads the seeds only when needed — making the process **automatic, safe, and repeatable**.

---

## 📁 Where Are Seeds Stored?

All seed files are stored in the following directory inside the Biofilter3R package:

```
biofilter/db/seed/
```

These files are in `.json` format and include:

| File Name | Description |
| --- | --- |
| `initial_config.json` | Default settings and paths |
| `initial_data_sources.json` | ETL DataSource definitions |
| `initial_entity_group.json` | Entity group categories |
| `initial_entity_relationship_types.json` | Types of entity-to-entity relationships |
| `initial_etl_processes.json` | ETL process presets for each DataSource |
| `initial_genome_assemblies.json` | Supported genome assemblies list |
| `initial_genome_assembly.json` | Default active genome assembly |
| `initial_metadata.json` | Project-level metadata |
| `initial_omic_status.json` | Omic status tags for genes, variants, etc. |
| `initial_source_systems.json` | Supported source systems (HGNC, dbSNP, etc) |

---

## 🔄 Future Enhancements

It is in our **product roadmap** to build an interface that allows:

- **Incremental seed updates**
- **Schema-aware DTP updates**
- **Cold Start refresh with historical control**

This will allow the Biofilter3R system to reprocess DTPs or schema evolutions **safely and automatically**, always keeping the seeds in sync with current standards.

---

## 📌 Summary

- You **do not** need to manage seeds manually.
- Seeds are **automatically** loaded during project initialization.
- They define **stable reference data** needed across the system.
- Seeded tables include source systems, entity groups, assemblies, ETL presets, and more.
- Updates to seeds will be **incrementally supported** in future versions.

---

> 🔎 For more technical details about how seeds are structured or how to create custom ones, see the Developer Guide – Seeds & Initialization page.
>