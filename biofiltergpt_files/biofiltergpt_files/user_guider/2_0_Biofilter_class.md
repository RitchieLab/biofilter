# `Biofilter` Class

> Audience: Developers, Data Scientists, ETL Engineers
> 
> 
> **Purpose**: Main entry point for interacting with the Biofilter3R system
> 
> **Tags**: Core Interface, ETL, Settings, Conflict, Reports, Query
> 

---

## 🔍 What Is `Biofilter`?

The `Biofilter` class is the **central interface** of the Biofilter3R system. It acts as a unified access point for all major operations, including:

- Connecting to the database
- Managing settings
- Running ETL pipelines
- Handling data curation conflicts
- Exploring models
- Generating reports

---

## 🚀 Why It Matters

Without needing to directly use each internal manager (like `ETLManager`, `ConflictManager`, etc.), you can do everything from this one class:

✅ Initialize

✅ Configure

✅ Update

✅ Explore

✅ Report

---

## ⚙️ Instantiation

```python
from biofilter import Biofilter

# Connect to an existing project
bf = Biofilter("sqlite:///my_biofilter.db")
```

---

## 🧰 Main Functionalities

| Method | Purpose |
| --- | --- |
| `create_new_project()` | Create a new database project |
| `connect_db()` | Connect to an existing database |
| `settings` (property) | Load and access global configuration |
| `update()` | Run ETL for selected source systems |
| `restart_etl()` | Restart ETL (deleting cached files and restarting from scratch) |
| `update_conflicts()` | Run ETL for conflict resolution using a manually curated CSV |
| `export_conflicts_to_excel()` | Export unresolved curation conflicts |
| `import_conflicts_from_excel()` | Import manually curated resolutions |
| `get_metadata()` | Fetch metadata from `biofilter_metadata` table |
| `migrate()` | Run Alembic migrations |
| `model_explorer()` | Return an object for schema/model inspection |
| `list_reports()` | List all registered reports |
| `run_report(name)` | Execute a report by name |

---

## 🧪 Quick Example

```python
# Instantiate
bf = Biofilter("sqlite:///biofilter.db")

# Run full ETL for selected systems
bf.update(source_system=["hgnc", "ncbi"])

# Restart ETL if something went wrong
bf.restart_etl(data_source=["dbsnp"])

# Export conflicts to Excel for manual curation
bf.export_conflicts_to_excel("curation.xlsx")

# Import resolved conflicts
bf.import_conflicts_from_excel("curation_resolved.xlsx")

# Run a report
bf.run_report("gene_summary")

```

---

## 🧠 Tips

- You **do not** need to access `ETLManager`, `ConflictManager`, or `ReportManager` directly — all common use cases are covered by this class.
- This interface was designed for **scripts**, **notebooks**, and **automation pipelines**.
- You can still access advanced managers if needed — this class does **not** restrict functionality, just makes it easier to start.

---

## 📎 Related Pages

- Query Interface
- ETL Overview
- Conflict Curation
- Settings System
- Reports Module

---

## ✅ Summary

The `Biofilter` class is your **gateway** to everything in Biofilter3R. With just a few lines of Python, you can:

- Run full ETL pipelines
- Curate and resolve conflicts
- Explore the database structure
- Execute reports
- Manage your system settings

Whether you're developing new features or running analyses, this is the interface to start with.