# Start a New Project - DB

> 📦 Audience: Developers, Data Managers, ETL Engineers
> 
> 
> 🎯 Goal: Initialize a new database to host Biofilter3R data
> 

---

## 📘 Overview

Creating a new Biofilter3R project means setting up a **fresh database** that will host:

- Biofilter3R's full schema
- Project metadata
- ETL tracking tables
- Master data for genes, variants, proteins, pathways, etc.

You can use this to:

- Create a sandbox for development
- Start a new curation initiative
- Load and organize omics data

---

## 🚀 Step-by-Step

### 1. Import the Biofilter class

```python
from biofilter import Biofilter
```

---

### 2. Create a new database

```python
bf = Biofilter()
bf.create_new_project("sqlite:///my_biofilter.db")
```

- This will:
    - Create all tables
    - Apply database migrations
    - Initialize metadata
- Supports SQLite or PostgreSQL:
    
    ```python
    bf.create_new_project("postgresql://user:pass@host:port/dbname")
    
    ```
    

---

### 3. Connect to the New Database

After creating, you can connect as usual:

```python
bf.connect_db("sqlite:///my_biofilter.db")
```

---

## ⚠️ Overwrite Existing Database

If the file or URI already exists and you want to replace it:

```python
bf.create_new_project("sqlite:///biofilter.db", overwrite=True)
```

> ⚠️ Use with caution! This will delete all existing data.
> 

---

## 🧠 What Happens Under the Hood?

When you run `create_new_project()`:

- A database connection is established
- All models are created
- Alembic migrations are applied (version-controlled schema)
- A metadata entry is stored (with timestamp, version, etc.)

No data is inserted yet — you’ll need to run the ETL pipeline to populate the tables.

---

## 🧪 Example: Starting a Clean SQLite Project

```python
from biofilter import Biofilter

bf = Biofilter()
bf.create_new_project("sqlite:///dev_biofilter.db")

print(bf.metadata)
```

---

## ✅ Next Steps

Now that the database exists, you can:

- Run the ETL pipeline:
    
    `bf.update(source_system=["hgnc", "ncbi"])`
    
- Explore with the Query interface:
    
    `q = Query(bf.db.get_session())`
    
- Import data manually or programmatically

---

## 📎 Related Pages

- Installation & Quick Start
- ETL Pipeline Overview
- Query Interface

---

### 🧬 Summary

Creating a new Biofilter project gives you full control over:

- Database location
- Schema creation
- Data ingestion
- Versioning and metadata

Whether you're managing omics pipelines or starting research from scratch, Biofilter3R makes it easy to bootstrap your infrastructure.