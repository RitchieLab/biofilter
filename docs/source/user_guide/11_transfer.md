# 🗄️ Biofilter Database Transfer Interface
_Backup, Restore, Export & Import_

Biofilter 4 provides a unified interface to **preserve, clone, and share databases**
without re-running the entire ETL pipeline.  
This interface supports two complementary use cases:

1. **Physical snapshots (backup / restore)**
2. **Logical full clones (export / import)**

Both approaches are designed to be **safe, reproducible, and explicit**.

---

## 📌 Core Concepts

### 1️⃣ Snapshot (Backup / Restore)

A **physical copy** of the database.

**Best for**
- Fast backups
- Rollbacks
- Environment migration
- Disaster recovery

**Implementation**
- **SQLite** → direct file copy
- **PostgreSQL** → `pg_dump` / `pg_restore`

Snapshots are **engine-specific** and restore the database exactly as-is.

---

### 2️⃣ Full Clone (Export / Import)

A **logical, engine-agnostic clone** of the database.

- One file per table
- Preserves **Primary Keys** and **Foreign Keys**
- Rebuilds the database state exactly as exported

**Best for**
- Scientific data sharing
- Publishing curated knowledge bases
- Moving data across engines (SQLite ↔ PostgreSQL)

**Supported formats**
- `parquet` (recommended)
- `csv`

---

## 🧱 Architecture Overview

```

Biofilter
└── TransferComponent
├── backup()      → physical snapshot
├── restore()     → restore snapshot
├── export()      → logical full clone
└── import_()     → restore logical clone

````

Key design principles:
- Uses **Database.engine** and **SQLAlchemy Core**
- No dependency on ETL state
- No long-lived sessions
- Safe to run independently of pipelines or reports

---

## 💻 CLI Interface

All commands live under:

```bash
biofilter db
````

---

### 🔹 Backup (Snapshot)

```bash
biofilter db backup --out snapshot.sqlite
```

**Description**

* Creates a physical backup of the current database.

**Notes**

* SQLite: copies the `.sqlite` file
* PostgreSQL: creates a dump

---

### 🔹 Restore (Snapshot)

```bash
biofilter db restore --in snapshot.sqlite
```

⚠️ **Destructive operation**
The current database is replaced.

**What happens**

1. Existing database is overwritten
2. Engine is reconnected
3. `bootstrap_models()` is reapplied

---

### 🔹 Export (Full Clone)

```bash
biofilter db export --out ./bundle --format parquet
```

**Generated structure**

```
bundle/
 ├── manifest.json
 └── tables/
     ├── entity.parquet
     ├── gene.parquet
     ├── variant.parquet
     └── ...
```

**Options**

```bash
--format parquet|csv
--schema-version 4.0.0
--chunksize 250000
```

---

### 🔹 Import (Full Clone)

```bash
biofilter db import --in ./bundle
```

⚠️ **Destructive operation**

**Import workflow**

1. Database must already exist
2. All tables are truncated
3. Data is reinserted preserving IDs
4. PostgreSQL sequences are reset
5. Indexes are rebuilt (default)

**Useful flags**

```bash
--no-rebuild-indexes
--no-reset-sequences
```

---

## 🐍 Python API Interface

### Common setup

```python
from biofilter.biofilter import Biofilter

bf = Biofilter(db_uri="sqlite:///biofilter.db")
bf.db.connect()
```

---

### 🔹 Backup (Snapshot)

```python
bf.transfer.backup("snapshot.sqlite")
```

---

### 🔹 Restore (Snapshot)

```python
bf.transfer.restore("snapshot.sqlite")
```

> The database connection is automatically re-established after restore.

---

### 🔹 Export (Full Clone)

```python
bf.transfer.export(
    out_dir="./bundle",
    fmt="parquet",
    schema_version="4.0.0"
)
```

---

### 🔹 Import (Full Clone)

```python
bf.transfer.import_(
    in_dir="./bundle",
    fmt="parquet",
    rebuild_indexes=True,
    reset_postgres_sequences=True,
)
```

---

## 🔐 Important Rules

### ✔️ Import does NOT create the schema

* The database schema must already exist
* Use:

  * `biofilter project create`
  * migrations
  * or a restored snapshot

---

### ✔️ IDs are preserved

* Primary Keys and Foreign Keys are kept intact
* Enables exact database replication

---

### ✔️ Seeds and system data

* Import truncates all tables
* Seeds must be:

  * included in the export bundle, or
  * recreated explicitly

---

## 🧪 Common Use Cases

### 🔁 Clone a database quickly

```bash
biofilter db export --out ./clone
biofilter project create --db-uri sqlite:///clone.db --overwrite
biofilter --db-uri sqlite:///clone.db db import --in ./clone
```

---

### 🧬 Share a curated scientific database

* Export → compress → distribute
* Import → analyze locally without ETL

---

### 🧯 Safe backup before ETL

```bash
biofilter db backup --out pre_etl.sqlite
```

---

## 🚀 Planned Extensions

* Interactive confirmation (`--yes`) for destructive commands
* Automatic compression (`.tar.gz`)
* Partial exports by `data_source_id`
* Provenance and license metadata in `manifest.json`
* Integrity checksums and signatures
* Automated tests for `TransferComponent`

---

## ✅ Summary

The Database Transfer Interface makes Biofilter a **persistent, portable, and reproducible knowledge platform**.

It enables:

* Fast recovery
* Reliable cloning
* Scientific data sharing
* ETL-free reuse of curated knowledge

This interface **complements** ETL and reporting — it does not replace them.

---

