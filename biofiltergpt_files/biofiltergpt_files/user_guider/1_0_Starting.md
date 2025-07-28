# Starting Biofilter3R

> 📦 Audience: Developers, Data Scientists, Bioinformaticians
> 
> 
> 🧭 Goal: Get the Biofilter3R tool installed and ready to use (no database creation yet)
> 

---

## 📥 Installation Options

You can install Biofilter3R in two ways:

### 1. From PyPI

```bash
pip install biofilter
```

### 2. From GitHub (development version)

```bash
git clone https://github.com/biofilter-dev/biofilter.git
cd biofilter
pip install -e .
```

> ✅ Tip: Use a virtual environment like venv or conda to isolate dependencies.
> 

---

## ✅ Requirements

- Python 3.9–3.12
- Recommended: `SQLAlchemy`, `pandas`, `pyarrow`, `openpyxl`, `sqlmodel`
- Compatible with:
    - SQLite (local or network path)
    - PostgreSQL (via standard URI)
    - Any SQLAlchemy-compatible backend

---

## ⚙️ Connect to an Existing Database

You **do not need to create a new database** to start using Biofilter3R.

If you already have a Biofilter-compatible database (e.g., from a shared server, cloud, or HPC), you can connect directly:

```python
from biofilter import Biofilter

# Connect to a local or remote database
bf = Biofilter("sqlite:///path/to/existing.db")
# or
bf = Biofilter("postgresql://user:pass@host:port/dbname")
```

Once connected, you can:

- Explore the data
- Run queries
- Generate reports
- Inspect metadata

No ETL or database creation is required unless you want to ingest new data.

---

## 🧪 Quick Test

```python
from biofilter import Biofilter

bf = Biofilter("sqlite:///biofilter.db")  # or use your server URI

print(bf.metadata)  # Show info about the project
```

---

## 🌐 Deployment Scenarios

Biofilter3R can connect to databases hosted in:

- ✅ Local files (`.db` via SQLite)
- ✅ Shared folders or HPC clusters
- ✅ Remote servers (e.g., PostgreSQL on AWS/RDS, GCP, Supabase, etc.)
- ✅ Containers or mounted volumes

---

## 📎 Related Pages

- Start a New Project (Create Database)
- ETL Pipeline Overview
- Query Interface

---

### 💡 Summary

- You can install Biofilter via `pip` or `git`.
- You **don’t** need to create a new database to use it.
- Biofilter works with **any existing database** that follows the expected schema.
- Perfect for analysis, curation, reporting, and data exploration without rebuilding data.

---

[Start a New Project - DB](https://www.notion.so/Start-a-New-Project-DB-23ae7f9c0f2380958bc1fd8be7650cb5?pvs=21)

[Start a New Project - Seeds](https://www.notion.so/Start-a-New-Project-Seeds-23be7f9c0f23806f95e4eb63bc797773?pvs=21)

[System Configuration Table (`system_config`)](https://www.notion.so/System-Configuration-Table-system_config-23be7f9c0f23807dbc0cd44ecb3b95a1?pvs=21)

[`.biofilter.toml` Configuration File](https://www.notion.so/biofilter-toml-Configuration-File-23be7f9c0f23802394e8cd8eb43ccc26?pvs=21)

[`biofilter.log` - Understanding  Logs](https://www.notion.so/biofilter-log-Understanding-Logs-23be7f9c0f2380ad923ece4c8a134e84?pvs=21)

[Database Versioning with Alembic](https://www.notion.so/Database-Versioning-with-Alembic-23be7f9c0f23800490d1d82b5b8c4a7e?pvs=21)