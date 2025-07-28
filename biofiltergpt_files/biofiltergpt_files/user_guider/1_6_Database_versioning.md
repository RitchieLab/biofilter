# Database Versioning with Alembic

> 🧠 Audience: Developers and Database Administrators
> 
> 
> 📌 Focus: Understand how Biofilter3R manages schema updates with Alembic
> 

---

### 🧬 What is Alembic?

[Alembic](https://alembic.sqlalchemy.org/) is a lightweight database migration tool used by Biofilter3R to manage schema evolution over time.

Every time the Biofilter3R schema is updated (e.g., new tables, columns, or relationships), Alembic ensures your database stays compatible.

---

### 📁 Where Is Alembic Configured?

You’ll find Alembic inside the main project folder:

```
biofilter/
└── alembic/
    ├── env.py
    ├── versions/
    └── alembic.ini
```

- The `versions/` folder contains migration scripts.
- Migrations are tied to the internal `schema_version` tracked in the table `biofilter_metadata`.

---

## ⚙️ How It Works in Biofilter3R

The versioning is fully integrated via:

### ✅ `Biofilter.migrate()`

You can run migrations programmatically:

```python
from biofilter import Biofilter

bf = Biofilter(db_uri="sqlite:///your_database.db")
bf.migrate()
```

Internally, this method:

- Reads the current schema version from `biofilter_metadata`
- Compares it with the current code version
- Runs Alembic migrations only if needed
- Updates the schema version on success

---

## 🖥️ CLI: `biofilter project migrate`

You can also trigger migrations using the command-line interface:

```bash
biofilter project migrate --db-uri sqlite:///biofilter.sqlite
```

If you omit `--db-uri`, Biofilter will try to read it from `.biofilter.toml`.

---

## 🔄 Auto-Migration Logic

Behind the scenes, the `run_migration()` function performs the logic:

1. It checks the current schema version stored in the database.
2. Compares it with the Biofilter3R version from the code.
3. If the schema is outdated:
    - Alembic runs the necessary upgrade steps.
    - The version is updated in `biofilter_metadata`.

Example output:

```
📦 Current schema: 3.0.0 | Target version: 3.1.0
🚀 Running Alembic migrations...
✅ Migration completed: 3.0.0 → 3.1.0
```

If up-to-date:

```
✅ Schema already up-to-date. No migration needed.
```

---

## 🛠️ Advanced Alembic Commands (Optional)

In rare cases, you may need to manage Alembic directly.

```bash
alembic current        # View current schema version
alembic history        # Show migration history
alembic upgrade head   # Apply all pending migrations
alembic revision --autogenerate -m "New table or column"
```

Only run these if you’re comfortable managing migrations manually.

---

## 📌 Notes

- Migrations **do not affect data** — they only change schema.
- The system will **never downgrade** automatically.
- If using `create_new_project()`, a fresh schema is created with no need for migrations.

---

## 📎 Footnote

Biofilter3R uses versioned schemas. In the future, we plan to introduce more advanced **schema audit** and **migration rollback** tools.