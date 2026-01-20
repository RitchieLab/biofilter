# 🧬 Biofilter 4 – Database Roles & Access Control (Production)

This document describes **how database access is structured in production**, how to **recreate roles after a database drop**, and **which connection strings must be used for each type of operation**.

This setup is designed to support:

* Safe public read-only access
* Controlled ETL operations
* Full administrative schema control
* Easy recovery after database rebuilds

---

## 📌 Overview

Production database:

```
biofilter_prod
```

PostgreSQL host:

```
127.0.0.1:5432
```

The database access model is intentionally **role-based**, separating responsibilities and minimizing risk.

---

## 👥 Roles & Responsibilities

### 1️⃣ `bioadmin` — Database Owner / Administrator

**Purpose**

* Owns the database and schema
* Creates / drops tables
* Runs migrations and bootstrap
* Creates indexes and partitions
* Runs `biofilter project create`

**Capabilities**

* Full DDL + DML
* DROP / ALTER tables
* DROP / CREATE indexes
* DROP database

**Connection string**

```text
postgresql+psycopg2://bioadmin:<PASSWORD>@127.0.0.1:5432/biofilter_prod
```

**When to use**

* Initial database creation
* Schema changes
* Partition logic
* Debugging low-level DB issues

---

### 2️⃣ `bioprod` — ETL / Write Operator

**Purpose**

* Run ETL pipelines
* Insert, update and delete data
* Create / drop indexes (via owner privileges already granted)
* Perform data maintenance tasks

**Capabilities**

* SELECT / INSERT / UPDATE / DELETE
* USAGE on sequences
* ❌ Cannot create or drop tables
* ❌ Cannot alter schema
* ❌ Cannot drop database

**Connection string**

```text
postgresql+psycopg2://bioprod:<PASSWORD>@127.0.0.1:5432/biofilter_prod
```

**When to use**

* `biofilter etl update`
* Rebuilding indexes
* Reloading data
* Bulk maintenance tasks

---

### 3️⃣ `biousers` — Public / Read-Only Users

**Purpose**

* External users
* Notebooks
* Analysis pipelines
* Dashboards
* Read-only services

**Capabilities**

* SELECT on all tables
* SELECT / USAGE on sequences
* ❌ No writes
* ❌ No deletes
* ❌ No schema changes

**Connection string**

```text
postgresql+psycopg2://biousers:<PASSWORD>@127.0.0.1:5432/biofilter_prod
```

**When to use**

* Any public or shared query
* Jupyter notebooks
* External collaborators
* Production analytics

---

## 📂 Role Management Script

All roles and permissions are defined in:

```text
scripts/db/roles_prod.sql
```

This script is:

* ✅ **Idempotent** (safe to run multiple times)
* ✅ **Recoverable** (can be rerun after DB rebuild)
* ✅ **Version controlled**
* ❌ Does not contain real passwords (placeholders only)

---

## 🚀 How to Run the Script

### 1️⃣ Prerequisites

* PostgreSQL running
* Database `biofilter_prod` already exists
* You have access as `bioadmin` or `postgres`

---

### 2️⃣ Run the script

```bash
psql -U bioadmin -d postgres -f scripts/db/roles_prod.sql
```

> ⚠️ The script connects internally to `biofilter_prod` using `\c`.

---

### 3️⃣ Set real passwords (mandatory)

After the script runs, **set secure passwords manually**:

```sql
ALTER ROLE bioprod  PASSWORD 'STRONG_PASSWORD_HERE';
ALTER ROLE biousers PASSWORD 'STRONG_PASSWORD_HERE';
```

---

## 🔁 When Should This Script Be Run?

### ✅ Required

* After **dropping and recreating** `biofilter_prod`
* After **restoring from backup**
* When **setting up a new production VPS**
* When **roles were accidentally modified**

### ❌ Not required

* Normal ETL runs
* Normal queries
* Code deployments

---

## 🧪 Typical Operational Flow

### 1️⃣ Create database (admin only)

```bash
biofilter project create \
  --db-uri postgresql+psycopg2://bioadmin:...@127.0.0.1:5432/biofilter_prod
```

---

### 2️⃣ Apply roles

```bash
psql -U bioadmin -d postgres -f scripts/db/roles_prod.sql
```

---

### 3️⃣ Run ETL

```bash
biofilter etl update --data-source hgnc
```

(using `bioprod` credentials in `.biofilter.toml`)

---

### 4️⃣ Public access

Users configure `.biofilter.toml` with:

```toml
[database]
db_uri = "postgresql+psycopg2://biousers:*****@127.0.0.1:5432/biofilter_prod"
```

---

## 🔐 Security Notes (Important)

* **Never** use `bioadmin` credentials in notebooks or public services
* **Never** expose write credentials to external users
* Password rotation does **not** require schema changes
* This model allows **safe public access without API layer**

---

## 🧠 Design Rationale (Why This Matters)

This role separation:

* Prevents accidental schema destruction
* Allows safe sharing of the database
* Makes production reproducible
* Supports Knowledge-as-a-Service (KaaS) style usage
* Scales naturally to future API or DNS-based access

---

## 🔮 Future Extensions (Optional)

* Dedicated schema (e.g. `kaas`)
* Read-only replicas
* API layer on top of `biousers`
* Role-based access per entity group
* Automated role provisioning in `biofilter project create`

---

## ✅ TL;DR

| Task                | Role             |
| ------------------- | ---------------- |
| Create / drop DB    | `bioadmin`       |
| Schema changes      | `bioadmin`       |
| ETL & data load     | `bioprod`        |
| Public queries      | `biousers`       |
| Recover permissions | `roles_prod.sql` |

---

If you want next:

* I can generate `check_roles.sql` (audit permissions)
* Or a `roles_dev.sql`
* Or document `.biofilter.toml` best practices
* Or align this with a future API / DNS abstraction
