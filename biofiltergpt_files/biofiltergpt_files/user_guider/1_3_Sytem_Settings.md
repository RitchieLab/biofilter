# System Configuration Table (`system_config`)

> 📁 Subpage of: Seeds: Initializing the Biofilter3R Schema
> 
> 
> 🧠 Audience: Users and system administrators
> 

---

## 📌 Purpose

The `system_config` table defines **core runtime parameters** that influence how Biofilter3R operates.

These settings are loaded automatically during database initialization (via seed files) and can be queried or updated programmatically using the Biofilter interface.

Each configuration option controls a specific behavior of the ETL engine or file system handling.

---

## 🧾 Sample Configuration Entries

| Key | Type | Description |
| --- | --- | --- |
| `store_all_snps` | `bool` | If `true`, all SNPs will be stored, even unvalidated or unsupported ones |
| `convert_merged_snps` | `bool` | Enables automatic conversion of merged SNPs to current RSIDs |
| `download_path` | `path` | Directory where raw data will be stored |
| `processed_path` | `path` | Directory where transformed files will be saved |
| `keep_raw_files` | `bool` | If `false`, raw files will be deleted after ETL |
| `keep_processed_files` | `bool` | If `true`, processed files will be retained for auditing |

---

## 🔐 Editable Values

- Each configuration has a field `editable`.
    
    If `editable = 1`, users can update that setting via scripts or future UI.
    
- If `editable = 0`, the configuration is **locked** and must not be altered unless by a developer or system admin.

---

## 💾 Where is this initialized?

This table is seeded automatically via:

```
biofilter/db/seed/initial_config.json
```

Users do **not** need to manually edit this file — the system reads it when a new database is created.

---

## ⚠️ Why It Matters

The values in `system_config` **directly affect ETL execution**:

- Improper paths may result in broken downloads or processing.
- Changing flags (like `keep_raw_files`) can save disk space or preserve audit logs.
- Merging SNPs incorrectly may lead to inconsistent variant representations.

Because of this, only change configuration values **if you understand the impact**.

---

## 🛠️ How to View or Modify Configs

If you need to review or change these settings:

```python
biofilter = Biofilter("sqlite:///my.db")
biofilter.settings.get("download_path")
biofilter.settings.set("download_path", "/custom/data/raw")
```

Or use the CLI (future support planned).

---

## 🧠 Summary

- The `system_config` table stores essential system settings for ETL and file management.
- These values are seeded automatically and editable depending on their metadata.
- Incorrect values may lead to broken processes — handle with care.

---

> 🔎 For more advanced usage or modifying configurations in bulk, see the Developer Guide – System Settings and Parameters.
> 

---

**📝 Note:**

As the system is still under active development, the structure and content of this table may change in future versions.