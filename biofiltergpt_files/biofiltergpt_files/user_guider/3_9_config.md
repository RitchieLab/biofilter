# Model: Config & Metadata

> 🧠 Audience: Developers, Administrators
> 
> 
> 📌 Focus: System-wide configuration and schema metadata management
> 

---

## 🔧 Why Configuration Models?

To make Biofilter3R more **flexible, dynamic, and maintainable**, some settings and global parameters are stored directly in the database rather than hardcoded.

This allows:

- Adjusting key system behaviors **without modifying the code**
- Tracking and managing **schema versions**
- Defining parameters that may be exposed or updated via a future **admin interface**

---

## 📚 Config Schema Overview

| Model | Description |
| --- | --- |
| `SystemConfig` | Stores key–value pairs for runtime or global configurations |
| `BiofilterMetadata` | Tracks schema and ETL versions deployed in the current system |

---

## 🔹 `SystemConfig`

A lightweight but powerful model used to store **runtime parameters** in key–value format.

| Field | Description |
| --- | --- |
| `key` | Unique identifier of the configuration (e.g., `"default_grch"`) |
| `value` | Stored as a string; type interpretation is defined separately |
| `type` | Data type hint (`string`, `int`, `bool`, `float`, etc.) |
| `description` | Optional description to aid documentation or UI rendering |
| `editable` | Boolean flag: can this config be modified externally? |
| `created_at` | Timestamp (UTC) of when it was created |
| `updated_at` | Timestamp (UTC) of last modification |

### 🧠 Design Highlights

- ✅ **Key/Value Model**: Easy to extend, no migrations required for new settings
- 🧩 **Editable Control**: Prevents unintentional modifications of critical values
- 📖 **Description Field**: Great for self-documenting configurations
- 📊 **Audit Ready**: Includes timestamps for change tracking

> 🔒 No encryption is applied by default — sensitive values should be handled securely at the application layer.
> 

---

## 🔹 `BiofilterMetadata`

Stores **versioning and descriptive metadata** about the current Biofilter3R deployment.

| Field | Description |
| --- | --- |
| `schema_version` | Semantic version of the current DB schema |
| `etl_version` | Version of the ETL logic in use |
| `description` | Optional notes or deployment context |
| `created_at` | Timestamp (UTC) of the deployment |

This model allows developers and system administrators to:

- Validate compatibility between **ETL pipelines and the database schema**
- Track **migrations** and **version upgrades**
- Provide **runtime introspection** of the system state

---

## 🔮 Future Considerations

- 🔁 **Validation layer** for config types and accepted values
- 🧾 **Change history** or audit log for tracking config edits
- 🛠️ **Admin UI** for visual management of config flags and metadata

---

## 🧠 Summary

- `SystemConfig` allows centralized, flexible configuration of the Biofilter3R system
- `BiofilterMetadata` tracks schema and ETL versioning for compatibility and auditing
- Both models support extensibility and runtime introspection
- They are essential for managing **production deployments** and **controlled environments**

> 👉 For guidance on modifying configs or deploying a new schema version, see:
> 
> 
> User Guide > Admin & Deployment
>