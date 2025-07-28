# `.biofilter.toml` Configuration File

> 📁 Subpage of: Settings & System Behavior
> 
> 
> 🧠 Audience: Users and developers configuring their working environment
> 

---

## 📌 What Is It?

The `.biofilter.toml` file is an **optional user-specific configuration file** that allows you to define custom settings that should be loaded **before** the Biofilter system initializes.

It serves as a way to persist default runtime preferences outside your code — especially useful for setting the database URI automatically without hardcoding it or passing it programmatically each time.

---

## 🧾 Minimal Example

```toml
[database]
db_uri = "sqlite:///dev_biofilter.db"
```

This configuration defines the default database connection string that will be used when instantiating the `Biofilter` class.

---

## 🔍 How It Works

- When you instantiate a `Biofilter()` object **without passing `db_uri`**, the system checks whether a `.biofilter.toml` file exists.
- If found, it reads the `db_uri` from it and uses it to initialize the connection.
- If no `db_uri` is found **either in the argument or in the TOML file**, an error is raised:
    
    > ❌ Database not connected. Use connect_db() first.
    > 

---

## 📁 Where to Place It?

The file must be placed in your **current working directory** (typically the root of your project or the directory from which you're running your script or notebook):

```
project/
│
├── my_script.py
├── .biofilter.toml  ✅
└── ...
```

It is **not tied to the database** itself but rather to the **local Python environment** and the instance running the Biofilter code.

> ℹ️ This makes it ideal for development environments, allowing different developers or systems to use different configurations without changing code.
> 

---

## 🛠️ Current Supported Fields

| Section | Key | Description |
| --- | --- | --- |
| `[database]` | `db_uri` | URI of the database to connect to on startup |

---

## 🧭 Future Extensions

While the current version only supports the `db_uri`, this mechanism will be expanded in future versions to support:

- Default logging level
- ETL paths (`download_path`, `processed_path`)
- Developer flags (e.g., debug mode, dry-run mode)
- Report output preferences

Stay tuned in the [Changelog](https://chatgpt.com/c/notion-link-to-changelog) for updates.

---

## ✅ Summary

- `.biofilter.toml` provides **pre-load configuration** for your Biofilter runtime.
- It allows for seamless setup without modifying scripts.
- Only one active file is read — it is **project-specific**, not database-specific.
- Currently, it supports setting `db_uri`, but it is expected to grow with new options.

---

📎 **Pro tip:** Commit this file to your `.gitignore` if you use different database paths across environments.