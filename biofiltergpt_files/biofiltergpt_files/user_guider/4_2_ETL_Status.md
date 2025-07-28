# **ETL Status and History**

> 🧠 Audience: Developers, Data Analysts, Bioinformaticians
> 
> 
> 📌 Focus: Consult historical ETL executions to verify which data sources were updated, when, and with what outcome.
> 

---

### 🔄 Overview

Every time an ETL process is executed in Biofilter3R, a record is stored in the database under the `ETLProcess` table. This ensures that all data updates are traceable, including:

- ✅ Which data source was updated
- ⏰ When the update was executed
- ℹ️ What was the outcome (completed, failed, skipped)
- 📊 Summary information (rows inserted, warnings, etc.)

This information is essential for auditing and for understanding the current state of the database.

---

### ⚙️ How to Consult the ETL History

You can check the history of ETL runs using two approaches:

### 1. **Using the CLI**

```bash
$ biofilter report list
```

Displays all available reports. One of them is:

```bash
$ biofilter report run --name qry_etl_status
```

This will print a summary of all ETL processes executed, including:

- Data Source Name
- Execution Time
- Status (Completed / Failed)
- Biofilter Version used

If you want to export the report:

```bash
$ biofilter report run --name qry_etl_status --as-csv --output etl_status.csv
```

---

### 2. **Using Python Interface**

```python
from biofilter.biofilter import Biofilter
bf = Biofilter(db_uri="sqlite:///biofilter.sqlite")
df = bf.run_report(name="qry_etl_status", as_dataframe=True)
print(df.head())
```

![Screenshot 2025-07-25 at 10.33.03 AM.png](attachment:4d5473e5-5013-4507-9a57-b9e0d4486114:Screenshot_2025-07-25_at_10.33.03_AM.png)

This is useful for interactive notebooks or programmatic inspection.

---

### 📂 Under the Hood

The `qry_etl_status` report queries the `ETLProcess` model and joins with:

- `DataSource` (to display the source name)
- `SourceSystem` (to group by external provider)

Depending on the version of Biofilter3R, the report may also include:

- Total duration of ETL steps
- Number of warnings
- File paths (raw / processed)

---

### 🖊️ Notes

- This feature works independently of the data source or entity type.
- All ETL runs are logged automatically, no manual logging is needed.
- You can filter by `status` or `data_source` in the report for diagnostics.

---

### 📅 Future Enhancements

- Advanced filters in CLI (e.g., `-status failed`)
- Interactive HTML report generation
- Link to log files for deeper diagnostics