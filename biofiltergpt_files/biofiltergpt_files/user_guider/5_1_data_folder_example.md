# **Example: Querying HGVS Codes from Processed Parquet Files with DuckDB**

> 🧠 Audience: …
> 
> 
> 📌 Focus: …
> 

---

### 📂 Context

In Biofilter3R, the `processed/` folder contains normalized and cleaned data files in **Parquet format**, designed for fast and flexible access. These files may contain more attributes than those ingested into the main SQL database, including annotations like **HGVS nomenclature**.

Projects may wish to access this additional data using a NoSQL approach. This guide provides an example of how to query **HGVS codes** directly from Parquet files using **DuckDB**, a lightweight analytical SQL engine ideal for local querying.

---

### 🔗 Use Case

You want to search for a specific HGVS code (e.g., `NC_000001.11:g.123456A>T`) across all variant files from the **dbSNP** dataset stored in:

```
biofilter_data/processed/ncbi/dbsnp_chr*/data_mestre_{}.parquet
```

---

### ⚙️ Requirements

Install DuckDB:

```bash
pip install duckdb
```

Ensure the Parquet files contain a column named `hgvs`.

---

### 📄 Python Script

```python
import duckdb
import os
from pathlib import Path

# Base directory containing the Parquet files
BASE_DIR = Path("biofilter_data/processed/ncbi")
HGVS_QUERY = "NC_000001.11:g.123456A>T"  # Replace with your target HGVS code

def find_hgvs_in_dbsnp(hgvs_code: str):
    db_files = sorted(BASE_DIR.glob("dbsnp_chr*/processed_part_*.parquet"))
    if not db_files:
        print("❌ No matching files found under dbsnp_chr*/processed_part_*.parquet")
        return

    print(f"🔍 Searching for HGVS: {hgvs_code}")
    conn = duckdb.connect(database=':memory:')

    total_matches = 0
    for fpath in db_files:
        try:
            df = conn.execute(f"""
                SELECT * FROM read_parquet('{fpath}')
                WHERE hgvs = ?
            """, [hgvs_code]).fetchdf()

            if not df.empty:
                print(f"\n✅ Found in: {fpath.name}")
                print(df)
                total_matches += len(df)

        except Exception as e:
            print(f"⚠️ Error in {fpath.name}: {e}")

    if total_matches == 0:
        print("🔎 No results found.")
    else:
        print(f"\n🎯 Total matches: {total_matches}")

if __name__ == "__main__":
    find_hgvs_in_dbsnp(HGVS_QUERY)

```

---

### 🔍 Output

- Displays rows with matching `hgvs` code.
- Identifies which chromosome file(s) contain the result.
- Supports scanning hundreds of MBs of data quickly with low memory overhead.

---

### 📅 Notes

- Can be adapted to accept CLI arguments (e.g. via `argparse`).
- May be extended to export results to CSV.
- Efficient for ad-hoc annotation lookups.