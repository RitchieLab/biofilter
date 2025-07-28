# **Biofilter Data Folders: First step to a Data Lake**

> 🧠 Audience: Developers, Data Engineers, Bioinformaticians
> 

> 📌 Focus: Understand the structure, purpose, and usage of the `biofilter_data/` folder, including raw and processed data layers used during the ETL process.
> 

### 

---

### 📁 What is `biofilter_data/`?

The `biofilter_data/` directory is the default root folder used by Biofilter3R to manage data files related to ETL operations. This includes:

- Files downloaded from external data sources
- Transformed (cleaned/normalized) files ready for ingestion

This directory structure follows a **Data Lake** paradigm, where raw and processed stages are separated and organized.

---

### ⚙️ Configurable Location

The location of this folder can be customized via the `system_config` table in the Biofilter3R database. A key named `data_root_dir` determines the root path.

Example:

```bash
/data/projects/biofilter_data/
```

---

### 📦 `raw/`: Original Data

This subfolder stores all original files exactly as downloaded from the external sources (e.g., dbSNP JSON, UniProt XML, HGNC TSV).

Typical path:

```
raw/<source_system>/<data_source>/
```

Examples:

```
raw/NCBI/dbsnp/dbsnp.json.bz2
raw/UniProt/uniprot_proteome.xml
```

These files are retained for reproducibility and reprocessing.

---

### 🧹 `processed/`: Cleaned & Normalized Data

The `processed/` subfolder contains files that have been transformed by the DTP into a standard format ready for database loading.

Typical path:

```
processed/<source_system>/<data_source>/
```

These files are in `.parquet` format and include:

- `data_master.parquet`: Cleaned and structured entries for loading into Biofilter domains (e.g., Genes, Variants)
- `relationship.parquet`: Relationship records between entities (e.g., Gene ↔ Pathway)

Parquet was chosen for being an open standard in data lake architectures and highly optimized for analytics and NoSQL-style queries.

---

### 🔍 Why Keep Extra Info in Processed Files?

Often, `processed/` files contain more information than what is loaded into the SQL database. These include:

- Alternate identifiers and synonyms
- HGVS notation for variants
- Structural annotations or flags

These extra fields can be used in:

- Annotation queries outside the main database
- External projects that require richer metadata
- Indexing pipelines or Data Catalogs

Example:

> The data_master.parquet for Variants includes all variant types (SNVs, InDels, deletions) and HGVS strings, even though the SQL tables only ingest SNVs initially.
> 

---

### 🔄 Reusability and Expansion

Because this system decouples the **transform** and **load** steps, other tools or pipelines can use the processed data without altering the SQL schema. This allows:

- Integration with NoSQL or external search systems
- Flexible schema evolution
- Rich data exports for collaborators

---

### ✅ Summary

| Layer | Purpose | Format | Usage |
| --- | --- | --- | --- |
| `raw/` | Preserve original source files | Original | Reproducibility & validation |
| `processed/` | Cleaned, structured data | `.parquet` | Load into DB or external usage |

These layers are essential for making Biofilter3R scalable, reproducible, and extensible across different research and clinical contexts.

[**Example: Querying HGVS Codes from Processed Parquet Files with DuckDB**](https://www.notion.so/Example-Querying-HGVS-Codes-from-Processed-Parquet-Files-with-DuckDB-23be7f9c0f23805bbb9cedee11c7c7b9?pvs=21)