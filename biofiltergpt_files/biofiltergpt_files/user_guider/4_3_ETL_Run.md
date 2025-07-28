> 🧠 Audience:  Developers, Data Engineers, Bioinformaticians
> 
> 
> 📌 Focus:  ETL executions to update data sources in the DB
> 

---

### 🧪 Overview

This page explains how to **run an update for a specific data source** using the Biofilter3R ETL engine. This process ensures that the latest version of external omics data is extracted, transformed, and loaded into the Biofilter3R database.

---

### 🤖 When to Run an Update

You should run an update when:

- New data is released from an external provider (e.g., new UniProt or dbSNP dump)
- A bug in a previous ETL run was fixed and you need to reprocess the source
- The schema or logic of the `transform()` or `load()` method was updated

---

### 🔧 1. CLI Command

You can run a data source update using the CLI with the following pattern:

```bash
$ biofilter etl update --data-source <DATASOURCE_NAME> --db-uri <DATABASE_URI>
```

### Example:

```bash
$ biofilter etl update --data-source dbsnp_chrx --db-uri sqlite:///dev_biofilter.db
```

This command will:

- Check if raw data exists (download if not or if forced)
- Transform the data into a standardized format (usually CSV or Parquet)
- Load the structured content into the Biofilter3R database
- Log the process and register a new ETLProcess record

---

### ⚠️ Tips

- Always check the `biofilter.log` and `ETLProcess` table after a run
- Use the `-force-steps` flag if you want to rerun `extract` and `transform` even if processed files exist

### Force Example:

```bash
$ biofilter etl update --source-system HGNC --db-uri sqlite:///dev_biofilter.db --force-steps
```

### 🔧 2. **Using Python Interface**

```python
from biofilter import Biofilter

db_uri = "sqlite:///dev_biofilter.db"

# Configure below
data_sources_to_process = [
    # Genes
    "hgnc",
    "gene_ncbi",
    # Proteins
    "pfam",
    "uniprot",
    # Pathways
    "reactome",
    "kegg_pathways",
    # Gene Ontology
    "gene_ontology",
    # Variants
    "dbsnp_sample",
    "dbsnp_chr1",
    "dbsnp_chr2",
    "dbsnp_chr3",
    "dbsnp_chr4",
    "dbsnp_chr5",
    "dbsnp_chr6",
    "dbsnp_chr7",
    "dbsnp_chr8",
    "dbsnp_chr9",
    "dbsnp_chr10",
    "dbsnp_chr11",
    "dbsnp_chr12",
    "dbsnp_chr13",
    "dbsnp_chr14",
    "dbsnp_chr15",
    "dbsnp_chr16",
    "dbsnp_chr17",
    "dbsnp_chr18",
    "dbsnp_chr19",
    "dbsnp_chr20",
    "dbsnp_chr21",
    "dbsnp_chr22",
    "dbsnp_chrx",
    "dbsnp_chry",
    "dbsnp_chrmt",
    # RelationShips
    "reactome_relationships",
    "uniprot_relationships",
]

run_steps = [
    "extract",
    "transform",
    "load"
]  # noqa E501

if __name__ == "__main__":
    bf = Biofilter(db_uri)

    for source in data_sources_to_process:
        for step in run_steps:
            try:
                print(f"▶ Running ETL - Source: {source} | Step: {step}")
                bf.update(
                    data_sources=[source],
                    run_steps=[step],
                    force_steps=[step],
                )
            except Exception as e:
                print(f"❌ Error processing {source} [{step}]: {e}")

    print("✅ All ETL tasks finished.")
    print("------------------------------")
```

---

### 📊 Where is the Data Stored?

Raw and processed files are stored in structured folders based on the source:

```
raw/<source_system>/<data_source>/
processed/<source_system>/<data_source>/
```

These folders contain:

- The original file (raw dump or download)
- The transformed, standardized files for ingestion

---

### 🌐 Supported Data Sources

To see which source systems are available, you can use the CLI:

```bash
$ biofilter report list
```

Or consult the ETL documentation section of this guide (coming soon).

---