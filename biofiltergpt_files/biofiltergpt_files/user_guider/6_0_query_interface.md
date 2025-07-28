# Query Interface

> 🧠 Audience: Developers, Data Analysts, Bioinformaticians
> 
> 
> 📌 Focus: Programmatically access, explore, and query Biofilter3R using Python.
> 

---

## 🔍 What Is the Query Interface?

The **Query Interface** is a programmatic class designed to help users explore and interact with the Biofilter3R database using Python and SQLAlchemy.

It provides:

- Easy access to all models
- Helper functions for querying and inspection
- Integration with pandas for DataFrame outputs
- Dynamic filtering by keyword arguments

This interface is ideal for **Jupyter notebooks**, **data exploration**, or **API integration**.

---

## ⚙️ How It Works

To use the interface:

```python
from biofilter import Biofilter
from biofilter.query import Query

# Instance Biofilter
bf = Biofilter("sqlite:///dev_biofilter.db")

# Instance Query with Biofilter DB Session
q = Query(bf.db.get_session())

# Get Gene Model
Gene = q.get_model("Gene")

# Run Query to access a Gene and Return DataFrame
my_gene_df = q.run_query(
	q.select(Gene).where(Gene.hgnc_id == "HGNC:5"),
	return_df=True
)
```

You now have access to:

- All models in the database
- SQLAlchemy core functions (select, and_, or_, func…)
- High-level querying utilities

---

## 📦 Available Models

To list all available models:

```python
for model in q.list_models():
    print(model)
```

You can access a model by name using:

```python
q.get_model("Gene")
```

Or directly via (I need add **getattr** in the class):

```python
q.Gene
```

---

## 🧪 Running Queries

### 1. Raw Query with SQLAlchemy

```python
stmt = q.select(q.Gene).where(q.Gene.hgnc_id == "HGNC:5")
results = q.run_query(stmt)
```

### 2. Query With Filters

```python
genes = q.query_model("Gene", hgnc_id="HGNC:5")
variant = q.query_model("Gene", entrez_id="11")
```

Retrieve variants from chromosome 1

 *Heads-up: Chromosome 1 is a big one. Hope your machine brought snacks (a.k.a. RAM)!*

```python
q.query_model("Variant", chromosome="1")
```

💡 Combine with return_df=True in run_query() if you want to export the results as a Pandas DataFrame.

### 3. Return as DataFrame

```python
stmt = q.select(q.Gene).limit(10)
df = q.run_query(stmt, return_df=True)
```

---

## 🧾 Raw SQL

```python
results = q.raw_sql("SELECT * FROM genes WHERE hgnc_id = 'HGNC:5'")
# You can wrap this into a DataFrame manually if needed
```

🔎 **Note:**

Table names follow **SQL naming conventions** (e.g., `gene_group_membership`), while model classes follow **Python class naming conventions** (e.g., `GeneGroupMembership`).

When querying models using this interface, always use the **model class name**, not the table name.

---

## 🧬 Model Inspection

### Describe a Model

```python
q.describe_model("Gene")
```

Returns:

```json
{
  "columns": ["id", "hgnc_id", "entrez_id", ...],
  "relationships": ["locus_group", "locations", ...]
}
```

> 💡 Useful for understanding which fields are available for filtering and what relationships can be used for joins or navigation.
> 

### Detailed Metadata

Returns a detailed structure for all columns in a model, including:

- Column type
- Nullability
- Whether it is a primary key

This is ideal for **dynamic documentation**, **form generation**, or **advanced schema introspection**.

✅ Example:

```python
q.get_model_metadata("Variant")
```

> 🛠️ Excellent for tools that need to generate views or validate model structure programmatically.
> 

## ✅ Summary

The `Query` class provides a high-level, powerful interface to interact with Biofilter3R using Python. It supports:

- Model lookup and filtering
- Pandas export
- Model metadata inspection
- Raw SQL fallback

You can integrate this in notebooks, scripts, or APIs to access and explore the Biofilter3R knowledge base.

> 👉 To see example queries per domain, continue to:
> 
> - Query > Genes
> - Query > Variants
> - Query > Pathways
> - Query > Proteins

---

[Example: Access one Gene](https://www.notion.so/Example-Access-one-Gene-23ae7f9c0f2380a7820ff632d8b920bd?pvs=21)

[Example: From Gene get Variants informations](https://www.notion.so/Example-From-Gene-get-Variants-informations-23ae7f9c0f238019840ad2ad650e4fa8?pvs=21)

[Example: Get all RelationShips from a SNP](https://www.notion.so/Example-Get-all-RelationShips-from-a-SNP-23ae7f9c0f2380c3aa06cb942624524c?pvs=21)