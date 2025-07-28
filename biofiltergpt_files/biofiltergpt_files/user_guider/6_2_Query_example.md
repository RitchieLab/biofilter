# Example: From Gene get Variants informations

```python
from biofilter import Biofilter
from biofilter.query import Query

# Define DB Link
db_uri = "sqlite:///dev_biofilter.db"

# Instance Biofilter
bf = Biofilter(db_uri)
db_session = bf.db.get_session()
q = Query(db_session)

Gene = q.get_model("Gene")
Variant = q.get_model("Variant")
VariantGeneRelationship = q.get_model("VariantGeneRelationship")

# Step 1: Fetch entrez_id for the gene with hgnc_id = 'HGNC:5'
stmt1 = q.select(Gene.entrez_id).where(Gene.hgnc_id == "HGNC:5")
entrez_result = q.run_query(stmt1)

if not entrez_result:
    raise ValueError("Gene not found.")

entrez_id = entrez_result[0]  # entrez_id is a string

# Step 2: Find all variant_ids linked to this entrez_id
stmt2 = q.select(VariantGeneRelationship.variant_id).where(
    VariantGeneRelationship.gene_id == int(entrez_id)  # gene_id is stored as integer
)
variant_ids = [r for r in q.run_query(stmt2)]

# Step 3: Fetch full Variant records for those variant_ids
stmt3 = q.select(Variant).where(Variant.variant_id.in_(variant_ids))
variants_df = q.run_query(stmt3, return_df=True)

print(variants_df)
```

