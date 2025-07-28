Example: Access one Gene

```python
from biofilter import Biofilter
from biofilter.query import Query

# Define DB Link
db_uri = "sqlite:///dev_biofilter.db"

# Instance Biofilter
bf = Biofilter(db_uri)
db_session = bf.db.get_session()
q = Query(db_session)

# Get Models
Gene = q.get_model("Gene")

# Create stmt
stmt = q.select(Gene).where(Gene.hgnc_id == "HGNC:5")
# Get Data / Return a DataFrame
df_genes = q.run_query(stmt, return_df=True)
print(df_genes)
```