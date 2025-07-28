# Example: Get all RelationShips from a SNP

```python
import pandas as pd
from biofilter import Biofilter
from biofilter.query import Query

# Define DB Link
db_uri = "sqlite:///dev_biofilter.db"

# Instance Biofilter
bf = Biofilter(db_uri)
db_session = bf.db.get_session()
q = Query(db_session)

# Get Models
Variant = q.get_model("Variant")
VariantGeneRelationship = q.get_model("VariantGeneRelationship")
Gene = q.get_model("Gene")
Entity = q.get_model("Entity")
EntityName = q.get_model("EntityName")
EntityGroup = q.get_model("EntityGroup")
EntityRelationship = q.get_model("EntityRelationship")

gene_id_result = q.run_query(
    q.select(VariantGeneRelationship.gene_id).where(
        VariantGeneRelationship.variant_id == "1451"
    )
)

# Step 2: Get entity_id from Gene
entity_result = q.run_query(
    q.select(Gene.entity_id).where(Gene.entrez_id == str(gene_id_result[0]))
)

# Step 3: Find all entity relationships where the gene is involved
relationships = q.run_query(
    q.select(EntityRelationship).where(
        (EntityRelationship.entity_1_id == entity_result[0])
        | (EntityRelationship.entity_2_id == entity_result[0])
    )
)

# Step 4: For each related entity, fetch its name and group
related_entity_ids = set()
for rel in relationships:
    if rel.entity_1_id != entity_result[0]:
        related_entity_ids.add(rel.entity_1_id)
    if rel.entity_2_id != entity_result[0]:
        related_entity_ids.add(rel.entity_2_id)

# Step 5: Join Entity, EntityName, EntityGroup for each related entity
stmt4 = (
    q.select(
        Entity.id.label("entity_id"),
        EntityGroup.name.label("group_name"),
        EntityName.name.label("entity_name"),
    )
    .join(EntityName, Entity.id == EntityName.entity_id)
    .join(EntityGroup, Entity.group_id == EntityGroup.id)
    .where(Entity.id.in_(related_entity_ids))
    .where(EntityName.is_primary == True)
)
raw = q.session.execute(stmt4).fetchall()

columns = ["entity_id", "group_name", "entity_name"]
df = pd.DataFrame(raw, columns=columns)

print(df)
```

..