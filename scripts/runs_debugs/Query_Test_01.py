import pandas as pd
from biofilter import Biofilter
from biofilter.query import Query

# Define DB Link
db_uri = "sqlite:///dev_biofilter.db"

# Instance Biofilter
bf = Biofilter(db_uri)
db_session = bf.db.get_session()
q = Query(db_session)

# # Explore Models Informations
# print(q.list_models())
# print(q.describe_model("Gene"))
# print(q.describe_model("Variant"))

# Get Models
Variant = q.get_model("Variant")
VariantGeneRelationship = q.get_model("VariantGeneRelationship")
Gene = q.get_model("Gene")
Entity = q.get_model("Entity")
EntityName = q.get_model("EntityName")
EntityGroup = q.get_model("EntityGroup")
EntityRelationship = q.get_model("EntityRelationship")


# Create stmt
stmt = q.select(Gene).where(Gene.hgnc_id == "HGNC:5")
# Get Data / Return a DataFrame
df_genes = q.run_query(stmt, return_df=True)
print(df_genes)


# Query 2: From 1 Gene get Variants informations
# Step 1: Fetch entrez_id for the gene with hgnc_id = 'HGNC:5'
Gene = q.get_model("Gene")
Variant = q.get_model("Variant")
VariantGeneRelationship = q.get_model("VariantGeneRelationship")

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


# # Query 3: Get all RelationShips from a SNP
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


# # Query 3.1: Get all RelationShips from a SNP
# # Step 1: Get entrez_id for a given rsID
# Variant = q.get_model("Variant")
# VariantGeneRelationship = q.get_model("VariantGeneRelationship")
# Gene = q.get_model("Gene")
# Entity = q.get_model("Entity")
# EntityName = q.get_model("EntityName")
# EntityGroup = q.get_model("EntityGroup")
# EntityRelationship = q.get_model("EntityRelationship")

# # Input rsID
# rsid = "1451"  # <== rdID

# # Step 1: Get entrez_id from VariantGeneRelationship
# stmt1 = (
#     q.select(VariantGeneRelationship.gene_id)
#     .where(VariantGeneRelationship.variant_id == rsid)
# )
# gene_id_result = q.run_query(stmt1)

# if not gene_id_result:
#     raise ValueError("No gene associated with this rsID.")

# entrez_id = gene_id_result[0]

# # Step 2: Get entity_id from Gene
# stmt2 = q.select(Gene.entity_id).where(Gene.entrez_id == str(entrez_id))
# entity_result = q.run_query(stmt2)

# if not entity_result:
#     raise ValueError("Gene not found.")

# entity_id = entity_result[0]

# # Step 3: Find all entity relationships where the gene is involved
# stmt3 = q.select(EntityRelationship).where(
#     (EntityRelationship.entity_1_id == entity_id)
#     | (EntityRelationship.entity_2_id == entity_id)
# )
# relationships = q.run_query(stmt3)

# # Step 4: For each related entity, fetch its name and group
# related_entity_ids = set()
# for rel in relationships:
#     if rel.entity_1_id != entity_id:
#         related_entity_ids.add(rel.entity_1_id)
#     if rel.entity_2_id != entity_id:
#         related_entity_ids.add(rel.entity_2_id)

# # Step 5: Join Entity, EntityName, EntityGroup for each related entity
# stmt4 = (
#     q.select(
#         Entity.id.label("entity_id"),
#         EntityGroup.name.label("group_name"),
#         EntityName.name.label("entity_name")
#     )
#     .join(EntityName, Entity.id == EntityName.entity_id)
#     .join(EntityGroup, Entity.group_id == EntityGroup.id)
#     .where(Entity.id.in_(related_entity_ids))
#     .where(EntityName.is_primary == True)
# )
# # df = q.run_query(stmt4, return_df=True)
# import pandas as pd
# raw = q.session.execute(stmt4).fetchall()
# columns = ["entity_id", "group_name", "entity_name"]
# df = pd.DataFrame(raw, columns=columns)

# print(df)
