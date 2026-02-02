from biofilter import Biofilter

# db_uri = "sqlite:///dev_biofilter.db"
db_uri = "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_prod"
bf = Biofilter(db_uri)

df = bf.report.run(
    "entity_neighborhood_summary",
    items=["gene:BRCA1", "APOE"],
    resolver_mode="exact",
)

df = bf.report.run(
    "entity_neighborhood_summary",
    items=["chemical:((R)-3-Hydroxybutanoyl)(n-2)"],
    resolver_mode="search",
    include_candidates=True,
    candidates_top_n=10,
)

df = bf.report.run(
    "entity_neighborhood_summary",
    items=["chemical:((R)-3-Hydroxybutanoyl)", "gene:BRCA1", "alzheimers"],
    resolver_mode="hybrid",
    include_candidates=True,
)

print(df)
