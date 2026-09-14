from biofilter import Biofilter

# db_uri = "sqlite:///dev_biofilter.db"
db_uri = "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_prod"
bf = Biofilter(db_uri)

# Run Report Explain
# print(bf.report.explain("etl_status"))
print(bf.report.explain("entity_neighborhood_summary"))
