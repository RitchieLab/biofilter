from biofilter import Biofilter

# db_uri = "sqlite:///dev_biofilter.db"
db_uri = "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_prod"
bf = Biofilter(db_uri)

# Run Report Explain
bf.report.refresh()
