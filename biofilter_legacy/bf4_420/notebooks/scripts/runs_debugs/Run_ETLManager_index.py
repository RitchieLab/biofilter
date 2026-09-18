import time
from biofilter import Biofilter

# db_uri = "sqlite:///dev_biofilter.db"
db_uri = "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter"

bf = Biofilter(db_uri)

# bf.rebuild_indexes()
bf.rebuild_indexes(groups="chemical")
# bf.rebuild_indexes(groups=["gene", "proteins"])
# bf.rebuild_indexes(groups="gene", drop_only=True)
# bf.rebuild_indexes(drop_only=True)

print("Database updated successfully.")
