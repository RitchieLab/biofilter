from biofilter import Biofilter

# if __name__ == "__main__":
db_uri = "sqlite:///dev_biofilter.db"

bf = Biofilter(db_uri)
# bf.connect_db(db_uri)

# bf.restart_etl(
#     data_source=["hgnc_genes"]
# )

bf.update(source_system=["HGNC"])
print("Database updated successfully.")
