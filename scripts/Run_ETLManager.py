from biofilter import Biofilter

# if __name__ == "__main__":
db_uri = "sqlite:///dev_biofilter.db"

bf = Biofilter(db_uri)
# bf.connect_db(db_uri)  # ou connect_db se já existir

# bf.restart_etl(
#     data_source=["hgnc_genes"]
# )  # Altere aqui para o sistema que quiser testar

bf.update(source_system=["HGNC"])  # Altere aqui para o sistema que quiser testar
print("Database updated successfully.")
