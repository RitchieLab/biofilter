from biofilter import Biofilter

# if __name__ == "__main__":
db_uri = "sqlite:///biofilter_301.db"

bf = Biofilter(db_uri)
# bf.connect_db(db_uri)  # ou connect_db se já existir

bf.restart_etl(
    data_source=["dbsnp_sample"]
)  # Altere aqui para o sistema que quiser testar
print("Database updated successfully.")
