from biofilter import Biofilter

# if __name__ == "__main__":
db_uri = "sqlite:///dev_biofilter.db"

bf = Biofilter(db_uri)

# bf.export_conflicts_to_excel("conflitos.xlsx")

bf.import_conflicts_from_excel("conflitos.xlsx")

print("Conflitos exportados com sucesso.")
