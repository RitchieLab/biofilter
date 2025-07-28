from biofilter import Biofilter

db_uri = "sqlite:///dev_biofilter.db"

if __name__ == "__main__":

    bf = Biofilter(db_uri)

    bf.migrate()
