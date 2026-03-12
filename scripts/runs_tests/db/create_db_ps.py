from biofilter import Biofilter

bf = Biofilter()

# bf.db.create("sqlite:///biofilter_dev.db", overwrite=True)
bf.db.create_db(
    "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_dev", overwrite=True
)
