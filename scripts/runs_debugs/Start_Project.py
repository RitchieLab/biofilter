from biofilter import Biofilter

# uri = "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_test"

bf = Biofilter("postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_dev")
# bf.create_new_project(uri, overwrite=True)
# bf.create_new_project("postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter")
# bf.create_new_project("postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_dev")
# bf.create_new_project("sqlite:///dev_biofilter_2.db", overwrite=True)

# bf.db.create("sqlite:///dev_biofilter_2.db", overwrite=True)
bf.db.create_db(
    "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_prod", overwrite=True
)
