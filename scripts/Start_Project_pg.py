from biofilter import Biofilter

bf = Biofilter()
bf.create_new_project(
    "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter", overwrite=True
)
