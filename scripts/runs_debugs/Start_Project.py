from biofilter import Biofilter

bf = Biofilter()
bf.create_new_project("postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter", overwrite=True)
# bf.create_new_project("sqlite:///dev_biofilter.db", overwrite=True)
