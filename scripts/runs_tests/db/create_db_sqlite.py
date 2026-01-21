from biofilter import Biofilter

bf = Biofilter()

bf.db.create_db("sqlite:///biofilter_dev.db", overwrite=True)
