from biofilter import Biofilter

bf = Biofilter("sqlite:///biofilter_dev.db")
bf.etl.index()  # all indexs
