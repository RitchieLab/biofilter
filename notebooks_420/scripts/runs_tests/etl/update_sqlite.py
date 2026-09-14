from biofilter import Biofilter

bf = Biofilter("sqlite:///biofilter_dev.db")
bf.etl.update(data_sources="dbsnp_sample")
