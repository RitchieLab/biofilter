from biofilter import Biofilter

bf = Biofilter("postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_dev")
bf.etl.update(data_sources="dbsnp_sample")
