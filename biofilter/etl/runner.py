# from etl.sources.dbsnp import DBSNPEtl
from biofilter.etl.sources.hgnc import run_hgnc_etl


def run_all_etl(db):
    jobs = [run_hgnc_etl]

    for job in jobs:
        job()
