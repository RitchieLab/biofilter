from biofilter import Biofilter

db_uri = "sqlite:///dev_biofilter_2.db"

if __name__ == "__main__":
    bf = Biofilter(db_uri)

    # ETL HGNC -- GENES
    bf.update(
        data_sources=["dbsnp_chry"], run_steps=["transform"], force_steps=["transform"]
    )
    bf.update(data_sources=["dbsnp_chry"], run_steps=["load"], force_steps=["load"])
    bf.update(
        data_sources=["dbsnp_chrx"], run_steps=["transform"], force_steps=["transform"]
    )
    bf.update(data_sources=["dbsnp_chrx"], run_steps=["load"], force_steps=["load"])
