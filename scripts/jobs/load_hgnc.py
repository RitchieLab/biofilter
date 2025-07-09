from biofilter import Biofilter

db_uri = "sqlite:///dev_biofilter.db"

if __name__ == "__main__":
    bf = Biofilter(db_uri)

    # ETL HGNC -- GENES
    bf.update(data_sources=["hgnc"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["hgnc"], run_steps=["transform"], force_steps=["transform"])
    bf.update(data_sources=["hgnc"], run_steps=["load"], force_steps=["load"])
