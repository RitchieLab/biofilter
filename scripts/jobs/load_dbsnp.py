from biofilter import Biofilter

db_uri = "sqlite:///dev_biofilter_2.db"

if __name__ == "__main__":
    bf = Biofilter(db_uri)

    # ETL HGNC -- GENES
    # bf.update(data_sources=["dbsnp_chr1"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr1"], run_steps=["load"], force_steps=["load"])

    bf.update(
        data_sources=["dbsnp_chr2"], run_steps=["extract"], force_steps=["extract"]
    )
    # bf.update(data_sources=["dbsnp_chr2"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr2"], run_steps=["load"], force_steps=["load"])

    # bf.update(data_sources=["dbsnp_chr6"], run_steps=["extract"], force_steps=["extract"])
    # bf.update(data_sources=["dbsnp_chr6"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr6"], run_steps=["load"], force_steps=["load"])

    # bf.update(data_sources=["dbsnp_chr7"], run_steps=["extract"], force_steps=["extract"])
    # bf.update(data_sources=["dbsnp_chr7"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr7"], run_steps=["load"], force_steps=["load"])

    # bf.update(data_sources=["dbsnp_chr8"], run_steps=["extract"], force_steps=["extract"])
    # bf.update(data_sources=["dbsnp_chr8"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr8"], run_steps=["load"], force_steps=["load"])

    # bf.update(data_sources=["dbsnp_chr9"], run_steps=["extract"], force_steps=["extract"])
    # bf.update(data_sources=["dbsnp_chr9"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr9"], run_steps=["load"], force_steps=["load"])
