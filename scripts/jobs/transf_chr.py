from biofilter import Biofilter

db_uri = "sqlite:///dev_biofilter_2.db"

if __name__ == "__main__":
    bf = Biofilter(db_uri)

    # ETL dbSNP -- Transforms
    # bf.update(data_sources=["dbsnp_chr2"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr3"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr4"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr5"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr6"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr7"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr8"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr9"], run_steps=["transform"], force_steps=["transform"])
    bf.update(data_sources=["dbsnp_chr10"], run_steps=["transform"], force_steps=["transform"])
    bf.update(data_sources=["dbsnp_chr11"], run_steps=["transform"], force_steps=["transform"])
    bf.update(data_sources=["dbsnp_chr12"], run_steps=["transform"], force_steps=["transform"])
    bf.update(data_sources=["dbsnp_chr13"], run_steps=["transform"], force_steps=["transform"])
    bf.update(data_sources=["dbsnp_chr14"], run_steps=["transform"], force_steps=["transform"])
    bf.update(data_sources=["dbsnp_chr15"], run_steps=["transform"], force_steps=["transform"])
    bf.update(data_sources=["dbsnp_chr16"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr9"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["dbsnp_chr9"], run_steps=["load"], force_steps=["load"])
