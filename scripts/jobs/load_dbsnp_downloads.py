from biofilter import Biofilter

db_uri = "sqlite:///dev_biofilter_2.db"

if __name__ == "__main__":
    bf = Biofilter(db_uri)

    # ETL HGNC -- GENES
    bf.update(data_sources=["dbsnp_chry"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chrx"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr22"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr21"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr20"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr19"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr18"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr17"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr16"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr15"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr14"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr13"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr12"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr11"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr10"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr9"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr8"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr7"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr6"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr5"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr4"], run_steps=["extract"], force_steps=["extract"])
    bf.update(data_sources=["dbsnp_chr3"], run_steps=["extract"], force_steps=["extract"])
