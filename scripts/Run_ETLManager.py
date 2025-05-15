from biofilter import Biofilter

db_uri = "sqlite:///dev_biofilter.db"

bf = Biofilter(db_uri)

# bf.update(source_system=["HGNC"])
bf.update(data_sources=["hgnc_genes"], run_steps=["extract"], force_steps=["extract"])
# bf.update(data_sources=["dbSNP_chrY"], run_steps=["load"], force_steps=["load"])
# bf.update(data_sources=["dbSNP_chr22"])
# bf.update(source_system=["dbSNP"], run_steps=["extract"], force_steps=["extract"])
# bf.update(
#     data_sources=["dbSNP_chrY"], run_steps=["extract"], force_steps=["extract"]
# )
# bf.update(
#     data_sources=["Reactome_Pathways"], run_steps=["extract"]
# )
# bf.update(
#     data_sources=["dbSNP_SAMPLE"], run_steps=["load"], force_steps=["load"]
# )


print("Database updated successfully.")
