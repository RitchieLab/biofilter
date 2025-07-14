from biofilter import Biofilter

db_uri = "sqlite:///dev_biofilter.db"

if __name__ == "__main__":

    bf = Biofilter(db_uri)

    # ETL HGNC -- GENES
    # bf.update(data_sources=["hgnc"], run_steps=["extract"], force_steps=["extract"])      # noqa: E501
    # bf.update(data_sources=["hgnc"], run_steps=["transform"], force_steps=["transform"])  # noqa: E501
    bf.update(data_sources=["pfam"], run_steps=["extract"], force_steps=["extract"])
    # bf.update(data_sources=["pfam"], run_steps=["transform"], force_steps=["transform"])
    # bf.update(data_sources=["pfam"], run_steps=["load"], force_steps=["load"])
    # bf.update(data_sources=["uniprot"], run_steps=["extract"], force_steps=["extract"])
    # bf.update(
    #     data_sources=["uniprot"], run_steps=["transform"], force_steps=["transform"]
    # )
    # bf.update(
    #     data_sources=["uniprot_relationships"], run_steps=["load"], force_steps=["load"]
    # )  # noqa: E501
    # bf.update(data_sources=["hgnc"], run_steps=["load"], force_steps=["load"])

    # bf.update(data_sources=["gene_ontology"], run_steps=["load"], force_steps=["load"])
    # bf.update(
    #     data_sources=["gene_ontology"], run_steps=["extract"], force_steps=["extract"]
    # )
    # bf.update(
    #     data_sources=["gene_ontology"],
    #     run_steps=["transform"],
    #     force_steps=["transform"],
    # )
    # bf.update(data_sources=["gene_ontology"], run_steps=["load"], force_steps=["load"])

    # ETL dbSNP - VARIANTS
    # bf.update(
    #     data_sources=["dbsnp_sample"], run_steps=["extract"], force_steps=["extract"]
    # )  # noqa: E501
    # bf.update(
    #     data_sources=["dbsnp_chry"],
    #     run_steps=["transform"],
    #     force_steps=["transform"],
    # )  # noqa: E501
    # bf.update(
    #     data_sources=["dbsnp_chry"], run_steps=["load"], force_steps=["load"]
    # )  # noqa: E501

    # bf.update(data_sources=["dbsnp_chr8"], run_steps=["extract"], force_steps=["extract"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chr8"], run_steps=["transform"], force_steps=["transform"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chr18"], run_steps=["load"], force_steps=["load"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chr19"], run_steps=["load"], force_steps=["load"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chr20"], run_steps=["load"], force_steps=["load"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chr21"], run_steps=["load"], force_steps=["load"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chr3"], run_steps=["extract"], force_steps=["extract"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chr3"], run_steps=["transform"], force_steps=["transform"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chr4"], run_steps=["extract"], force_steps=["extract"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chr4"], run_steps=["transform"], force_steps=["transform"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chr4"], run_steps=["load"], force_steps=["load"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chry"], run_steps=["load"], force_steps=["load"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chrmt"], run_steps=["extract"], force_steps=["extract"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chrmt"], run_steps=["transform"], force_steps=["transform"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chrmt"], run_steps=["load"], force_steps=["load"])  # noqa: E501

    # bf.update(data_sources=["dbsnp_chrx"], run_steps=["extract"], force_steps=["extract"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chrx"], run_steps=["transform"], force_steps=["transform"])  # noqa: E501
    # bf.update(data_sources=["dbsnp_chrx"], run_steps=["load"], force_steps=["load"])  # noqa: E501

    # ETL REACTOME - PATHWAYS
    # bf.update(data_sources=["reactome"], run_steps=["extract"], force_steps=["extract"]) # noqa: E501
    # bf.update(data_sources=["reactome"], run_steps=["transform"], force_steps=["transform"]) # noqa: E501
    # bf.update(data_sources=["reactome"], run_steps=["load"], force_steps=["load"]) # noqa: E501

    # bf.update(data_sources=["reactome_relationships"], run_steps=["extract"], force_steps=["extract"]) # noqa: E501
    # bf.update(data_sources=["reactome_relationships"], run_steps=["transform"], force_steps=["transform"]) # noqa: E501
    # bf.update(data_sources=["reactome_relationships"], run_steps=["load"], force_steps=["load"]) # noqa: E501

    # bf.update(data_sources=["gene_ncbi"], run_steps=["extract"], force_steps=["extract"])  # noqa: E501
    # bf.update(data_sources=["gene_ncbi"], run_steps=["transform"], force_steps=["transform"]) # noqa: E501
    # bf.update(
    #     data_sources=["gene_ncbi"], run_steps=["load"], force_steps=["load"]
    # )  # noqa: E501


print("Database updated successfully.")
print("------------------------------")
