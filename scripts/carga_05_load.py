from biofilter import Biofilter

# db_uri = "sqlite:///biofilter.db"
db_uri = "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter"

# Configure below
data_sources_to_process = [
    # Genes
    # -----
    # "hgnc",
    # "gene_ncbi",
    # "ensembl",
    # "gene_ncbi",
    #
    # Proteins
    # --------
    # "pfam",
    # "uniprot",
    #
    # Pathways
    # --------
    # "reactome",
    # "kegg_pathways",
    #
    # Gene Ontology
    # -------------
    # "gene_ontology",
    #
    # Variants
    # --------
    # "dbsnp_sample",
    # "dbsnp_chr1",
    # "dbsnp_chr2",
    # "dbsnp_chr3",
    # "dbsnp_chr4",
    "dbsnp_chr5",
    # "dbsnp_chr6",
    # "dbsnp_chr7",
    # "dbsnp_chr8",
    # "dbsnp_chr9",
    # "dbsnp_chr10",
    # "dbsnp_chr11",
    # "dbsnp_chr12",
    # "dbsnp_chr13",
    # "dbsnp_chr14",
    # "dbsnp_chr15",
    # "dbsnp_chr16",
    # "dbsnp_chr17",
    # "dbsnp_chr18",
    # "dbsnp_chr19",
    # "dbsnp_chr21",
    # "dbsnp_chr21",
    # "dbsnp_chr22",
    # "dbsnp_chrx",
    # "dbsnp_chry",
    # "dbsnp_chrmt",
    # "gwas",
    #
    # RelationShips
    # -------------
    # "reactome_relationships",
    # "uniprot_relationships",
    #
    # DISEASE
    # -------
    # "mondo",
    # "mondo_relationships",
    #
    # CHEMICAL
    # --------
    # "chebi",
]

run_steps = [
    # "extract",
    # "transform",
    "load",
    # "all"
]  # noqa E501

if __name__ == "__main__":
    bf = Biofilter(db_uri, debug_mode=True)
    # bf = Biofilter(db_uri)

    for source in data_sources_to_process:
        for step in run_steps:
            if step != "all":
                try:
                    print(f"▶ Running ETL - Source: {source} | Step: {step}")
                    bf.update(
                        data_sources=[source],
                        run_steps=[step],
                        force_steps=[step],
                    )
                except Exception as e:
                    print(f"❌ Error processing {source} [{step}]: {e}")
            elif step == "all":
                try:
                    print(f"▶ Running ETL - Source: {source} | Step: {step}")
                    bf.update(
                        data_sources=[source],
                        # run_steps=[step],
                        # force_steps=[step],
                    )
                except Exception as e:
                    print(f"❌ Error processing {source} [{step}]: {e}")

    print("✅ All ETL tasks finished.")
    print("------------------------------")
