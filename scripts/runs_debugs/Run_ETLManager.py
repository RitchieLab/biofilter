import time

from biofilter import Biofilter

# db_uri = "sqlite:///dev_biofilter_2.db"
db_uri = "postgresql+psycopg2://admin:admin@localhost/biofilter_dev"

# Configure below
data_sources_to_process = [
    # Genes
    # -----
    # "hgnc",
    # "gene_ncbi",
    # "ensembl",
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

    # Gene Ontology
    # -------------
    # "gene_ontology",

    # Variants
    # --------
    # "dbsnp_sample",
    # "dbsnp_chr1",
    # "dbsnp_chr2",
    # "dbsnp_chr3",
    # "dbsnp_chr4",
    # "dbsnp_chr5",
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
    # "dbsnp_chr20",
    # "dbsnp_chr21",
    # "dbsnp_chr22",
    # "dbsnp_chrx",
    # "dbsnp_chry",
    # "dbsnp_chrmt",
    # "gwas",
    # "gnomad_chr1",
    # "gnomad_chr2",
    # "gnomad_chr3",
    # "gnomad_chr4",
    # "gnomad_chr5",
    # "gnomad_chr6",
    # "gnomad_chr7",
    # "gnomad_chr8",
    # "gnomad_chr9",
    # "gnomad_chr10",
    # "gnomad_chr11",
    # "gnomad_chr12",
    # "gnomad_chr13",
    # "gnomad_chr14",
    # "gnomad_chr15",
    # "gnomad_chr16",
    # "gnomad_chr17",
    # "gnomad_chr18",
    # "gnomad_chr19",
    # "gnomad_chr20",
    # "gnomad_chr21",
    # "gnomad_chr22",
    # "gnomad_chrx",
    # "gnomad_chry",
    # "gnomad_chrmt",
    "alphamissense",

    #
    # DISEASE
    # -------
    # "mondo",
    # "clingen",
    # "omim",

    # CHEMICAL
    # --------
    # "chebi",

    # RelationShips
    # -------------
    # "reactome_relationships",
    # "uniprot_relationships",
    # "mondo_relationships",
    # "biogrid",
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

    start_total = time.time()

    print()

    for source in data_sources_to_process:
        for step in run_steps:

            start_process = time.time()

            if step != "all":
                try:
                    print(f"▶ Running ETL - Source: {source} | Step: {step}")
                    bf.etl.update(
                        data_sources=[source],
                        run_steps=[step],
                        force_steps=[step],
                    )
                except Exception as e:
                    print(f"❌ Error processing {source} [{step}]: {e}")
            elif step == "all":
                try:
                    print(f"▶ Running ETL - Source: {source} | Step: {step}")
                    bf.etl.update(
                        data_sources=[source],
                        # run_steps=[step],
                        # force_steps=[step],
                    )
                except Exception as e:
                    print(f"❌ Error processing {source} [{step}]: {e}")

            end_process = time.time() - start_process
            msg = str(
                f"processed Time Total: {end_process:.2f}s"  # noqa E501
            )  # noqa E501
            print(msg)

    end_time = time.time() - start_total
    msg = str(f"job Time Total: {end_time:.2f}s")  # noqa E501  # noqa E501
    print(msg)

    print("✅ All ETL tasks finished.")
    print("------------------------------")
