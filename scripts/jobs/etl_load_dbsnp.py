import os
from biofilter import Biofilter


# Caminho no Python
db_path = os.path.join(os.environ["TMPDIR"], "biofilter_snp.db")
db_uri = f"sqlite:///{db_path}"

if __name__ == "__main__":
    bf = Biofilter(db_uri)

    # Variants / SNPs
    bf.update(data_sources=["dbsnp_chr1"], run_steps=["transform"], force_steps=["transform"])  # noqa E501