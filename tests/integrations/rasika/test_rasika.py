import pytest
import os
import subprocess
from pathlib import Path


"""
biofilter.py 
--knowledge ~/group/datasets/loki/loki-20220926.db
--gene-file ROSMAP_RNAseq_FPKM_gene_ensembl_list_edit.txt
--gene-identifier-type ensembl_gid
--filter gene group source
--source kegg reactome go
--verbose
--report-configuration
--prefix ROSMAP_RNAseq_ENSEMBL_gene_pathways
--overwrite

biofilter.py
--knowledge ~/group/datasets/loki/loki-20220926.dd
--gene-file ROSMAP_RNAseq_removedbiofilt.txt
--gene-identifier-type ensembl_gid
--filter gene group source
--source kegg reactome go
--verbose
--report-configuration
--prefix ROSMAP_RNAseq_removedbiofilt
--overwrite
"""


@pytest.fixture
def setup_paths():
    base_dir = Path(__file__).parent.parent.parent
    knowledge_path = base_dir / "data/loki-20220926.db"
    gene_file_path = base_dir / "integrations/rasika/data-in/ROSMAP_RNAseq_FPKM_gene_ensembl_list_edit.txt"  # noqa E501
    output_prefix = base_dir / "integrations/rasika/data-out/ROSMAP_RNAseq_ENSEMBL_gene_pathways" # noqa E501

    # Create output directory if it does not exist
    if output_prefix.parent.exists():
        for file in output_prefix.parent.glob("*"):
            file.unlink()  # drop all files in the output directory
    else:
        os.makedirs(output_prefix.parent)  # create the output directory

    return {
        "knowledge": knowledge_path,
        "gene_file": gene_file_path,
        "output_prefix": output_prefix,
    }


def test_integration_rasika_first_run(setup_paths):
    """
    Integrates test for the biofilter.py command with specific parameters.

    Documentation to this test in doc.md
    ====================================
    """
    # Parâmetros do comando
    command = [
        "python", "biofilter_modules/biofilter.py",
        "--knowledge", str(setup_paths["knowledge"]),
        "--gene-file", str(setup_paths["gene_file"]),
        "--gene-identifier-type", "ensembl_gid",
        "--filter", "gene", "group", "source",
        "--source", "kegg", "reactome", "go",
        "--verbose",
        "--report-configuration",
        "--prefix", str(setup_paths["output_prefix"]),
        "--overwrite"
    ]

    # Run the command
    result = subprocess.run(command, capture_output=True, text=True)

    # Check return code
    assert result.returncode == 0,f"Erro ao executar o comando: {result.stderr}"  # noqa E501

    # set of output files expected
    output_files = [
        str(setup_paths["output_prefix"]) + ".configuration",
        str(setup_paths["output_prefix"]) + ".gene-group-source",
        str(setup_paths["output_prefix"]) + ".log",
    ]

    # Check if the output files were created
    for output_file in output_files:
        assert Path(output_file).exists(), f"Output file {output_file} not found." # noqa E501

    # # Check if the output files have content
    # with open(output_files[0], "r") as f:
    #     content = f.read()
    #     assert "Gene" in content, "-------"
