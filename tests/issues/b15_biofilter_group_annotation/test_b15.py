import pytest
import os
import subprocess
from pathlib import Path

"""
Documentation to this test in README.md
====================================

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
    gene_file_path = (
        base_dir
        # / "issues/b15_biofilter_group_annotation/data-in/ROSMAP_RNAseq_FPKM_gene_ensembl_list_edit.txt"  # noqa E501
        / "issues/b15_biofilter_group_annotation/data-in/TEST_SINGLE_GENE.txt"  # noqa E501
    )  # noqa E501
    output_prefix = (
        base_dir
        / "issues/b15_biofilter_group_annotation/data-out/ROSMAP_RNAseq_ENSEMBL_gene_pathways"  # noqa E501
    )  # noqa E501
    gene_file_path_run_2 = (
        base_dir
        / "issues/b15_biofilter_group_annotation/data-in/genes_out_from_run_1.txt"  # noqa E501
    )  # noqa E501
    output_prefix_run_2 = (
        base_dir
        / "issues/b15_biofilter_group_annotation/data-out/outcome_run_2"  # noqa E501
    )  # noqa E501

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
        "gene_file_2": gene_file_path_run_2,
        "output_prefix_2": output_prefix_run_2,
    }


def test_issue_b15_run_1(setup_paths):
    """
    Run filter to all GENES in the input file

    """
    # Parâmetros do comando
    command = [
        "python",
        "biofilter_modules/biofilter.py",
        "--knowledge",
        str(setup_paths["knowledge"]),
        # INPUT
        "--gene-file",
        str(setup_paths["gene_file"]),
        "--source",
        "kegg",
        "reactome",
        "go",
        # SETTINGS PARAMETERS
        "--gene-identifier-type",
        "ensembl_gid",
        "--verbose",
        # OUTPUT
        "--filter",
        "gene",
        "group",
        "source",
        "--report-configuration",
        "--prefix",
        str(setup_paths["output_prefix"]),
        # "--allow-duplicate-output",
        # "yes",
        "--overwrite",
    ]

    # Run the command
    result = subprocess.run(command, capture_output=True, text=True)

    # Check return code
    assert (
        result.returncode == 0
    ), f"Erro ao executar o comando: {result.stderr}"  # noqa E501

    # set of output files expected
    output_files = [
        str(setup_paths["output_prefix"]) + ".configuration",
        str(setup_paths["output_prefix"]) + ".gene-group-source",
        str(setup_paths["output_prefix"]) + ".log",
    ]

    # Check if the output files were created
    for output_file in output_files:
        assert Path(
            output_file
        ).exists(), f"Output file {output_file} not found."  # noqa E501

    # # Check if the number of lines in the output file is 294620
    # file_path = Path(str(setup_paths["output_prefix"]) + ".gene-group-source")  # noqa E501
    # with file_path.open("r") as file:
    #     line_count = sum(1 for line in file)
    # assert (
    #     line_count == 294620
    # ), f"Expected 16000 lines, but found {line_count}"  # noqa E501

    # # Check if the log file was created and content is as expected
    # log_file_path = Path(str(setup_paths["output_prefix"]) + ".log")

    # # Trechos esperados no log
    # expected_log_snippets = [
    #     "WARNING: ignored 22673 unrecognized gene identifier(s)",
    # ]

    # # Abre o arquivo de log e lê o conteúdo
    # with log_file_path.open("r") as log_file:
    #     log_content = log_file.read()

    # # Realiza asserts para cada trecho esperado no log
    # for snippet in expected_log_snippets:
    #     assert (
    #         snippet in log_content
    #     ), f"Expected log snippet not found: '{snippet}'"  # noqa E501


def test_issue_b15_run_2(setup_paths):
    """
    Run filter now with GENES that did not return values ​​from the first run.

    Documentation to this test in doc.md
    ====================================
    """
    # Parâmetros do comando
    command = [
        "python",
        "biofilter_modules/biofilter.py",
        "--knowledge",
        str(setup_paths["knowledge"]),
        # INPUT
        "--gene-file",
        str(setup_paths["gene_file_2"]),
        "--source",
        "kegg",
        "reactome",
        "go",
        # SETTINGS PARAMETERS
        "--gene-identifier-type",
        "ensembl_gid",
        "--verbose",
        # OUTPUT
        "--filter",
        "gene",
        "group",
        "source",
        "--report-configuration",
        "--prefix",
        str(setup_paths["output_prefix_2"]),
        "--overwrite",
    ]

    # Run the command
    result = subprocess.run(command, capture_output=True, text=True)

    # Check return code
    assert (
        result.returncode == 0
    ), f"Erro ao executar o comando: {result.stderr}"  # noqa E501

    # set of output files expected
    output_files = [
        str(setup_paths["output_prefix_2"]) + ".configuration",
        str(setup_paths["output_prefix_2"]) + ".gene-group-source",
        str(setup_paths["output_prefix_2"]) + ".log",
    ]

    # Check if the output files were created
    for output_file in output_files:
        assert Path(
            output_file
        ).exists(), f"Output file {output_file} not found."  # noqa E501
