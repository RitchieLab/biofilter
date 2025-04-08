import pytest
import os
import subprocess
from pathlib import Path


@pytest.fixture
def setup_paths():
    base_dir = Path(__file__).parent.parent.parent

    # knowledge_path = base_dir / "data/loki-20220926.db"
    knowledge_path = "/Users/andrerico/Works/Sys/biofilter/data/loki-20220926.db"

    input_file_path = (
        base_dir / "issues/l16_build_37_loki/data-in/input_filename"  # noqa E501
    )  # noqa E501

    output_prefix = (
        base_dir / "issues/l16_build_37_loki/data-out/outcomes"  # noqa E501
    )  # noqa E501

    # Create output directory if it does not exist
    if output_prefix.parent.exists():
        for file in output_prefix.parent.glob("*"):
            file.unlink()  # drop all files in the output directory
    else:
        os.makedirs(output_prefix.parent)  # create the output directory

    return {
        "knowledge": knowledge_path,
        "input_file": input_file_path,
        "output_prefix": output_prefix,
    }


def test_issue_l16_build_37_annotate_run(setup_paths):
    """
    Issue test for the biofilter.py command with specific parameters.

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
        "--position-file",
        str(setup_paths["input_file"]),
        "--source",
        "kegg",
        "reactome",
        "go",
        # SETTINGS PARAMETERS
        "--ucsc-build-version",
        "19",
        "--verbose",
        # OUTPUT
        "--annotate",
        "position_label",
        "snp",
        "position",
        "gene",
        "upstream",
        "downstream",
        "--report-configuration",
        "--report-invalid-input",
        "--prefix",
        str(setup_paths["output_prefix"]),
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
        str(setup_paths["output_prefix"])
        + ".position_label.snp-position-gene-upstream-downstream",  # noqa E501
        str(setup_paths["output_prefix"]) + ".log",
        # str(setup_paths["output_prefix"]) + ".invalid.position",
    ]

    # Check if the output files were created
    for output_file in output_files:
        assert Path(
            output_file
        ).exists(), f"Output file {output_file} not found."  # noqa E501
