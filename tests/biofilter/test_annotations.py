import subprocess
import sys
from pathlib import Path


def test_biofilter_annotation_snp():
    """
    Runs the legacy Biofilter (v3.0) with the same arguments used in the Debug mode.  # noqa E501
    Validates the full Annotation workflow: Position -> Gene, using multiple sources.  # noqa E501
    """

    # Project paths
    workspace = Path(__file__).resolve().parents[2]  # Project root
    biofilter_py = workspace / "biofilter_modules" / "biofilter.py"
    loki_db = workspace / "loki.db"
    input_positions = workspace / "tests" / "biofilter" / "data" / "input" / "input_snps.txt"  # noqa E501
    outcomes_prefix = workspace / "tests" / "biofilter" / "data" / "outcome" / "annotation" / "test_biofilter_annotation_snp__"  # noqa E501

    # Validate required files
    assert input_positions.exists(), f"Input file not found: {input_positions}"
    assert loki_db.exists(), f"LOKI DB not found: {loki_db}"

    # CLI command (mirrors VSCode debug configuration)
    cmd = [
        sys.executable, str(biofilter_py),
        "--verbose",
        "--knowledge", str(loki_db),
        # "--position-file", str(input_positions),
        "--snp-file", str(input_positions),
        "--source", "kegg", "reactome", "go",
        "--annotate", "position_label", "snp", "position", "gene", "upstream", "downstream",  # noqa E501
        "--report-invalid-input",
        "--report-configuration",
        "--overwrite",
        "--prefix", str(outcomes_prefix),
        "--ucsc-build-version", "19",
    ]

    # Run Biofilter
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Print output for debugging
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    # Ensure successful execution
    assert result.returncode == 0, f"Biofilter failed: {result.stderr}"

    # Validate output files
    expected_files = [
        outcomes_prefix.with_suffix(".configuration"),
        # outcomes_prefix.with_suffix(".invalid.position"),
    ]
    for file in expected_files:
        assert file.exists(), f"Expected output file not found: {file}"
        assert file.stat().st_size > 0, f"Output file is empty: {file}"


def test_biofilter_annotation_cli_position():
    """
    Runs the legacy Biofilter (v3.0) with the same arguments used in the Debug mode.  # noqa E501
    Validates the full Annotation workflow: Position -> Gene, using multiple sources.  # noqa E501
    """

    # Project paths
    workspace = Path(__file__).resolve().parents[2]  # Project root
    biofilter_py = workspace / "biofilter_modules" / "biofilter.py"
    loki_db = workspace / "loki.db"
    input_positions = workspace / "tests" / "biofilter" / "data" / "input" / "input_positions.txt"  # noqa E501
    outcomes_prefix = workspace / "tests" / "biofilter" / "data" / "outcome" / "out_test_biofilter_annotation_cli_position__"  # noqa E501

    # Validate required files
    assert input_positions.exists(), f"Input file not found: {input_positions}"
    assert loki_db.exists(), f"LOKI DB not found: {loki_db}"

    # CLI command (mirrors VSCode debug configuration)
    cmd = [
        sys.executable, str(biofilter_py),
        "--verbose",
        "--knowledge", str(loki_db),
        "--position-file", str(input_positions),
        "--source", "kegg", "reactome", "go",
        "--annotate", "position_label", "snp", "position", "gene", "upstream", "downstream",  # noqa E501
        "--report-invalid-input",
        "--report-configuration",
        "--overwrite",
        "--prefix", str(outcomes_prefix),
        "--ucsc-build-version", "19",
    ]

    # Run Biofilter
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Print output for debugging
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    # Ensure successful execution
    assert result.returncode == 0, f"Biofilter failed: {result.stderr}"

    # Validate output files
    expected_files = [
        outcomes_prefix.with_suffix(".configuration"),
        # outcomes_prefix.with_suffix(".invalid.position"),
    ]
    for file in expected_files:
        assert file.exists(), f"Expected output file not found: {file}"
        assert file.stat().st_size > 0, f"Output file is empty: {file}"
