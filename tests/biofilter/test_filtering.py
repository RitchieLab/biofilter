import subprocess
import sys
from pathlib import Path


def test_biofilter_filtering_snp_gene_list():
    """
    Runs the legacy Biofilter with SNP -> Gene filtering mode.
    Validates that only SNPs mapped to the provided genes are returned.
    """

    workspace = Path(__file__).resolve().parents[2]
    biofilter_py = workspace / "biofilter_modules" / "biofilter.py"
    loki_db = workspace / "loki.db"
    input_snps = workspace / "tests" / "biofilter" / "data" / "input" / "input_snps.txt"  # noqa E501
    input_genes = workspace / "tests" / "biofilter" / "data" / "input" / "input_genes.txt"  # noqa E501
    outcomes_prefix = workspace / "tests" / "biofilter" / "data" / "outcome" / "filtering" / "test_biofilter_filtering_snp_gene_list__"  # noqa E501

    assert input_snps.exists()
    assert input_genes.exists()
    assert loki_db.exists()

    cmd = [
        sys.executable, str(biofilter_py),
        "--verbose",
        "--knowledge", str(loki_db),
        "--snp-file", str(input_snps),
        "--gene-file", str(input_genes),
        "--filter", "snp", "gene",
        "--overwrite",
        "--prefix", str(outcomes_prefix),
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    assert result.returncode == 0, "Filtering execution failed"

    filtered_file = outcomes_prefix.with_suffix(".snp-gene")
    assert filtered_file.exists(), "Filtered output missing"
    with open(filtered_file) as f:
        lines = [l.strip() for l in f if l.strip()]
    lines = lines[1:]  # drop header
    assert all(line.startswith("rs") for line in lines)
    assert len(lines) < 4, "Expected subset of SNPs (filtered)"


def test_biofilter_filtering_snps_lists():
    """
    Tests SNP <-> SNP filtering using two SNP input lists
    Simulates filtering by genotyping platform.
    """

    workspace = Path(__file__).resolve().parents[2]
    biofilter_py = workspace / "biofilter_modules" / "biofilter.py"
    loki_db = workspace / "loki.db"
    input1 = workspace / "tests" / "biofilter" / "data" / "input" / "filtering" / "input_snps_1.txt"  # noqa E501
    input2 = workspace / "tests" / "biofilter" / "data" / "input" / "filtering" / "input_snps_2.txt"  # noqa E501
    outcomes_prefix = workspace / "tests" / "biofilter" / "data" / "outcome" / "filtering" / "out_filter_snps_"  # noqa E501

    assert input1.exists(), f"Missing input1: {input1}"
    assert input2.exists(), f"Missing input2: {input2}"
    assert loki_db.exists(), f"Missing test.db: {loki_db}"

    cmd = [
        sys.executable, str(biofilter_py),
        "--knowledge", str(loki_db),
        "--snp-file", str(input1),
        "--snp-file", str(input2),
        "--filter", "snp",
        "--prefix", str(outcomes_prefix),
        "--overwrite",
        "--verbose",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    print("STDOUT:\n", result.stdout)
    print("STDERR:\n", result.stderr)

    assert result.returncode == 0, f"Filtering failed: {result.stderr}"

    # TODO: Review the outputs
    # filtered_file = outcomes_prefix.with_suffix(".filtered.snp")
    # assert filtered_file.exists(), "Filtered SNP output missing"
    # assert filtered_file.stat().st_size > 0, "Filtered SNP output is empty"

    # # Validate expected SNPs
    # with open(filtered_file) as f:
    #     lines = [l.strip() for l in f if l.strip() and not l.startswith("#")]
    # expected = {"rs9", "rs14", "rs15", "rs16"}
    # assert set(lines) == expected, f"Unexpected SNPs in output: {set(lines)}"
