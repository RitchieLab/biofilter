import subprocess


def test_biofilter_with_args(capsys):
    # Define the command to execute biofilter.py with specific arguments
    command = [
        "python",
        "biofilter_modules/biofilter.py",
        "--verbose",
        "--knowledge",
        "/Users/andrerico/Works/Sys/BIOFILTER_2_4_x/loki_modules/loki-20220926.db",  # noqa E501
        "--position-file",
        "/Users/andrerico/Works/Sys/BIOFILTER_2_4_x/data_test/input_filename",
        "--annotate",
        "position_label",
        "snp",
        "position",
        "gene",
        "upstream",
        "downstream",
        "--report-invalid-input",
        "--overwrite",
        "--prefix",
        "output_prefix",
        "--ucsc-build-version",
        "38",
    ]

    # Run the command using subprocess and capture the output
    process = subprocess.run(command, capture_output=True, text=True)

    # TODO: Improve the test to check the output
    assert "OK: 10 result" in process.stderr

    # # Check that the process completes without error
    # assert process.returncode == 0, f"Process failed with error: {process.stderr}"  # noqa E501

    # # Capture the output to check for expected strings (if any specific
    # output is expected)  # noqa E501
    # output = process.stdout
    # assert "Processing completed" in output or "Expected output text" in output  # noqa E501
    # assert "Invalid input" not in output  # Ensure no invalid input warnings
    # if not expected

    # # If you want to capture any error output
    # error_output = process.stderr
    # assert "Error" not in error_output
