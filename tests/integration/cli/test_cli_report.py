from __future__ import annotations

import pytest

from biofilter.api.cli.main import main


@pytest.mark.integration
def test_cli_report_list(sqlite_seeded_db_uri, cli_runner):
    """
    Launch.json reference:
    - "CLI - Reports List"
    """
    result = cli_runner.invoke(
        main,
        ["--db-uri", sqlite_seeded_db_uri, "report", "list"],
    )

    assert result.exit_code == 0, result.output
    assert "Available Reports" in result.output
    assert "etl_status" in result.output


@pytest.mark.integration
def test_cli_report_explain(sqlite_seeded_db_uri, cli_runner):
    """
    Launch.json reference:
    - "CLI - Reports Explain"
    """
    result = cli_runner.invoke(
        main,
        [
            "--db-uri",
            sqlite_seeded_db_uri,
            "report",
            "explain",
            "--report-name",
            "etl_status",
        ],
    )

    assert result.exit_code == 0, result.output
    assert ("`etl_status`" in result.output) or ("ETL Status" in result.output)


@pytest.mark.integration
def test_cli_report_example_input(sqlite_seeded_db_uri, cli_runner):
    result = cli_runner.invoke(
        main,
        [
            "--db-uri",
            sqlite_seeded_db_uri,
            "report",
            "example-input",
            "--report-name",
            "etl_status",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output.strip() in {"", "None"}


@pytest.mark.integration
def test_cli_report_available_columns(sqlite_seeded_db_uri, cli_runner):
    result = cli_runner.invoke(
        main,
        [
            "--db-uri",
            sqlite_seeded_db_uri,
            "report",
            "available-columns",
            "--report-name",
            "etl_status",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "source_system" in result.output
    assert "data_source" in result.output


@pytest.mark.integration
def test_cli_report_run_with_output_writes_csv(
    sqlite_seeded_db_uri, cli_runner, tmp_path
):
    output_file = tmp_path / "etl_packages_report.csv"

    result = cli_runner.invoke(
        main,
        [
            "--db-uri",
            sqlite_seeded_db_uri,
            "report",
            "run",
            "--name",
            "etl_packages",
            "--output",
            str(output_file),
        ],
    )

    assert result.exit_code == 0, result.output
    assert output_file.exists()
    content = output_file.read_text(encoding="utf-8")
    assert "package_id" in content


@pytest.mark.integration
def test_cli_report_run_accepts_report_name_option(sqlite_seeded_db_uri, cli_runner):
    result = cli_runner.invoke(
        main,
        [
            "--db-uri",
            sqlite_seeded_db_uri,
            "report",
            "run",
            "--report-name",
            "etl_status",
        ],
    )

    assert result.exit_code == 0, result.output


@pytest.mark.integration
def test_cli_report_run_accepts_inputs_and_generic_params(
    sqlite_seeded_db_uri, cli_runner, tmp_path
):
    input_file = tmp_path / "inputs.txt"
    input_file.write_text("TP53\nBRCA1\n", encoding="utf-8")
    output_file = tmp_path / "etl_status_report.csv"

    result = cli_runner.invoke(
        main,
        [
            "--db-uri",
            sqlite_seeded_db_uri,
            "report",
            "run",
            "--name",
            "etl_status",
            "--input-file",
            str(input_file),
            "--input",
            "NOT_FOUND_ENTITY",
            "--params-json",
            '{"only_active": true}',
            "--param",
            "source_system=NCBI",
            "--output",
            str(output_file),
        ],
    )

    assert result.exit_code == 0, result.output
    assert output_file.exists()


@pytest.mark.integration
def test_cli_report_run_params_template_for_report(sqlite_seeded_db_uri, cli_runner):
    result = cli_runner.invoke(
        main,
        [
            "--db-uri",
            sqlite_seeded_db_uri,
            "report",
            "run",
            "--name",
            "entity_relationship_model",
            "--params-template",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "relationship_scope" in result.output
    assert "input_data" in result.output


@pytest.mark.integration
def test_cli_report_run_param_value_from_file_reference(
    sqlite_seeded_db_uri, cli_runner, tmp_path
):
    source_file = tmp_path / "sources.txt"
    source_file.write_text("NCBI\n", encoding="utf-8")
    output_file = tmp_path / "etl_status_filtered.csv"

    result = cli_runner.invoke(
        main,
        [
            "--db-uri",
            sqlite_seeded_db_uri,
            "report",
            "run",
            "--name",
            "etl_status",
            "--param",
            f"source_system=@{source_file}",
            "--output",
            str(output_file),
        ],
    )

    assert result.exit_code == 0, result.output
    assert output_file.exists()


@pytest.mark.integration
def test_cli_report_run_rejects_input_conflict(sqlite_seeded_db_uri, cli_runner):
    result = cli_runner.invoke(
        main,
        [
            "--db-uri",
            sqlite_seeded_db_uri,
            "report",
            "run",
            "--name",
            "etl_status",
            "--input",
            "TP53",
            "--param",
            "input_data=[\"BRCA1\"]",
        ],
    )

    assert result.exit_code != 0
    assert "Input conflict" in result.output


@pytest.mark.integration
def test_cli_report_run_invalid_report_name(sqlite_seeded_db_uri, cli_runner):
    result = cli_runner.invoke(
        main,
        [
            "--db-uri",
            sqlite_seeded_db_uri,
            "report",
            "run",
            "--name",
            "report_that_does_not_exist",
        ],
    )

    assert result.exit_code != 0
    details = result.output
    if result.exception is not None:
        details += f"\n{result.exception}"
    assert "Report not found" in details


@pytest.mark.integration
@pytest.mark.postgres
def test_cli_report_list_in_postgres(postgres_db_uri, cli_runner):
    """
    Same command path as launch.json, executed against containerized Postgres.
    """
    create = cli_runner.invoke(
        main,
        ["db", "create-db", "--db-uri", postgres_db_uri, "--overwrite"],
    )
    assert create.exit_code == 0, create.output

    result = cli_runner.invoke(
        main,
        ["--db-uri", postgres_db_uri, "report", "list"],
    )
    assert result.exit_code == 0, result.output
    assert "Available Reports" in result.output
