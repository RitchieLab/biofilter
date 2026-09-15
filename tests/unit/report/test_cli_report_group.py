from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

import biofilter.api.cli.groups.report as report_cli_mod


class FakeDataFrame:
    def __init__(self):
        self.to_csv_calls = []
        self.to_string_calls = []

    def to_csv(self, output, index=False):
        self.to_csv_calls.append((output, index))
        Path(output).write_text("col_a\n1\n", encoding="utf-8")

    def to_string(self, index=False):
        self.to_string_calls.append(index)
        return "col_a\n1"


class FakeReportFacade:
    def __init__(self):
        self.calls = []
        self.list_result = []
        self.explain_result = "explain"
        self.example_input_result = "example"
        self.available_columns_result = ["col_a", "col_b"]
        self.run_result = FakeDataFrame()
        self.run_error = None

    def list(self, verbose=False):
        self.calls.append(("list", {"verbose": verbose}))
        return self.list_result

    def explain(self, identifier):
        self.calls.append(("explain", {"identifier": identifier}))
        return self.explain_result

    def example_input(self, identifier):
        self.calls.append(("example_input", {"identifier": identifier}))
        return self.example_input_result

    def available_columns(self, identifier, print_output=True):
        self.calls.append(
            (
                "available_columns",
                {"identifier": identifier, "print_output": print_output},
            )
        )
        return self.available_columns_result

    def refresh(self):
        self.calls.append(("refresh", {}))

    def run(self, identifier, **kwargs):
        self.calls.append(("run", {"identifier": identifier, "kwargs": kwargs}))
        if self.run_error is not None:
            raise self.run_error
        return self.run_result


def _patch_biofilter(monkeypatch, facade, capture):
    class FakeBiofilter:
        def __init__(self, db_uri=None, debug_mode=False):
            capture["db_uri"] = db_uri
            capture["debug_mode"] = debug_mode
            self.report = facade

    monkeypatch.setattr(report_cli_mod, "Biofilter", FakeBiofilter)


def test_report_list_verbose_prints_description_and_module(monkeypatch):
    runner = CliRunner()
    facade = FakeReportFacade()
    facade.list_result = [
        {
            "name": "etl_status",
            "description": "ETL Status report",
            "module": "report_etl_status",
        }
    ]
    capture = {}
    _patch_biofilter(monkeypatch, facade, capture)

    result = runner.invoke(
        report_cli_mod.report,
        ["list", "--db-uri", "sqlite:///test.db", "--verbose"],
    )

    assert result.exit_code == 0, result.output
    assert "Available Reports" in result.output
    assert "etl_status" in result.output
    assert "ETL Status report" in result.output
    assert "module: report_etl_status" in result.output
    assert capture["db_uri"] == "sqlite:///test.db"
    assert capture["debug_mode"] is False


def test_report_list_empty_prints_no_reports(monkeypatch):
    runner = CliRunner()
    facade = FakeReportFacade()
    capture = {}
    _patch_biofilter(monkeypatch, facade, capture)

    result = runner.invoke(
        report_cli_mod.report,
        ["list", "--db-uri", "sqlite:///test.db"],
    )

    assert result.exit_code == 0, result.output
    assert "No reports found." in result.output


def test_report_explain_prints_text(monkeypatch):
    runner = CliRunner()
    facade = FakeReportFacade()
    facade.explain_result = "This is explain text"
    _patch_biofilter(monkeypatch, facade, {})

    result = runner.invoke(
        report_cli_mod.report,
        [
            "explain",
            "--db-uri",
            "sqlite:///test.db",
            "--report-name",
            "etl_status",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "This is explain text" in result.output


def test_report_example_input_prints_text(monkeypatch):
    runner = CliRunner()
    facade = FakeReportFacade()
    facade.example_input_result = '{"id": "x"}'
    _patch_biofilter(monkeypatch, facade, {})

    result = runner.invoke(
        report_cli_mod.report,
        [
            "example-input",
            "--db-uri",
            "sqlite:///test.db",
            "--report-name",
            "etl_status",
        ],
    )

    assert result.exit_code == 0, result.output
    assert '{"id": "x"}' in result.output


def test_report_available_columns_prints_columns(monkeypatch):
    runner = CliRunner()
    facade = FakeReportFacade()
    facade.available_columns_result = ["source_system", "data_source"]
    _patch_biofilter(monkeypatch, facade, {})

    result = runner.invoke(
        report_cli_mod.report,
        [
            "available-columns",
            "--db-uri",
            "sqlite:///test.db",
            "--report-name",
            "etl_status",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "source_system" in result.output
    assert "data_source" in result.output


def test_report_refresh_calls_refresh_and_prints_confirmation(monkeypatch):
    runner = CliRunner()
    facade = FakeReportFacade()
    _patch_biofilter(monkeypatch, facade, {})

    result = runner.invoke(
        report_cli_mod.report,
        ["refresh", "--db-uri", "sqlite:///test.db"],
    )

    assert result.exit_code == 0, result.output
    assert "Report cache refreshed" in result.output
    assert ("refresh", {}) in facade.calls


def test_report_run_with_output_writes_csv_file(monkeypatch, tmp_path):
    runner = CliRunner()
    facade = FakeReportFacade()
    fake_df = FakeDataFrame()
    facade.run_result = fake_df
    _patch_biofilter(monkeypatch, facade, {})

    out_file = tmp_path / "report.csv"
    result = runner.invoke(
        report_cli_mod.report,
        [
            "run",
            "--db-uri",
            "sqlite:///test.db",
            "--name",
            "etl_status",
            "--output",
            str(out_file),
        ],
    )

    assert result.exit_code == 0, result.output
    assert out_file.exists()
    assert "Report exported to" in result.output
    assert fake_df.to_csv_calls == [(str(out_file), False)]
    run_call = [payload for name, payload in facade.calls if name == "run"][-1]
    assert run_call["kwargs"] == {}


def test_report_run_accepts_report_name_option(monkeypatch):
    runner = CliRunner()
    facade = FakeReportFacade()
    _patch_biofilter(monkeypatch, facade, {})

    result = runner.invoke(
        report_cli_mod.report,
        [
            "run",
            "--db-uri",
            "sqlite:///test.db",
            "--report-name",
            "etl_status",
        ],
    )

    assert result.exit_code == 0, result.output
    run_call = [payload for name, payload in facade.calls if name == "run"][-1]
    assert run_call["identifier"] == "etl_status"


def test_report_run_without_csv_prints_table(monkeypatch):
    runner = CliRunner()
    facade = FakeReportFacade()
    fake_df = FakeDataFrame()
    facade.run_result = fake_df
    _patch_biofilter(monkeypatch, facade, {})

    result = runner.invoke(
        report_cli_mod.report,
        [
            "run",
            "--db-uri",
            "sqlite:///test.db",
            "--name",
            "etl_status",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "col_a" in result.output
    assert fake_df.to_string_calls == [False]
    run_call = [payload for name, payload in facade.calls if name == "run"][-1]
    assert run_call["kwargs"] == {}


def test_report_run_params_template_prints_example_and_skips_run(monkeypatch):
    runner = CliRunner()
    facade = FakeReportFacade()
    facade.example_input_result = {
        "input_data": ["TP53", "BRCA1"],
        "relationship_scope": "input_to_any",
    }
    _patch_biofilter(monkeypatch, facade, {})

    result = runner.invoke(
        report_cli_mod.report,
        [
            "run",
            "--db-uri",
            "sqlite:///test.db",
            "--name",
            "entity_relationship_model",
            "--params-template",
        ],
    )

    assert result.exit_code == 0, result.output
    assert '"input_data": [' in result.output
    assert '"relationship_scope": "input_to_any"' in result.output
    assert not any(name == "run" for name, _ in facade.calls)


def test_report_run_with_direct_inputs_and_params(monkeypatch):
    runner = CliRunner()
    facade = FakeReportFacade()
    _patch_biofilter(monkeypatch, facade, {})

    result = runner.invoke(
        report_cli_mod.report,
        [
            "run",
            "--db-uri",
            "sqlite:///test.db",
            "--name",
            "entity_relationship_model",
            "--input",
            "TP53",
            "--input",
            "BRCA1",
            "--param",
            "relationship_scope=input_to_any",
            "--param",
            "deduplicate_pairs=true",
        ],
    )

    assert result.exit_code == 0, result.output
    run_call = [payload for name, payload in facade.calls if name == "run"][-1]
    assert run_call["kwargs"] == {
        "input_data": ["TP53", "BRCA1"],
        "relationship_scope": "input_to_any",
        "deduplicate_pairs": True,
    }


def test_report_run_param_value_from_file_reference(monkeypatch, tmp_path):
    runner = CliRunner()
    facade = FakeReportFacade()
    _patch_biofilter(monkeypatch, facade, {})

    values_file = tmp_path / "relationship_types.txt"
    values_file.write_text("gene_to_pathway\ngene_to_protein\n", encoding="utf-8")

    result = runner.invoke(
        report_cli_mod.report,
        [
            "run",
            "--db-uri",
            "sqlite:///test.db",
            "--name",
            "entity_relationship_model",
            "--param",
            f"relationship_types=@{values_file}",
            "--input",
            "TP53",
        ],
    )

    assert result.exit_code == 0, result.output
    run_call = [payload for name, payload in facade.calls if name == "run"][-1]
    assert run_call["kwargs"]["relationship_types"] == [
        "gene_to_pathway",
        "gene_to_protein",
    ]


def test_report_run_rejects_input_conflict_with_param_input_data(monkeypatch):
    runner = CliRunner()
    facade = FakeReportFacade()
    _patch_biofilter(monkeypatch, facade, {})

    result = runner.invoke(
        report_cli_mod.report,
        [
            "run",
            "--db-uri",
            "sqlite:///test.db",
            "--name",
            "entity_relationship_model",
            "--input",
            "TP53",
            "--param",
            'input_data=["BRCA1"]',
        ],
    )

    assert result.exit_code != 0
    assert "Input conflict: use --input/--input-file for inputs" in result.output
    assert "input_data" in result.output
    assert not any(name == "run" for name, _ in facade.calls)


def test_report_run_with_input_file_csv_and_column(monkeypatch, tmp_path):
    runner = CliRunner()
    facade = FakeReportFacade()
    _patch_biofilter(monkeypatch, facade, {})

    input_file = tmp_path / "genes.csv"
    input_file.write_text("gene,other\nTP53,1\nBRCA1,2\n", encoding="utf-8")

    result = runner.invoke(
        report_cli_mod.report,
        [
            "run",
            "--db-uri",
            "sqlite:///test.db",
            "--name",
            "entity_filter",
            "--input-file",
            str(input_file),
            "--input-column",
            "gene",
        ],
    )

    assert result.exit_code == 0, result.output
    run_call = [payload for name, payload in facade.calls if name == "run"][-1]
    assert run_call["kwargs"]["input_data"] == ["TP53", "BRCA1"]


def test_report_run_with_input_file_csv_numeric_column_skips_header(monkeypatch, tmp_path):
    runner = CliRunner()
    facade = FakeReportFacade()
    _patch_biofilter(monkeypatch, facade, {})

    input_file = tmp_path / "genes.csv"
    input_file.write_text("gene,other\nTP53,1\nBRCA1,2\n", encoding="utf-8")

    result = runner.invoke(
        report_cli_mod.report,
        [
            "run",
            "--db-uri",
            "sqlite:///test.db",
            "--name",
            "entity_filter",
            "--input-file",
            str(input_file),
            "--input-column",
            "0",
        ],
    )

    assert result.exit_code == 0, result.output
    run_call = [payload for name, payload in facade.calls if name == "run"][-1]
    assert run_call["kwargs"]["input_data"] == ["TP53", "BRCA1"]


def test_report_run_input_column_without_csv_file_errors(monkeypatch, tmp_path):
    runner = CliRunner()
    facade = FakeReportFacade()
    _patch_biofilter(monkeypatch, facade, {})

    input_file = tmp_path / "genes.txt"
    input_file.write_text("TP53\nBRCA1\n", encoding="utf-8")

    result = runner.invoke(
        report_cli_mod.report,
        [
            "run",
            "--db-uri",
            "sqlite:///test.db",
            "--name",
            "entity_filter",
            "--input-file",
            str(input_file),
            "--input-column",
            "gene",
        ],
    )

    assert result.exit_code != 0
    assert "--input-column is only valid with CSV input files." in result.output


def test_report_run_params_file_json_and_overrides(monkeypatch, tmp_path):
    runner = CliRunner()
    facade = FakeReportFacade()
    _patch_biofilter(monkeypatch, facade, {})

    params_file = tmp_path / "params.json"
    params_file.write_text(
        '{"relationship_scope": "between_inputs", "deduplicate_pairs": false}',
        encoding="utf-8",
    )

    result = runner.invoke(
        report_cli_mod.report,
        [
            "run",
            "--db-uri",
            "sqlite:///test.db",
            "--name",
            "entity_relationship_model",
            "--params-file",
            str(params_file),
            "--params-json",
            '{"relationship_scope":"input_to_any"}',
            "--param",
            "deduplicate_pairs=true",
            "--input",
            "TP53",
        ],
    )

    assert result.exit_code == 0, result.output
    run_call = [payload for name, payload in facade.calls if name == "run"][-1]
    assert run_call["kwargs"] == {
        "relationship_scope": "input_to_any",
        "deduplicate_pairs": True,
        "input_data": ["TP53"],
    }


def test_report_run_params_json_list_maps_to_input_data(monkeypatch):
    runner = CliRunner()
    facade = FakeReportFacade()
    _patch_biofilter(monkeypatch, facade, {})

    result = runner.invoke(
        report_cli_mod.report,
        [
            "run",
            "--db-uri",
            "sqlite:///test.db",
            "--name",
            "entity_filter",
            "--params-json",
            '["TP53", "BRCA1"]',
        ],
    )

    assert result.exit_code == 0, result.output
    run_call = [payload for name, payload in facade.calls if name == "run"][-1]
    assert run_call["kwargs"]["input_data"] == ["TP53", "BRCA1"]


def test_report_run_invalid_name_prints_friendly_error_without_traceback(monkeypatch):
    runner = CliRunner()
    facade = FakeReportFacade()
    facade.list_result = [
        {"name": "etl_status"},
        {"name": "etl_packages"},
    ]
    facade.run_error = ValueError(
        "Report not found: 'qry_etl_status'. Available reports: ['etl_packages', 'etl_status']"  # noqa E501
    )
    _patch_biofilter(monkeypatch, facade, {})

    result = runner.invoke(
        report_cli_mod.report,
        [
            "run",
            "--db-uri",
            "sqlite:///test.db",
            "--name",
            "qry_etl_status",
        ],
    )

    assert result.exit_code != 0
    assert "Report not found: 'qry_etl_status'." in result.output
    assert "Did you mean:" in result.output
    assert "etl_status" in result.output
    assert "biofilter report list" in result.output
    assert "Traceback" not in result.output
