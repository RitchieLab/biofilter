from __future__ import annotations

from click.testing import CliRunner
import pandas as pd

import biofilter.api.cli.groups.etl as etl_cli_mod


class FakeETLFacade:
    def __init__(self):
        self.calls = []
        self.index_result = (True, "index ok")
        self.update_all_result = {
            "selected": 0,
            "skipped": 0,
            "processed": 0,
            "succeeded": 0,
            "failed": 0,
        }

    def update(self, **kwargs):
        self.calls.append(("update", kwargs))

    def update_all(self, **kwargs):
        self.calls.append(("update_all", kwargs))
        return self.update_all_result

    def restart(self, **kwargs):
        self.calls.append(("restart", kwargs))
        return {"ok": True}

    def index(self, **kwargs):
        self.calls.append(("index", kwargs))
        return self.index_result


class FakeReportFacade:
    def __init__(self):
        self.calls = []
        self.responses = {}

    def run(self, identifier, **kwargs):
        self.calls.append(("run", identifier, kwargs))
        return self.responses.get(identifier, pd.DataFrame())


class FakeDBFacade:
    def __init__(self):
        self.connect_calls = 0

    def connect(self):
        self.connect_calls += 1


def _patch_biofilter(monkeypatch, fake_db, fake_etl, fake_report, capture):
    class FakeBiofilter:
        def __init__(self, db_uri=None, debug_mode=False):
            capture.setdefault("init", []).append(
                {"db_uri": db_uri, "debug_mode": debug_mode}
            )
            self.db = fake_db
            self.etl = fake_etl
            self.report = fake_report

    monkeypatch.setattr(etl_cli_mod, "Biofilter", FakeBiofilter)


def _patch_require_db_uri(monkeypatch, capture):
    def fake_require_db_uri(ctx, local_db_uri=None):
        capture.setdefault("require_db_uri", []).append(local_db_uri)
        return local_db_uri or "sqlite:///resolved.db"

    monkeypatch.setattr(etl_cli_mod, "require_db_uri", fake_require_db_uri)


def test_to_list_or_none_helper():
    assert etl_cli_mod._to_list_or_none(()) is None
    assert etl_cli_mod._to_list_or_none([]) is None
    assert etl_cli_mod._to_list_or_none(["x"]) == ["x"]
    assert etl_cli_mod._to_list_or_none(("a", "b")) == ["a", "b"]


def test_group_invocation_without_subcommand_shows_help():
    runner = CliRunner()
    result = runner.invoke(etl_cli_mod.etl, [])
    assert result.exit_code == 0, result.output
    assert "Run and manage ETL pipelines." in result.output


def test_update_calls_etl_update_with_normalized_lists(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    fake_etl = FakeETLFacade()
    fake_report = FakeReportFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, fake_etl, fake_report, capture)
    _patch_require_db_uri(monkeypatch, capture)

    result = runner.invoke(
        etl_cli_mod.etl,
        [
            "update",
            "--db-uri",
            "sqlite:///etl.db",
            "--source-system",
            "NCBI",
            "--data-source",
            "dbsnp_sample",
            "--run-step",
            "extract",
            "--run-step",
            "transform",
            "--force-step",
            "load",
            "--debug",
        ],
    )

    assert result.exit_code == 0, result.output
    assert fake_db.connect_calls == 1
    assert fake_etl.calls == [
        (
            "update",
            {
                "source_system": ["NCBI"],
                "data_sources": ["dbsnp_sample"],
                "run_steps": ["extract", "transform"],
                "force_steps": ["load"],
            },
        )
    ]
    assert capture["init"] == [{"db_uri": "sqlite:///etl.db", "debug_mode": True}]


def test_restart_calls_etl_restart_with_normalized_lists(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    fake_etl = FakeETLFacade()
    fake_report = FakeReportFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, fake_etl, fake_report, capture)
    _patch_require_db_uri(monkeypatch, capture)

    result = runner.invoke(
        etl_cli_mod.etl,
        [
            "restart",
            "--db-uri",
            "sqlite:///etl.db",
            "--data-source",
            "dbsnp_sample",
            "--source-system",
            "NCBI",
            "--delete-files",
            "--debug",
        ],
    )

    assert result.exit_code == 0, result.output
    assert fake_db.connect_calls == 1
    assert fake_etl.calls == [
        (
            "restart",
            {
                "data_source": ["dbsnp_sample"],
                "source_system": ["NCBI"],
                "delete_files": True,
            },
        )
    ]
    assert capture["init"] == [{"db_uri": "sqlite:///etl.db", "debug_mode": True}]


def test_update_all_calls_facade_and_prints_summary(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    fake_etl = FakeETLFacade()
    fake_report = FakeReportFacade()
    fake_etl.update_all_result = {
        "selected": 5,
        "skipped": 2,
        "processed": 3,
        "succeeded": 2,
        "failed": 1,
    }
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, fake_etl, fake_report, capture)
    _patch_require_db_uri(monkeypatch, capture)

    result = runner.invoke(
        etl_cli_mod.etl,
        [
            "update-all",
            "--db-uri",
            "sqlite:///etl.db",
            "--source-system",
            "NCBI",
            "--data-source",
            "hgnc",
            "--drop-files",
            "--all",
            "--stop-on-error",
            "--debug",
        ],
    )

    assert result.exit_code == 0, result.output
    assert fake_db.connect_calls == 1
    assert fake_etl.calls == [
        (
            "update_all",
            {
                "source_system": ["NCBI"],
                "data_sources": ["hgnc"],
                "drop_files_on_success": True,
                "only_active": False,
                "stop_on_error": True,
            },
        )
    ]
    assert "selected=5" in result.output
    assert "skipped=2" in result.output
    assert "processed=3" in result.output
    assert "succeeded=2" in result.output
    assert "failed=1" in result.output


def test_index_calls_etl_index_and_echoes_message_on_success(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    fake_etl = FakeETLFacade()
    fake_report = FakeReportFacade()
    fake_etl.index_result = (True, "indexes rebuilt")
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, fake_etl, fake_report, capture)
    _patch_require_db_uri(monkeypatch, capture)

    result = runner.invoke(
        etl_cli_mod.etl,
        [
            "index",
            "--db-uri",
            "sqlite:///etl.db",
            "--group",
            "genes",
            "--group",
            "variant",
            "--drop-only",
            "--no-drop-first",
            "--no-write-mode",
            "--no-read-mode",
            "--debug",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "indexes rebuilt" in result.output
    assert fake_db.connect_calls == 1
    assert fake_etl.calls == [
        (
            "index",
            {
                "groups": ["genes", "variant"],
                "drop_only": True,
                "drop_first": False,
                "set_write_mode": False,
                "set_read_mode": False,
            },
        )
    ]


def test_index_raises_click_exception_when_etl_returns_not_ok(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    fake_etl = FakeETLFacade()
    fake_report = FakeReportFacade()
    fake_etl.index_result = (False, "index failed")
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, fake_etl, fake_report, capture)
    _patch_require_db_uri(monkeypatch, capture)

    result = runner.invoke(
        etl_cli_mod.etl,
        ["index", "--db-uri", "sqlite:///etl.db"],
    )

    assert result.exit_code != 0
    assert "index failed" in result.output


def test_status_prints_all_data_sources_with_load_result_and_last_execution(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    fake_etl = FakeETLFacade()
    fake_report = FakeReportFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, fake_etl, fake_report, capture)
    _patch_require_db_uri(monkeypatch, capture)

    fake_report.responses["etl_status"] = pd.DataFrame(
        [
            {
                "data_source_id": 1,
                "data_type": "Gene",
                "source_system": "NCBI",
                "data_source": "hgnc",
                "data_source_active": True,
                "data_version": "2026.1",
            },
            {
                "data_source_id": 2,
                "data_type": "Disease",
                "source_system": "NCBI",
                "data_source": "mondo",
                "data_source_active": True,
                "data_version": "2026.2",
            },
            {
                "data_source_id": 3,
                "data_type": "Gene",
                "source_system": "EBI",
                "data_source": "ensembl",
                "data_source_active": False,
                "data_version": "2026.3",
            },
        ]
    )
    fake_report.responses["etl_packages"] = pd.DataFrame(
        [
            {
                "package_id": 10,
                "data_source_id": 1,
                "source_system": "NCBI",
                "data_source": "hgnc",
                "operation_type": "load",
                "load_status": "completed",
                "load_end": "2026-03-01 10:00:00",
                "created_at": "2026-03-01 10:01:00",
            },
            {
                "package_id": 20,
                "data_source_id": 3,
                "source_system": "EBI",
                "data_source": "ensembl",
                "operation_type": "load",
                "load_status": "failed",
                "load_end": None,
                "created_at": "2026-03-02 13:30:00",
            },
            {
                "package_id": 9,
                "data_source_id": 1,
                "source_system": "NCBI",
                "data_source": "hgnc",
                "operation_type": "extract",
                "load_status": None,
                "load_end": None,
                "created_at": "2026-02-25 08:00:00",
            },
        ]
    )

    result = runner.invoke(
        etl_cli_mod.etl,
        [
            "status",
            "--db-uri",
            "sqlite:///etl.db",
            "--source-system",
            "NCBI",
            "--data-source",
            "hgnc",
            "--all",
            "--debug",
        ],
    )

    assert result.exit_code == 0, result.output
    assert fake_db.connect_calls == 1
    assert ("run", "etl_status", {"source_system": ["NCBI"], "data_sources": ["hgnc"], "only_active": False}) in fake_report.calls  # noqa E501
    assert ("run", "etl_packages", {"source_system": ["NCBI"], "data_sources": ["hgnc"], "only_active": False}) in fake_report.calls  # noqa E501
    assert "hgnc" in result.output
    assert "loaded" in result.output
    assert "ensembl" in result.output
    assert "failed" in result.output
    assert "mondo" in result.output
    assert "never" in result.output
    assert "2026-03-01 10:00:00" in result.output
    assert "2026-03-02 13:30:00" in result.output
    assert "Domain" in result.output
    assert "active" in result.output
    assert "data_version" in result.output


def test_status_uses_only_latest_load_per_data_source(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    fake_etl = FakeETLFacade()
    fake_report = FakeReportFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, fake_etl, fake_report, capture)
    _patch_require_db_uri(monkeypatch, capture)

    fake_report.responses["etl_status"] = pd.DataFrame(
        [
            {
                "data_source_id": 1,
                "data_type": "Gene",
                "source_system": "NCBI",
                "data_source": "hgnc",
                "data_source_active": True,
                "data_version": "2026.1",
            },
            {
                "data_source_id": 1,
                "data_type": "Gene",
                "source_system": "NCBI",
                "data_source": "hgnc",
                "data_source_active": True,
                "data_version": "2026.1",
            },
        ]
    )
    fake_report.responses["etl_packages"] = pd.DataFrame(
        [
            {
                "package_id": 100,
                "data_source_id": 1,
                "source_system": "NCBI",
                "data_source": "hgnc",
                "operation_type": "load",
                "load_status": "completed",
                "created_at": "2026-03-10 10:00:00",
            },
            {
                "package_id": 101,
                "data_source_id": 1,
                "source_system": "NCBI",
                "data_source": "hgnc",
                "operation_type": "load",
                "load_status": "running",
                "created_at": "2026-03-10 10:05:00",
            },
            {
                "package_id": 99,
                "data_source_id": 1,
                "source_system": "NCBI",
                "data_source": "hgnc",
                "operation_type": "load",
                "load_status": "failed",
                "created_at": "2026-03-10 09:59:00",
            },
        ]
    )

    result = runner.invoke(
        etl_cli_mod.etl,
        [
            "status",
            "--db-uri",
            "sqlite:///etl.db",
        ],
    )

    assert result.exit_code == 0, result.output
    assert result.output.count("hgnc") == 1
    assert "running" in result.output


def test_status_ignores_not_applicable_load_attempts(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    fake_etl = FakeETLFacade()
    fake_report = FakeReportFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, fake_etl, fake_report, capture)
    _patch_require_db_uri(monkeypatch, capture)

    fake_report.responses["etl_status"] = pd.DataFrame(
        [
            {
                "data_source_id": 1,
                "data_type": "Gene",
                "source_system": "NCBI",
                "data_source": "hgnc",
                "data_source_active": True,
                "data_version": "2026.1",
            }
        ]
    )
    fake_report.responses["etl_packages"] = pd.DataFrame(
        [
            {
                "package_id": 11,
                "data_source_id": 1,
                "source_system": "NCBI",
                "data_source": "hgnc",
                "operation_type": "load",
                "status": "not-applicable",
                "load_status": "not-applicable",
                "created_at": "2026-03-10 10:10:00",
            },
            {
                "package_id": 10,
                "data_source_id": 1,
                "source_system": "NCBI",
                "data_source": "hgnc",
                "operation_type": "load",
                "status": "completed",
                "load_status": "completed",
                "load_end": "2026-03-10 10:00:00",
                "created_at": "2026-03-10 10:01:00",
            },
        ]
    )

    result = runner.invoke(
        etl_cli_mod.etl,
        [
            "status",
            "--db-uri",
            "sqlite:///etl.db",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "loaded" in result.output
    assert "2026-03-10 10:00:00" in result.output
    assert "2026-03-10 10:10:00" not in result.output


def test_status_handles_empty_etl_status_report(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    fake_etl = FakeETLFacade()
    fake_report = FakeReportFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, fake_etl, fake_report, capture)
    _patch_require_db_uri(monkeypatch, capture)

    fake_report.responses["etl_status"] = pd.DataFrame()

    result = runner.invoke(
        etl_cli_mod.etl,
        [
            "status",
            "--db-uri",
            "sqlite:///etl.db",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "No data sources found." in result.output
