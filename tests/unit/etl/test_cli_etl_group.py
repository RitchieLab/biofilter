from __future__ import annotations

from click.testing import CliRunner

import biofilter.api.cli.groups.etl as etl_cli_mod


class FakeETLFacade:
    def __init__(self):
        self.calls = []
        self.index_result = (True, "index ok")

    def update(self, **kwargs):
        self.calls.append(("update", kwargs))

    def restart(self, **kwargs):
        self.calls.append(("restart", kwargs))
        return {"ok": True}

    def index(self, **kwargs):
        self.calls.append(("index", kwargs))
        return self.index_result


class FakeDBFacade:
    def __init__(self):
        self.connect_calls = 0

    def connect(self):
        self.connect_calls += 1


def _patch_biofilter(monkeypatch, fake_db, fake_etl, capture):
    class FakeBiofilter:
        def __init__(self, db_uri=None, debug_mode=False):
            capture.setdefault("init", []).append(
                {"db_uri": db_uri, "debug_mode": debug_mode}
            )
            self.db = fake_db
            self.etl = fake_etl

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
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, fake_etl, capture)
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
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, fake_etl, capture)
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


def test_index_calls_etl_index_and_echoes_message_on_success(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    fake_etl = FakeETLFacade()
    fake_etl.index_result = (True, "indexes rebuilt")
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, fake_etl, capture)
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
    fake_etl.index_result = (False, "index failed")
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, fake_etl, capture)
    _patch_require_db_uri(monkeypatch, capture)

    result = runner.invoke(
        etl_cli_mod.etl,
        ["index", "--db-uri", "sqlite:///etl.db"],
    )

    assert result.exit_code != 0
    assert "index failed" in result.output
