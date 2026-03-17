from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

import biofilter.api.cli.groups.db as db_cli_mod


class FakeDBFacade:
    def __init__(self):
        self.calls = []

    def create_db(self, db_uri=None, overwrite=False):
        self.calls.append(
            ("create_db", {"db_uri": db_uri, "overwrite": overwrite})
        )

    def migrate(self, **kwargs):
        self.calls.append(("migrate", kwargs))
        return True

    def connect(self):
        self.calls.append(("connect", {}))

    def upgrade(self, seed_dir="seed"):
        self.calls.append(("upgrade", {"seed_dir": seed_dir}))
        return True

    def backup(self, out_path):
        self.calls.append(("backup", {"out_path": out_path}))
        return out_path

    def restore(self, in_path):
        self.calls.append(("restore", {"in_path": in_path}))

    def export(self, **kwargs):
        self.calls.append(("export", kwargs))
        return kwargs["out_dir"]

    def import_(self, **kwargs):
        self.calls.append(("import_", kwargs))


def _patch_biofilter(monkeypatch, fake_db, capture):
    class FakeBiofilter:
        def __init__(self, db_uri=None, debug_mode=False):
            capture.setdefault("init", []).append(
                {"db_uri": db_uri, "debug_mode": debug_mode}
            )
            self.db = fake_db

    monkeypatch.setattr(db_cli_mod, "Biofilter", FakeBiofilter)


def _patch_require_db_uri(monkeypatch, capture):
    def fake_require_db_uri(ctx, local_db_uri=None):
        capture.setdefault("require_db_uri", []).append(local_db_uri)
        return local_db_uri or "sqlite:///resolved.db"

    monkeypatch.setattr(db_cli_mod, "require_db_uri", fake_require_db_uri)


def test_create_db_calls_create_db_with_expected_args(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, capture)

    result = runner.invoke(
        db_cli_mod.db,
        [
            "create-db",
            "--db-uri",
            "sqlite:///created.db",
            "--overwrite",
            "--debug",
        ],
    )

    assert result.exit_code == 0, result.output
    assert capture["init"] == [{"db_uri": None, "debug_mode": True}]
    assert fake_db.calls == [
        ("create_db", {"db_uri": "sqlite:///created.db", "overwrite": True})
    ]


def test_migrate_default_action_is_upgrade(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, capture)
    _patch_require_db_uri(monkeypatch, capture)

    result = runner.invoke(
        db_cli_mod.db,
        ["migrate", "--db-uri", "sqlite:///x.db"],
    )

    assert result.exit_code == 0, result.output
    assert "Database migration completed." in result.output
    assert fake_db.calls == [
        ("migrate", {"action": "upgrade", "target": "head", "force": False})
    ]
    assert capture["init"] == [{"db_uri": "sqlite:///x.db", "debug_mode": False}]


def test_migrate_status_action_and_message(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, capture)
    _patch_require_db_uri(monkeypatch, capture)

    result = runner.invoke(
        db_cli_mod.db,
        ["migrate", "--db-uri", "sqlite:///x.db", "--status"],
    )

    assert result.exit_code == 0, result.output
    assert "Status displayed." in result.output
    assert fake_db.calls[0] == (
        "migrate",
        {"action": "status", "target": "head", "force": False},
    )


def test_migrate_stamp_head_action_and_message(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, capture)
    _patch_require_db_uri(monkeypatch, capture)

    result = runner.invoke(
        db_cli_mod.db,
        ["migrate", "--db-uri", "sqlite:///x.db", "--stamp-head"],
    )

    assert result.exit_code == 0, result.output
    assert "Database stamped to head." in result.output
    assert fake_db.calls[0] == (
        "migrate",
        {"action": "stamp-head", "target": "head", "force": False},
    )


def test_migrate_dry_run_action_and_message(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, capture)
    _patch_require_db_uri(monkeypatch, capture)

    result = runner.invoke(
        db_cli_mod.db,
        ["migrate", "--db-uri", "sqlite:///x.db", "--dry-run"],
    )

    assert result.exit_code == 0, result.output
    assert "Dry-run completed" in result.output
    assert fake_db.calls[0] == (
        "migrate",
        {"action": "dry-run", "target": "head", "force": False},
    )


def test_upgrade_calls_connect_then_migrate_then_upgrade(monkeypatch):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, capture)
    _patch_require_db_uri(monkeypatch, capture)

    result = runner.invoke(
        db_cli_mod.db,
        [
            "upgrade",
            "--db-uri",
            "sqlite:///up.db",
            "--seed-dir",
            "custom_seed",
            "--force",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Database upgraded (schema + seeds)." in result.output
    assert fake_db.calls == [
        ("connect", {}),
        ("migrate", {"action": "upgrade", "target": "head", "force": True}),
        ("upgrade", {"seed_dir": "custom_seed"}),
    ]


def test_backup_calls_backup_and_prints_created_path(monkeypatch, tmp_path):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, capture)
    _patch_require_db_uri(monkeypatch, capture)

    out_file = tmp_path / "backup.dump"
    result = runner.invoke(
        db_cli_mod.db,
        ["backup", "--db-uri", "sqlite:///x.db", "--out", str(out_file)],
    )

    assert result.exit_code == 0, result.output
    assert "Backup created" in result.output
    assert fake_db.calls == [("backup", {"out_path": out_file})]
    assert capture["init"] == [{"db_uri": "sqlite:///x.db", "debug_mode": False}]


def test_restore_calls_restore_and_prints_confirmation(monkeypatch, tmp_path):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, capture)
    _patch_require_db_uri(monkeypatch, capture)

    in_file = tmp_path / "snapshot.dump"
    in_file.write_text("x", encoding="utf-8")

    result = runner.invoke(
        db_cli_mod.db,
        ["restore", "--db-uri", "sqlite:///x.db", "--in", str(in_file)],
    )

    assert result.exit_code == 0, result.output
    assert "Restore completed." in result.output
    assert fake_db.calls == [("restore", {"in_path": in_file})]


def test_export_calls_export_with_normalized_format(monkeypatch, tmp_path):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, capture)
    _patch_require_db_uri(monkeypatch, capture)

    out_dir = tmp_path / "bundle"
    result = runner.invoke(
        db_cli_mod.db,
        [
            "export",
            "--db-uri",
            "sqlite:///x.db",
            "--out",
            str(out_dir),
            "--format",
            "CSV",
            "--schema-version",
            "4.2.0",
            "--chunksize",
            "123",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Bundle exported" in result.output
    assert fake_db.calls == [
        (
            "export",
            {
                "out_dir": out_dir,
                "fmt": "csv",
                "schema_version": "4.2.0",
                "chunksize": 123,
            },
        )
    ]


def test_import_calls_import_with_flag_inversion(monkeypatch, tmp_path):
    runner = CliRunner()
    fake_db = FakeDBFacade()
    capture = {}
    _patch_biofilter(monkeypatch, fake_db, capture)
    _patch_require_db_uri(monkeypatch, capture)

    in_dir = tmp_path / "bundle"
    in_dir.mkdir(parents=True, exist_ok=True)

    result = runner.invoke(
        db_cli_mod.db,
        [
            "import",
            "--db-uri",
            "sqlite:///x.db",
            "--in",
            str(in_dir),
            "--format",
            "CSV",
            "--no-rebuild-indexes",
            "--no-reset-sequences",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Bundle import completed." in result.output
    assert fake_db.calls == [
        (
            "import_",
            {
                "in_dir": in_dir,
                "fmt": "csv",
                "rebuild_indexes": False,
                "reset_postgres_sequences": False,
            },
        )
    ]
