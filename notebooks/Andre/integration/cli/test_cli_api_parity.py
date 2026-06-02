from __future__ import annotations

from pathlib import Path

import pytest

from biofilter import Biofilter
from biofilter.api.cli.main import main
from biofilter.modules.db.models import SystemConfig

PARITY_CONTRACT = {
    "db": {
        "component": "db",
        "operations": [
            ("create_db", "create-db"),
            ("migrate", "migrate"),
            ("upgrade", "upgrade"),
            ("backup", "backup"),
            ("restore", "restore"),
            ("export", "export"),
            ("import_", "import"),
        ],
    },
    "report": {
        "component": "report",
        "operations": [
            ("list", "list"),
            ("explain", "explain"),
            ("example_input", "example-input"),
            ("available_columns", "available-columns"),
            ("refresh", "refresh"),
            ("run", "run"),
        ],
    },
    "etl": {
        "component": "etl",
        "operations": [
            ("update", "update"),
            ("restart", "restart"),
            ("index", "index"),
        ],
    },
}


def _set_config_key(db_uri: str, key: str, value: str):
    bf = Biofilter(db_uri=db_uri, debug_mode=False)
    with bf.db.get_session() as session:
        row = (
            session.query(SystemConfig)
            .filter(SystemConfig.key == key)
            .one_or_none()
        )
        if row is None:
            row = SystemConfig(
                key=key,
                value=value,
                type="string",
                description="integration parity marker",
                editable=True,
            )
            session.add(row)
        else:
            row.value = value
        session.commit()


def _has_config_key(db_uri: str, key: str) -> bool:
    bf = Biofilter(db_uri=db_uri, debug_mode=False)
    with bf.db.get_session() as session:
        return (
            session.query(SystemConfig)
            .filter(SystemConfig.key == key)
            .one_or_none()
            is not None
        )


@pytest.mark.integration
def test_cli_exposes_supported_api_surface():
    bf = Biofilter(debug_mode=False)

    for group_name, contract in PARITY_CONTRACT.items():
        component = getattr(bf, contract["component"])
        cli_commands = set(main.commands[group_name].commands.keys())

        for api_method, cli_command in contract["operations"]:
            assert hasattr(component, api_method)
            assert cli_command in cli_commands


@pytest.mark.integration
def test_api_cli_report_list_parity(sqlite_seeded_db_uri, cli_runner):
    bf = Biofilter(db_uri=sqlite_seeded_db_uri, debug_mode=False)
    api_rows = bf.report.list()
    api_names = {row.get("name") for row in api_rows if row.get("name")}

    result = cli_runner.invoke(
        main,
        ["--db-uri", sqlite_seeded_db_uri, "report", "list"],
    )

    assert result.exit_code == 0, result.output
    for report_name in api_names:
        assert report_name in result.output


@pytest.mark.integration
def test_api_cli_report_explain_parity(sqlite_seeded_db_uri, cli_runner):
    bf = Biofilter(db_uri=sqlite_seeded_db_uri, debug_mode=False)
    api_text = bf.report.explain("etl_status").strip()

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
    assert api_text in result.output


@pytest.mark.integration
def test_api_cli_report_available_columns_parity(sqlite_seeded_db_uri, cli_runner):
    bf = Biofilter(db_uri=sqlite_seeded_db_uri, debug_mode=False)
    api_columns = bf.report.available_columns("etl_status")

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
    for col in api_columns:
        assert col in result.output


@pytest.mark.integration
def test_api_backup_cli_restore_parity(sqlite_seeded_db_uri, cli_runner, tmp_path):
    marker_key = "api_cli_backup_restore_marker"
    backup_path = Path(tmp_path) / "api_backup.sqlite"

    bf = Biofilter(db_uri=sqlite_seeded_db_uri, debug_mode=False)
    bf.db.backup(backup_path)
    assert backup_path.exists()

    _set_config_key(sqlite_seeded_db_uri, marker_key, "created_after_backup")
    assert _has_config_key(sqlite_seeded_db_uri, marker_key)

    restore = cli_runner.invoke(
        main,
        [
            "--db-uri",
            sqlite_seeded_db_uri,
            "db",
            "restore",
            "--in",
            str(backup_path),
        ],
    )
    assert restore.exit_code == 0, restore.output
    assert not _has_config_key(sqlite_seeded_db_uri, marker_key)


@pytest.mark.integration
def test_api_export_cli_import_parity_csv(
    sqlite_seeded_db_uri, cli_runner, tmp_path
):
    marker_key = "api_cli_export_import_marker"
    bundle_dir = Path(tmp_path) / "bundle_from_api_csv"
    target_db_path = Path(tmp_path) / "target_from_cli_import.db"
    target_db_uri = f"sqlite:///{target_db_path}"

    _set_config_key(sqlite_seeded_db_uri, marker_key, "from_source_api_export")
    assert _has_config_key(sqlite_seeded_db_uri, marker_key)

    bf = Biofilter(db_uri=sqlite_seeded_db_uri, debug_mode=False)
    bf.db.export(out_dir=bundle_dir, fmt="csv")
    assert (bundle_dir / "manifest.json").exists()

    create_target = cli_runner.invoke(
        main,
        ["db", "create-db", "--db-uri", target_db_uri, "--overwrite"],
    )
    assert create_target.exit_code == 0, create_target.output

    imported = cli_runner.invoke(
        main,
        [
            "--db-uri",
            target_db_uri,
            "db",
            "import",
            "--in",
            str(bundle_dir),
            "--format",
            "csv",
            "--no-rebuild-indexes",
        ],
    )
    assert imported.exit_code == 0, imported.output
    assert _has_config_key(target_db_uri, marker_key)


@pytest.mark.integration
def test_api_export_filtered_cli_import_allow_missing_parity_csv(
    sqlite_seeded_db_uri, cli_runner, tmp_path
):
    marker_key = "api_cli_filtered_export_marker"
    bundle_dir = Path(tmp_path) / "bundle_filtered_api_csv"
    target_db_path = Path(tmp_path) / "target_filtered_cli_import.db"
    target_db_uri = f"sqlite:///{target_db_path}"

    _set_config_key(sqlite_seeded_db_uri, marker_key, "filtered_from_api_export")
    assert _has_config_key(sqlite_seeded_db_uri, marker_key)

    bf = Biofilter(db_uri=sqlite_seeded_db_uri, debug_mode=False)
    bf.db.export(out_dir=bundle_dir, fmt="csv", tables=["system_config"])
    assert (bundle_dir / "manifest.json").exists()

    create_target = cli_runner.invoke(
        main,
        ["db", "create-db", "--db-uri", target_db_uri, "--overwrite"],
    )
    assert create_target.exit_code == 0, create_target.output

    imported = cli_runner.invoke(
        main,
        [
            "--db-uri",
            target_db_uri,
            "db",
            "import",
            "--in",
            str(bundle_dir),
            "--format",
            "csv",
            "--allow-missing-tables",
            "--no-rebuild-indexes",
        ],
    )
    assert imported.exit_code == 0, imported.output
    assert _has_config_key(target_db_uri, marker_key)


@pytest.mark.integration
def test_cli_export_filtered_api_import_allow_missing_parity_csv(
    sqlite_seeded_db_uri, cli_runner, tmp_path
):
    marker_key = "cli_api_filtered_import_marker"
    bundle_dir = Path(tmp_path) / "bundle_filtered_cli_csv"
    target_db_path = Path(tmp_path) / "target_filtered_api_import.db"
    target_db_uri = f"sqlite:///{target_db_path}"

    _set_config_key(sqlite_seeded_db_uri, marker_key, "filtered_from_cli_export")
    assert _has_config_key(sqlite_seeded_db_uri, marker_key)

    exported = cli_runner.invoke(
        main,
        [
            "--db-uri",
            sqlite_seeded_db_uri,
            "db",
            "export",
            "--out",
            str(bundle_dir),
            "--format",
            "csv",
            "--table",
            "system_config",
        ],
    )
    assert exported.exit_code == 0, exported.output
    assert (bundle_dir / "manifest.json").exists()

    create_target = cli_runner.invoke(
        main,
        ["db", "create-db", "--db-uri", target_db_uri, "--overwrite"],
    )
    assert create_target.exit_code == 0, create_target.output

    bf_target = Biofilter(db_uri=target_db_uri, debug_mode=False)
    bf_target.db.import_(
        in_dir=bundle_dir,
        fmt="csv",
        rebuild_indexes=False,
        allow_missing_tables=True,
    )

    assert _has_config_key(target_db_uri, marker_key)
