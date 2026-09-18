from __future__ import annotations

from pathlib import Path

import pytest

from biofilter import Biofilter
from biofilter.api.cli.main import main
from biofilter.modules.db.models import SystemConfig


def _insert_config_key(db_uri: str, key: str, value: str):
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
                description="integration marker",
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
def test_cli_db_upgrade_is_idempotent(sqlite_seeded_db_uri, cli_runner):
    # Fresh create-db does not stamp alembic_version; stamp first.
    stamp = cli_runner.invoke(
        main,
        ["--db-uri", sqlite_seeded_db_uri, "db", "migrate", "--stamp-head"],
    )
    assert stamp.exit_code == 0, stamp.output

    first = cli_runner.invoke(
        main,
        ["--db-uri", sqlite_seeded_db_uri, "db", "upgrade"],
    )
    assert first.exit_code == 0, first.output
    assert "Database upgraded (schema + seeds)." in first.output

    second = cli_runner.invoke(
        main,
        ["--db-uri", sqlite_seeded_db_uri, "db", "upgrade"],
    )
    assert second.exit_code == 0, second.output
    assert "Database upgraded (schema + seeds)." in second.output


@pytest.mark.integration
def test_cli_db_backup_and_restore_sqlite(sqlite_seeded_db_uri, cli_runner, tmp_path):
    backup_file = Path(tmp_path) / "biofilter_backup.sqlite"
    marker_key = "integration_restore_marker"

    backup = cli_runner.invoke(
        main,
        [
            "--db-uri",
            sqlite_seeded_db_uri,
            "db",
            "backup",
            "--out",
            str(backup_file),
        ],
    )
    assert backup.exit_code == 0, backup.output
    assert backup_file.exists()

    _insert_config_key(sqlite_seeded_db_uri, marker_key, "before_restore")
    assert _has_config_key(sqlite_seeded_db_uri, marker_key)

    restore = cli_runner.invoke(
        main,
        [
            "--db-uri",
            sqlite_seeded_db_uri,
            "db",
            "restore",
            "--in",
            str(backup_file),
        ],
    )
    assert restore.exit_code == 0, restore.output
    assert "Restore completed." in restore.output

    assert not _has_config_key(sqlite_seeded_db_uri, marker_key)


# @pytest.mark.integration
# def test_cli_db_export_import_roundtrip_sqlite(sqlite_seeded_db_uri, cli_runner, tmp_path):
#     marker_key = "integration_roundtrip_marker"
#     _insert_config_key(sqlite_seeded_db_uri, marker_key, "from_source")
#     assert _has_config_key(sqlite_seeded_db_uri, marker_key)

#     bundle_dir = Path(tmp_path) / "bundle_csv"
#     export = cli_runner.invoke(
#         main,
#         [
#             "--db-uri",
#             sqlite_seeded_db_uri,
#             "db",
#             "export",
#             "--out",
#             str(bundle_dir),
#             "--format",
#             "csv",
#         ],
#     )
#     assert export.exit_code == 0, export.output
#     assert (bundle_dir / "manifest.json").exists()
#     assert (bundle_dir / "tables").exists()

#     target_db_path = Path(tmp_path) / "biofilter_target.db"
#     target_db_uri = f"sqlite:///{target_db_path}"

#     create_target = cli_runner.invoke(
#         main,
#         ["db", "create-db", "--db-uri", target_db_uri, "--overwrite"],
#     )
#     assert create_target.exit_code == 0, create_target.output
#     assert not _has_config_key(target_db_uri, marker_key)

#     imported = cli_runner.invoke(
#         main,
#         [
#             "--db-uri",
#             target_db_uri,
#             "db",
#             "import",
#             "--in",
#             str(bundle_dir),
#             "--format",
#             "csv",
#             "--no-rebuild-indexes",
#         ],
#     )
#     assert imported.exit_code == 0, imported.output
#     assert "Bundle import completed." in imported.output
#     assert _has_config_key(target_db_uri, marker_key)

