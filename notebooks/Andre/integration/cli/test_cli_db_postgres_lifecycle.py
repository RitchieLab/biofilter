from __future__ import annotations

import shutil
from pathlib import Path
from uuid import uuid4

import pytest

from biofilter import Biofilter
from biofilter.api.cli.main import main
from biofilter.modules.db.models import SystemConfig


def _skip_if_pg_client_version_mismatch(result, tool_name: str):
    details = (result.output or "")
    if result.exception is not None:
        details += f"\n{result.exception}"

    lowered = details.lower()
    mismatch_signals = [
        "server version mismatch",
        "aborting because of server version mismatch",
        "pg_dump version",
        "pg_restore version",
        "unsupported version",
    ]
    if any(signal in lowered for signal in mismatch_signals):
        pytest.skip(
            f"{tool_name} client/server version mismatch in local environment: {details}"
        )


def _postgres_uri_with_db_name(base_uri: str, db_name: str) -> str:
    prefix, _, _ = base_uri.rpartition("/")
    return f"{prefix}/{db_name}"


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
@pytest.mark.postgres
def test_cli_db_export_import_roundtrip_postgres_filtered(
    postgres_db_uri, cli_runner, tmp_path
):
    source_uri = _postgres_uri_with_db_name(
        postgres_db_uri, f"biofilter_src_{uuid4().hex[:8]}"
    )
    target_uri = _postgres_uri_with_db_name(
        postgres_db_uri, f"biofilter_tgt_{uuid4().hex[:8]}"
    )

    create_source = cli_runner.invoke(
        main,
        ["db", "create-db", "--db-uri", source_uri, "--overwrite"],
    )
    assert create_source.exit_code == 0, create_source.output

    marker_key = "integration_pg_roundtrip_marker"
    _insert_config_key(source_uri, marker_key, "from_postgres_source")
    assert _has_config_key(source_uri, marker_key)

    bundle_dir = Path(tmp_path) / "bundle_pg_csv"
    exported = cli_runner.invoke(
        main,
        [
            "--db-uri",
            source_uri,
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
        ["db", "create-db", "--db-uri", target_uri, "--overwrite"],
    )
    assert create_target.exit_code == 0, create_target.output

    imported = cli_runner.invoke(
        main,
        [
            "--db-uri",
            target_uri,
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
    assert _has_config_key(target_uri, marker_key)


@pytest.mark.integration
@pytest.mark.postgres
def test_cli_db_backup_restore_postgres_roundtrip(
    postgres_db_uri, cli_runner, tmp_path
):
    if shutil.which("pg_dump") is None or shutil.which("pg_restore") is None:
        pytest.skip("pg_dump/pg_restore are required for postgres backup/restore tests")

    db_uri = _postgres_uri_with_db_name(
        postgres_db_uri, f"biofilter_bkp_{uuid4().hex[:8]}"
    )
    create = cli_runner.invoke(
        main,
        ["db", "create-db", "--db-uri", db_uri, "--overwrite"],
    )
    assert create.exit_code == 0, create.output

    backup_file = Path(tmp_path) / "postgres_backup.dump"
    backup = cli_runner.invoke(
        main,
        ["--db-uri", db_uri, "db", "backup", "--out", str(backup_file)],
    )
    _skip_if_pg_client_version_mismatch(backup, "pg_dump")
    assert backup.exit_code == 0, backup.output
    assert backup_file.exists()

    marker_key = "integration_pg_restore_marker"
    _insert_config_key(db_uri, marker_key, "created_after_backup")
    assert _has_config_key(db_uri, marker_key)

    restore = cli_runner.invoke(
        main,
        ["--db-uri", db_uri, "db", "restore", "--in", str(backup_file)],
    )
    _skip_if_pg_client_version_mismatch(restore, "pg_restore")
    assert restore.exit_code == 0, restore.output
    assert not _has_config_key(db_uri, marker_key)
