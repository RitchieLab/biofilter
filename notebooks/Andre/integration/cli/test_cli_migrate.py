from __future__ import annotations

import pytest

from biofilter.api.cli.main import main


@pytest.mark.integration
def test_cli_db_migrate_status_sqlite(sqlite_seeded_db_uri, cli_runner):
    result = cli_runner.invoke(
        main,
        ["--db-uri", sqlite_seeded_db_uri, "db", "migrate", "--status"],
    )

    assert result.exit_code == 0, result.output
    assert "Status displayed." in result.output


@pytest.mark.integration
def test_cli_db_migrate_dry_run_sqlite(sqlite_seeded_db_uri, cli_runner):
    result = cli_runner.invoke(
        main,
        ["--db-uri", sqlite_seeded_db_uri, "db", "migrate", "--dry-run"],
    )

    assert result.exit_code == 0, result.output
    assert "Dry-run completed" in result.output


@pytest.mark.integration
def test_cli_db_migrate_stamp_head_sqlite(sqlite_seeded_db_uri, cli_runner):
    result = cli_runner.invoke(
        main,
        ["--db-uri", sqlite_seeded_db_uri, "db", "migrate", "--stamp-head"],
    )

    assert result.exit_code == 0, result.output
    assert "Database stamped to head." in result.output


@pytest.mark.integration
@pytest.mark.postgres
def test_cli_db_migrate_status_postgres(postgres_db_uri, cli_runner):
    create = cli_runner.invoke(
        main,
        ["db", "create-db", "--db-uri", postgres_db_uri, "--overwrite"],
    )
    assert create.exit_code == 0, create.output

    result = cli_runner.invoke(
        main,
        ["--db-uri", postgres_db_uri, "db", "migrate", "--status"],
    )

    assert result.exit_code == 0, result.output
    assert "Status displayed." in result.output

