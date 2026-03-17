from __future__ import annotations

import pytest

from biofilter.api.cli.main import main


@pytest.mark.integration
def test_cli_db_create_db_in_sqlite(sqlite_db_target, cli_runner):
    """
    Launch.json reference:
    - "CLI - DB Create DB in SQLite"
    """
    db_uri, db_path = sqlite_db_target

    result = cli_runner.invoke(
        main,
        ["db", "create-db", "--db-uri", db_uri, "--overwrite"],
    )

    assert result.exit_code == 0, result.output
    assert db_path.exists()


@pytest.mark.integration
@pytest.mark.postgres
def test_cli_db_create_db_in_postgres(postgres_db_uri, cli_runner):
    """
    Launch.json reference:
    - "CLI - DB Create DB in PS"
    """
    result = cli_runner.invoke(
        main,
        ["db", "create-db", "--db-uri", postgres_db_uri, "--overwrite"],
    )
    assert result.exit_code == 0, result.output

