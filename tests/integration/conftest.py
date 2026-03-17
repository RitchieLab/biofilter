from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from biofilter.api.cli.main import main


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture
def sqlite_db_target(tmp_path):
    """
    Temporary SQLite DB target for integration tests.
    Returns:
        (db_uri, db_path)
    """
    db_path = Path(tmp_path) / "biofilter_integration.db"
    db_uri = f"sqlite:///{db_path}"
    return db_uri, db_path


@pytest.fixture
def test_data_dir():
    return Path(__file__).resolve().parents[1] / "test_data"


@pytest.fixture
def sqlite_seeded_db_uri(cli_runner, sqlite_db_target):
    db_uri, _ = sqlite_db_target
    result = cli_runner.invoke(
        main,
        ["db", "create-db", "--db-uri", db_uri, "--overwrite"],
    )
    assert result.exit_code == 0, result.output
    return db_uri


@pytest.fixture(scope="session")
def postgres_db_uri():
    """
    Start an ephemeral PostgreSQL container and return a target DB URI that can
    be created by `biofilter db create-db`.
    """
    postgres_mod = pytest.importorskip(
        "testcontainers.postgres",
        reason="testcontainers is required for postgres integration tests",
    )

    try:
        with postgres_mod.PostgresContainer("postgres:16-alpine") as pg:
            base_uri = pg.get_connection_url()
            if base_uri.startswith("postgresql://"):
                base_uri = base_uri.replace(
                    "postgresql://",
                    "postgresql+psycopg2://",
                    1,
                )

            # Use a dedicated target DB name for explicit create-db coverage.
            target_db_name = "biofilter_integration"
            prefix, _, _ = base_uri.rpartition("/")
            yield f"{prefix}/{target_db_name}"
    except Exception as exc:
        pytest.skip(f"PostgreSQL container unavailable: {exc}")
