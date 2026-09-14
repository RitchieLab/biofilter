from __future__ import annotations

import pytest

from biofilter import Biofilter


@pytest.mark.integration
def test_api_report_list(sqlite_seeded_db_uri):
    """
    Script flow reference:
      - scripts/runs_tests/reports/list.py
    """
    bf = Biofilter(db_uri=sqlite_seeded_db_uri, debug_mode=False)
    rows = bf.report.list()

    assert isinstance(rows, list)
    assert rows
    assert any(row.get("name") == "etl_status" for row in rows)


@pytest.mark.integration
@pytest.mark.postgres
def test_api_report_list_in_postgres(postgres_db_uri, cli_runner):
    """
    API report flow on top of a containerized PostgreSQL DB.
    """
    from biofilter.api.cli.main import main

    create = cli_runner.invoke(
        main,
        ["db", "create-db", "--db-uri", postgres_db_uri, "--overwrite"],
    )
    assert create.exit_code == 0, create.output

    bf = Biofilter(db_uri=postgres_db_uri, debug_mode=False)
    rows = bf.report.list()

    assert isinstance(rows, list)
    assert rows
    assert any("name" in row for row in rows)

