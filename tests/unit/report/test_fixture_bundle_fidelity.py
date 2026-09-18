"""
The fixture bundle must look like a real one.

Three times now a report has failed against the fixture for a column the
real tables carry and the fixture did not — `data_source_id` on aliases,
`etl_package_id` on relationships. Each was found by a report crashing,
which is late and tells you nothing about the other tables.

This compares the fixture against the models, which are the schema
contract (ADR-004 §2.1). A fixture table missing a column the models
declare is a gap waiting for the next report to fall into.
"""

from __future__ import annotations

import pytest

from biofilter.modules.db.base import Base
from biofilter.modules.report import Bundle

#: Columns a fixture is not expected to carry. Keep this short and
#: justified — every entry is a test that will not run.
TOLERATED_GAPS = {
    # Written by the build, meaningless in a hand-made fixture.
    "biofilter_metadata",
}


def test_fixture_tables_are_declared_by_the_models(fixture_bundle):
    with Bundle.open(fixture_bundle) as bundle:
        undeclared = sorted(set(bundle.tables) - set(Base.metadata.tables))

    assert undeclared == [], (
        f"The fixture carries tables the models do not declare: {undeclared}. "
        f"Either the models are behind, or the fixture invented something."
    )


def test_fixture_columns_match_the_models(fixture_bundle):
    """
    Every column a fixture table declares must be one the models know,
    and every column the models declare must be in the fixture — the
    second half is what catches the gaps that have bitten us.
    """
    problems: list[str] = []

    with Bundle.open(fixture_bundle) as bundle:
        for name in sorted(bundle.tables):
            if name in TOLERATED_GAPS or name not in Base.metadata.tables:
                continue

            declared = set(Base.metadata.tables[name].columns.keys())
            present = {
                row[0]
                for row in bundle.con.execute(f'DESCRIBE "{name}"').fetchall()
            }

            missing = sorted(declared - present)
            extra = sorted(present - declared)
            if missing:
                problems.append(f"{name}: fixture is missing {missing}")
            if extra:
                problems.append(f"{name}: fixture has {extra}, models do not")

    assert problems == [], "\n".join(problems)
