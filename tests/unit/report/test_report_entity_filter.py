from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from biofilter.modules.db.base import Base
from biofilter.modules.db.models import Entity, EntityAlias, EntityGroup
from biofilter.modules.report.reports.report_entity_filter import EntityFilterReport


class DummyLogger:
    def log(self, message, level="INFO"):
        pass


def _report(session, **kwargs):
    return EntityFilterReport(
        session=session,
        db=SimpleNamespace(engine=getattr(session, "bind", None)),
        logger=DummyLogger(),
        **kwargs,
    )


def _seed_entities(session):
    group = EntityGroup(name="Gene")
    session.add(group)
    session.flush()

    tp53 = Entity(group_id=group.id, has_conflict=False, is_active=True)
    brca1 = Entity(group_id=group.id, has_conflict=True, is_active=False)
    apoe = Entity(group_id=group.id, has_conflict=False, is_active=True)
    session.add_all([tp53, brca1, apoe])
    session.flush()

    session.add_all(
        [
            EntityAlias(
                entity_id=tp53.id,
                group_id=group.id,
                alias_value="TP53",
                alias_norm="tp53",
                alias_type="preferred",
                is_primary=True,
            ),
            EntityAlias(
                entity_id=brca1.id,
                group_id=group.id,
                alias_value="BRCA1",
                alias_type="preferred",
                is_primary=True,
            ),
            EntityAlias(
                entity_id=apoe.id,
                group_id=group.id,
                alias_value="APOE",
                alias_type="preferred",
                is_primary=True,
            ),
            EntityAlias(
                entity_id=brca1.id,
                group_id=group.id,
                alias_value="APOE",
                alias_type="synonym",
                is_primary=False,
            ),
        ]
    )
    session.commit()


def test_entity_filter_matches_aliases_and_marks_not_found():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        _seed_entities(session)
        report = _report(
            session,
            input_data=["TP53", "brca1", "APOE", "NOT_FOUND_ENTITY"],
        )

        df = report.run()

    assert list(df.columns) == [
        "input_original",
        "input",
        "is_primary",
        "entity_id",
        "primary_name",
        "group_id",
        "group_name",
        "has_conflict",
        "is_active",
        "is_deactive",
        "data_source_id",
        "observation",
    ]

    not_found = df[df["observation"] == "not found"]
    assert len(not_found) == 1
    assert not_found.iloc[0]["input_original"] == "NOT_FOUND_ENTITY"

    tp53 = df[df["primary_name"] == "TP53"].iloc[0]
    assert tp53["input_original"] == "TP53"
    assert tp53["is_deactive"] is False

    brca1 = df[
        (df["primary_name"] == "BRCA1") & (df["input_original"] == "brca1")
    ].iloc[0]
    assert brca1["is_deactive"] is True

    apoe = df[df["input_original"] == "APOE"]
    assert len(apoe) == 2
    assert set(apoe["observation"].tolist()) == {"multiple matches"}


def test_entity_filter_requires_at_least_one_non_empty_value():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        report = _report(session, input_data=["", " "])
        with pytest.raises(ValueError, match="at least one non-empty value"):
            report.run()
