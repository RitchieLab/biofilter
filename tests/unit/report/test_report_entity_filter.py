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


def _make_engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return engine


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


def _seed_pathways(session):
    group = EntityGroup(name="Pathway")
    session.add(group)
    session.flush()

    p1 = Entity(group_id=group.id, has_conflict=False, is_active=True)
    p2 = Entity(group_id=group.id, has_conflict=False, is_active=True)
    p3 = Entity(group_id=group.id, has_conflict=False, is_active=True)
    session.add_all([p1, p2, p3])
    session.flush()

    session.add_all(
        [
            EntityAlias(
                entity_id=p1.id,
                group_id=group.id,
                alias_value="MAPK signaling pathway",
                alias_norm="mapk signaling pathway",
                alias_type="preferred",
                is_primary=True,
            ),
            EntityAlias(
                entity_id=p2.id,
                group_id=group.id,
                alias_value="PI3K-Akt signaling pathway",
                alias_norm="pi3k-akt signaling pathway",
                alias_type="preferred",
                is_primary=True,
            ),
            EntityAlias(
                entity_id=p3.id,
                group_id=group.id,
                alias_value="Apoptosis pathway",
                alias_norm="apoptosis pathway",
                alias_type="preferred",
                is_primary=True,
            ),
        ]
    )
    session.commit()


def test_entity_filter_matches_aliases_and_marks_not_found():
    engine = _make_engine()
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


def test_entity_filter_invalid_match_mode():
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        report = _report(session, input_data=["TP53"], match_mode="typo")
        with pytest.raises(ValueError, match="match_mode must be one of"):
            report.run()


def test_entity_filter_like_mode_returns_substring_matches():
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        _seed_pathways(session)
        # "signaling" matches both MAPK and PI3K-Akt pathways
        report = _report(session, input_data=["signaling"], match_mode="like")
        df = report.run()

    matched = df[df["observation"] != "not found"]
    assert len(matched) >= 2
    primary_names = set(matched["primary_name"].tolist())
    assert "MAPK signaling pathway" in primary_names
    assert "PI3K-Akt signaling pathway" in primary_names

    not_found = df[df["observation"] == "not found"]
    assert len(not_found) == 0


def test_entity_filter_like_mode_not_found():
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        _seed_pathways(session)
        report = _report(session, input_data=["xyznonexistent"], match_mode="like")
        df = report.run()

    assert len(df) == 1
    assert df.iloc[0]["observation"] == "not found"


def test_entity_filter_fuzzy_mode_returns_similar_matches():
    pytest.importorskip("rapidfuzz")

    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        _seed_pathways(session)
        # Slightly misspelled — fuzzy should still match
        report = _report(
            session,
            input_data=["MAPK signalling pathway"],  # double-l typo
            match_mode="fuzzy",
            similarity_threshold=70,
        )
        df = report.run()

    matched = df[df["observation"] != "not found"]
    assert len(matched) >= 1
    assert "MAPK signaling pathway" in matched["primary_name"].tolist()
    assert "similarity_score" in df.columns
    assert matched["similarity_score"].iloc[0] >= 70


def test_entity_filter_fuzzy_mode_adds_similarity_score_column():
    pytest.importorskip("rapidfuzz")

    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        _seed_pathways(session)
        report = _report(
            session, input_data=["apoptosis"], match_mode="fuzzy", similarity_threshold=60
        )
        df = report.run()

    assert "similarity_score" in df.columns
    matched = df[df["observation"] != "not found"]
    assert len(matched) >= 1
    assert all(matched["similarity_score"] >= 60)


def test_entity_filter_group_filter_restricts_results():
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        _seed_entities(session)   # group = Gene
        _seed_pathways(session)   # group = Pathway
        # TP53 exists in Gene, not in Pathway
        report = _report(
            session, input_data=["TP53"], match_mode="exact", group_filter="Pathway"
        )
        df = report.run()

    assert len(df) == 1
    assert df.iloc[0]["observation"] == "not found"


def test_entity_filter_requires_at_least_one_non_empty_value():
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        report = _report(session, input_data=["", " "])
        with pytest.raises(ValueError, match="at least one non-empty value"):
            report.run()
