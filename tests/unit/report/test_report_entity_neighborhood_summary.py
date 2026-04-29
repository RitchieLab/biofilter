from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from biofilter.modules.db.base import Base
from biofilter.modules.db.models import (
    Entity,
    EntityAlias,
    EntityGroup,
    EntityRelationship,
    EntityRelationshipType,
)
from biofilter.modules.report.reports.report_entity_neighborhood_summary import (  # noqa: E501
    EntityNeighborhoodSummaryReport,
)


class DummyLogger:
    def log(self, message, level="INFO"):
        pass


def _make_engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return engine


def _report(session, **kwargs):
    return EntityNeighborhoodSummaryReport(
        session=session,
        db=SimpleNamespace(engine=getattr(session, "bind", None)),
        logger=DummyLogger(),
        **kwargs,
    )


def _seed_basic_graph(session):
    """
    Seed: BRCA1 (gene) — connected to TP53 (gene), 'DNA repair' (pathway),
    and 'Breast cancer' (disease). Provides a non-trivial 1-hop neighborhood.
    """
    g_genes = EntityGroup(name="Genes")
    g_path = EntityGroup(name="Pathways")
    g_dis = EntityGroup(name="Diseases")
    session.add_all([g_genes, g_path, g_dis])
    session.flush()

    rt = EntityRelationshipType(code="interacts_with")
    session.add(rt)
    session.flush()

    brca1 = Entity(group_id=g_genes.id, has_conflict=False, is_active=True)
    tp53 = Entity(group_id=g_genes.id, has_conflict=False, is_active=True)
    pathway = Entity(group_id=g_path.id, has_conflict=False, is_active=True)
    disease = Entity(group_id=g_dis.id, has_conflict=False, is_active=True)
    session.add_all([brca1, tp53, pathway, disease])
    session.flush()

    session.add_all([
        EntityAlias(
            entity_id=brca1.id, group_id=g_genes.id,
            alias_value="BRCA1", alias_norm="brca1",
            alias_type="symbol", is_primary=True,
        ),
        EntityAlias(
            entity_id=brca1.id, group_id=g_genes.id,
            alias_value="BRCC1", alias_norm="brcc1",
            alias_type="prev_symbol", is_primary=False,
        ),
        EntityAlias(
            entity_id=tp53.id, group_id=g_genes.id,
            alias_value="TP53", alias_norm="tp53",
            alias_type="symbol", is_primary=True,
        ),
        EntityAlias(
            entity_id=pathway.id, group_id=g_path.id,
            alias_value="DNA repair", alias_norm="dna repair",
            alias_type="label", is_primary=True,
        ),
        EntityAlias(
            entity_id=disease.id, group_id=g_dis.id,
            alias_value="Breast cancer", alias_norm="breast cancer",
            alias_type="label", is_primary=True,
        ),
    ])

    session.add_all([
        EntityRelationship(
            entity_1_id=brca1.id, entity_2_id=tp53.id,
            entity_1_group_id=g_genes.id, entity_2_group_id=g_genes.id,
            relationship_type_id=rt.id,
        ),
        EntityRelationship(
            entity_1_id=brca1.id, entity_2_id=pathway.id,
            entity_1_group_id=g_genes.id, entity_2_group_id=g_path.id,
            relationship_type_id=rt.id,
        ),
        EntityRelationship(
            entity_1_id=brca1.id, entity_2_id=disease.id,
            entity_1_group_id=g_genes.id, entity_2_group_id=g_dis.id,
            relationship_type_id=rt.id,
        ),
    ])
    session.commit()
    return {
        "brca1": brca1.id,
        "tp53": tp53.id,
        "pathway": pathway.id,
        "disease": disease.id,
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_resolves_exact_and_aggregates_neighborhood():
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        ids = _seed_basic_graph(session)
        report = _report(
            session,
            items=["gene:BRCA1"],
            match_mode="exact",
        )
        df = report.run()

    assert len(df) == 1
    row = df.iloc[0]
    assert row["Entity ID"] == ids["brca1"]
    assert row["Entity Type"] == "gene"
    assert row["Matched Name"] == "BRCA1"
    assert bool(row["Exact Match"]) is True
    assert row["Primary Alias"] == "BRCA1"
    assert row["Resolve Status"] == "resolved"
    assert row["Resolve Method"] == "exact"
    assert row["Degree Total (1-hop)"] == 3

    deg_by_type = json.loads(row["Degree By Type (1-hop)"])
    assert deg_by_type == {"Genes": 1, "Pathways": 1, "Diseases": 1}

    assert json.loads(row["Genes"]) == ["TP53"]
    assert json.loads(row["Pathways"]) == ["DNA repair"]
    assert json.loads(row["Diseases"]) == ["Breast cancer"]


def test_exact_match_flag_distinguishes_substring_from_full_match():
    """Exact Match should be True only when input == matched_alias
    (case-insensitive). Substring (like) matches must be False."""
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        _seed_basic_graph(session)
        report = _report(
            session,
            items=["pathway:repair", "pathway:DNA repair"],
            match_mode="like",
        )
        df = report.run()

    assert len(df) == 2
    by_input = {r["Input Word"]: r for _, r in df.iterrows()}

    # Substring match — not exact
    assert by_input["repair"]["Matched Name"] == "DNA repair"
    assert bool(by_input["repair"]["Exact Match"]) is False

    # Full match (case-insensitive) — exact
    assert by_input["DNA repair"]["Matched Name"] == "DNA repair"
    assert bool(by_input["DNA repair"]["Exact Match"]) is True


def test_matched_alias_uses_prev_symbol_when_input_matches_legacy():
    """When the input matches a legacy alias (prev_symbol), Matched Alias
    must show the legacy value, while Primary Alias keeps the canonical."""
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        _seed_basic_graph(session)
        report = _report(
            session, items=["gene:BRCC1"], match_mode="exact",
        )
        df = report.run()

    assert len(df) == 1
    row = df.iloc[0]
    assert row["Matched Name"] == "BRCC1"
    assert row["Primary Alias"] == "BRCA1"
    assert bool(row["Exact Match"]) is True


def test_emits_not_found_rows_when_requested():
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        _seed_basic_graph(session)
        report = _report(
            session,
            items=["gene:BRCA1", "ghost_term"],
            match_mode="exact",
            emit_not_found_rows=True,
        )
        df = report.run()

    assert len(df) == 2
    not_found = df[df["Resolve Status"] == "not_found"].iloc[0]
    assert not_found["Input Word"] == "ghost_term"
    eid = not_found["Entity ID"]
    assert eid is None or eid != eid  # NaN check (NaN != NaN)
    assert not_found["Degree Total (1-hop)"] == 0


def test_skips_not_found_rows_by_default():
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        _seed_basic_graph(session)
        report = _report(
            session,
            items=["gene:BRCA1", "ghost_term"],
            match_mode="exact",
        )
        df = report.run()

    assert len(df) == 1
    assert df.iloc[0]["Input Word"] == "BRCA1"


def test_type_hint_scopes_resolution_to_group():
    """A gene symbol that also exists as a synonym in another group must
    not bleed across when type_hint pins the search to a single group."""
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        ids = _seed_basic_graph(session)

        # Add BRCA1 as a synonym of the disease entity (cross-group collision)
        g_dis = session.query(EntityGroup).filter_by(name="Diseases").one()
        session.add(
            EntityAlias(
                entity_id=ids["disease"], group_id=g_dis.id,
                alias_value="BRCA1", alias_norm="brca1",
                alias_type="synonym", is_primary=False,
            )
        )
        session.commit()

        # gene:BRCA1 must only resolve to the gene entity
        report = _report(
            session, items=["gene:BRCA1"], match_mode="exact",
        )
        df = report.run()

    assert len(df) == 1
    assert df.iloc[0]["Entity ID"] == ids["brca1"]
    assert df.iloc[0]["Entity Type"] == "gene"


def test_like_mode_finds_substring_matches():
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        _seed_basic_graph(session)
        report = _report(
            session, items=["pathway:repair"], match_mode="like",
        )
        df = report.run()

    assert len(df) == 1
    assert df.iloc[0]["Primary Alias"] == "DNA repair"
    assert df.iloc[0]["Resolve Method"] == "like"


def test_like_mode_collapses_multiple_aliases_per_entity():
    """Multiple aliases of the same entity should yield a single row."""
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        ids = _seed_basic_graph(session)

        # Add several "alzheimer-like" aliases to the disease entity
        g_dis = session.query(EntityGroup).filter_by(name="Diseases").one()
        session.add_all([
            EntityAlias(
                entity_id=ids["disease"], group_id=g_dis.id,
                alias_value="Alzheimer disease",
                alias_norm="alzheimer disease",
                alias_type="label", is_primary=False,
            ),
            EntityAlias(
                entity_id=ids["disease"], group_id=g_dis.id,
                alias_value="Alzheimer dementia",
                alias_norm="alzheimer dementia",
                alias_type="synonym", is_primary=False,
            ),
            EntityAlias(
                entity_id=ids["disease"], group_id=g_dis.id,
                alias_value="Alzheimer's disease",
                alias_norm="alzheimer's disease",
                alias_type="synonym", is_primary=False,
            ),
        ])
        session.commit()

        report = _report(
            session, items=["disease:alzheimer"], match_mode="like",
        )
        df = report.run()

    # All 3 added aliases match, but the same entity should appear once
    assert len(df) == 1
    assert df.iloc[0]["Entity ID"] == ids["disease"]


def test_fuzzy_mode_resolves_with_score():
    pytest.importorskip("rapidfuzz")
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        _seed_basic_graph(session)
        report = _report(
            session,
            items=["pathway:DNA repare"],  # typo
            match_mode="fuzzy",
            similarity_threshold=70,
        )
        df = report.run()

    assert len(df) == 1
    assert df.iloc[0]["Primary Alias"] == "DNA repair"
    assert df.iloc[0]["Resolve Method"] == "fuzzy"
    assert df.iloc[0]["Resolve Score"] >= 70


def test_dynamic_neighbor_columns_include_all_groups():
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        _seed_basic_graph(session)
        report = _report(
            session, items=["gene:BRCA1"], match_mode="exact",
        )
        df = report.run()

    # Each EntityGroup in the DB should be a column on the output
    for group_name in ("Genes", "Pathways", "Diseases"):
        assert group_name in df.columns


def test_aliases_top_n_truncates_alias_list():
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        _seed_basic_graph(session)
        report = _report(
            session, items=["gene:BRCA1"], match_mode="exact",
            aliases_top_n=1,
        )
        df = report.run()

    aliases = json.loads(df.iloc[0]["Aliases Top"])
    assert len(aliases) == 1
    # Alias Count is the FULL count, not the truncated one
    assert df.iloc[0]["Alias Count"] == 2


def test_invalid_match_mode_raises():
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        report = _report(
            session, items=["gene:BRCA1"], match_mode="invalid",
        )
        with pytest.raises(ValueError, match="match_mode"):
            report.run()


def test_requires_items_param():
    engine = _make_engine()
    Session = sessionmaker(bind=engine, future=True)

    with Session() as session:
        report = _report(session)
        with pytest.raises(ValueError, match="items"):
            report.run()
