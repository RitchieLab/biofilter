from __future__ import annotations

from types import SimpleNamespace

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
from biofilter.modules.report.reports.report_entity_relationship_model import (
    EntityRelationshipModelReport,
)


class DummyLogger:
    def log(self, message, level="INFO"):
        pass


def _report(session, **kwargs):
    return EntityRelationshipModelReport(
        session=session,
        db=SimpleNamespace(engine=getattr(session, "bind", None)),
        logger=DummyLogger(),
        **kwargs,
    )


def _seed(session):
    gene = EntityGroup(name="Gene")
    pathway = EntityGroup(name="Pathway")
    protein = EntityGroup(name="Protein")
    session.add_all([gene, pathway, protein])
    session.flush()

    tp53 = Entity(group_id=gene.id, is_active=True)
    brca1 = Entity(group_id=gene.id, is_active=True)
    dna_path = Entity(group_id=pathway.id, is_active=True)
    mdm2 = Entity(group_id=protein.id, is_active=True)
    session.add_all([tp53, brca1, dna_path, mdm2])
    session.flush()

    session.add_all(
        [
            EntityAlias(
                entity_id=tp53.id,
                group_id=gene.id,
                alias_value="TP53",
                alias_norm="tp53",
                alias_type="preferred",
                is_primary=True,
            ),
            EntityAlias(
                entity_id=brca1.id,
                group_id=gene.id,
                alias_value="BRCA1",
                alias_norm="brca1",
                alias_type="preferred",
                is_primary=True,
            ),
            EntityAlias(
                entity_id=dna_path.id,
                group_id=pathway.id,
                alias_value="DNA_REPAIR_PATHWAY",
                alias_norm="dna_repair_pathway",
                alias_type="preferred",
                is_primary=True,
            ),
            EntityAlias(
                entity_id=mdm2.id,
                group_id=protein.id,
                alias_value="MDM2",
                alias_norm="mdm2",
                alias_type="preferred",
                is_primary=True,
            ),
        ]
    )

    rt_interacts = EntityRelationshipType(
        code="interacts_with", description="interacts with"
    )
    rt_involved = EntityRelationshipType(code="involved_in", description="involved in")
    rt_binds = EntityRelationshipType(code="binds_to", description="binds to")
    session.add_all([rt_interacts, rt_involved, rt_binds])
    session.flush()

    session.add_all(
        [
            EntityRelationship(
                entity_1_id=tp53.id,
                entity_1_group_id=gene.id,
                entity_2_id=brca1.id,
                entity_2_group_id=gene.id,
                relationship_type_id=rt_interacts.id,
            ),
            EntityRelationship(
                entity_1_id=tp53.id,
                entity_1_group_id=gene.id,
                entity_2_id=dna_path.id,
                entity_2_group_id=pathway.id,
                relationship_type_id=rt_involved.id,
            ),
            EntityRelationship(
                entity_1_id=brca1.id,
                entity_1_group_id=gene.id,
                entity_2_id=dna_path.id,
                entity_2_group_id=pathway.id,
                relationship_type_id=rt_involved.id,
            ),
            EntityRelationship(
                entity_1_id=mdm2.id,
                entity_1_group_id=protein.id,
                entity_2_id=tp53.id,
                entity_2_group_id=gene.id,
                relationship_type_id=rt_binds.id,
            ),
        ]
    )
    session.commit()


def _make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    return Session()


def test_between_inputs_returns_only_relationships_inside_input_set():
    with _make_session() as session:
        _seed(session)
        report = _report(
            session,
            input_data=["TP53", "BRCA1"],
            relationship_scope="between_inputs",
        )
        df = report.run()

    rows = df[df["observation"] != "not found"]
    assert len(rows) == 1
    row = rows.iloc[0]
    assert row["input_primary_name"] in {"TP53", "BRCA1"}
    assert row["related_primary_name"] in {"TP53", "BRCA1"}
    assert row["relationship_type"] == "interacts_with"


def test_input_to_any_with_output_group_filter_keeps_only_requested_groups():
    with _make_session() as session:
        _seed(session)
        report = _report(
            session,
            input_data=["TP53", "BRCA1"],
            output_entity_groups=["Pathway"],
            relationship_scope="input_to_any",
        )
        df = report.run()

    rows = df[df["observation"] != "not found"]
    assert len(rows) == 2
    assert set(rows["input_primary_name"].tolist()) == {"TP53", "BRCA1"}
    assert set(rows["related_group_name"].tolist()) == {"Pathway"}
    assert set(rows["related_primary_name"].tolist()) == {"DNA_REPAIR_PATHWAY"}


def test_input_found_on_entity_2_side_is_returned():
    with _make_session() as session:
        _seed(session)
        report = _report(
            session,
            input_data=["TP53"],
            output_entity_groups=["Protein"],
            relationship_scope="input_to_any",
        )
        df = report.run()

    rows = df[df["observation"] != "not found"]
    assert len(rows) == 1
    row = rows.iloc[0]
    assert row["input_primary_name"] == "TP53"
    assert row["match_side"] == "entity_2"
    assert row["direction"] == "related->input"
    assert row["related_primary_name"] == "MDM2"


def test_input_entity_group_filter_limits_resolution_and_marks_not_found():
    with _make_session() as session:
        _seed(session)
        report = _report(
            session,
            input_data=["TP53", "DNA_REPAIR_PATHWAY"],
            input_entity_groups=["Gene"],
            relationship_scope="input_to_any",
        )
        df = report.run()

    not_found = df[df["observation"] == "not found"]
    assert len(not_found) == 1
    assert not_found.iloc[0]["input_original"] == "DNA_REPAIR_PATHWAY"
