def test_create_entity_group(db_session):
    from biofilter.db.models import EntityGroup

    group = EntityGroup(
        name="gene_group", description="Group for gene-like entities"
    )  # noqa: E501
    db_session.add(group)
    db_session.commit()

    result = db_session.query(EntityGroup).filter_by(name="gene_group").first()
    assert result is not None
    assert result.description == "Group for gene-like entities"


def test_create_entity(db_session):
    from biofilter.db.models import Entity

    entity = Entity(group_id=1)
    db_session.add(entity)
    db_session.commit()

    result = db_session.query(Entity).first()
    assert result is not None
    assert result.group_id == 1


def test_create_entity_name(db_session):
    from biofilter.db.models import Entity, EntityName

    entity = Entity(group_id=2)
    db_session.add(entity)
    db_session.commit()

    name = EntityName(
        # entity_id=entity.id,
        entity_id=1,
        datasource_id=1,
        name="TP53",
        is_primary=True,
    )
    db_session.add(name)
    db_session.commit()

    result = db_session.query(EntityName).filter_by(name="TP53").first()
    assert result is not None
    assert result.is_primary is True


def test_create_relationship_type(db_session):
    from biofilter.db.models import RelationshipType

    rtype = RelationshipType(code="is_a", description="Subclass of")
    db_session.add(rtype)
    db_session.commit()

    result = db_session.query(RelationshipType).filter_by(code="is_a").first()
    assert result is not None
    assert result.description == "Subclass of"


def test_create_entity_relationship(db_session):
    from biofilter.db.models import (
        Entity,
        RelationshipType,
        EntityRelationship,
    )  # noqa: E501

    e1 = Entity(group_id=1)
    e2 = Entity(group_id=2)
    db_session.add_all([e1, e2])
    db_session.commit()

    rtype = RelationshipType(code="regulates")
    db_session.add(rtype)
    db_session.commit()

    rel = EntityRelationship(
        entity_1_id=e1.id, entity_2_id=e2.id, relationship_type_id=rtype.id
    )
    db_session.add(rel)
    db_session.commit()

    result = db_session.query(EntityRelationship).first()
    assert result is not None
    assert result.entity_1_id == e1.id
    assert result.relationship_type_id == rtype.id
