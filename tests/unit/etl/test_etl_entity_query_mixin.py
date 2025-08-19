import pytest
from biofilter.etl.mixins.entity_query_mixin import EntityQueryMixin
from db.models.model_entities import EntityName
from unittest.mock import MagicMock, patch
from sqlalchemy.exc import IntegrityError


class DummyEntityQuery(EntityQueryMixin):
    def __init__(self, session, logger):
        self.session = session
        self.logger = logger


@pytest.fixture
def entity_query(db_session):
    logger = MagicMock()
    return DummyEntityQuery(db_session, logger)


def test_create_new_entity(entity_query):
    entity_id, created = entity_query.get_or_create_entity("TP53", group_id=1)
    assert created is True
    assert isinstance(entity_id, int)


def test_get_existing_entity(entity_query):
    entity_id_1, created_1 = entity_query.get_or_create_entity(
        "BRCA1", group_id=1
    )  # noqa: E501
    entity_id_2, created_2 = entity_query.get_or_create_entity(
        "BRCA1", group_id=1
    )  # noqa: E501

    assert created_1 is True
    assert created_2 is False
    assert entity_id_1 == entity_id_2


def test_add_alias_to_entity(entity_query):
    entity_id, _ = entity_query.get_or_create_entity("EGFR", group_id=1)
    result = entity_query.get_or_create_entity_name(entity_id, "ERBB1")
    assert result is True


def test_prevent_duplicate_alias(entity_query):
    entity_id, _ = entity_query.get_or_create_entity("ALK", group_id=1)
    entity_query.get_or_create_entity_name(entity_id, "CD246")
    result = entity_query.get_or_create_entity_name(entity_id, "CD246")
    assert result is False


def test_entity_name_is_primary(entity_query, db_session):
    entity_id, _ = entity_query.get_or_create_entity("MYC", group_id=2)
    entity_name = db_session.query(EntityName).filter_by(name="MYC").first()
    assert entity_name.is_primary is True


def test_entity_creation_integrity_error(entity_query):
    # Create a valid instance initially
    name = "FAKE_GENE"
    group_id = 1

    # Jump the commit simulating an error on the second attempt
    with patch.object(
        entity_query.session,
        "commit",
        side_effect=IntegrityError("Mocked", None, None),  # noqa: E501
    ):  # noqa: E501
        entity_id, created = entity_query.get_or_create_entity(name, group_id)
        assert created is False
        assert entity_id is None
        entity_query.logger.log.assert_called_with(
            f"⚠️ Entity creation failed for: {name}", "WARNING"
        )
