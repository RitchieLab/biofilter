import pytest
from biofilter.db.models import SystemConfig
import datetime


def test_create_system_config(db_session):
    config = SystemConfig(
        key="max_entities",
        value="1000",
        type="integer",
        description="Maximum number of entities allowed",
        editable=True,
    )

    db_session.add(config)
    db_session.commit()

    retrieved = (
        db_session.query(SystemConfig).filter_by(key="max_entities").first()
    )  # noqa: E501
    assert retrieved is not None
    assert retrieved.value == "1000"
    assert retrieved.type == "integer"
    assert retrieved.description == "Maximum number of entities allowed"
    assert retrieved.editable is True
    assert isinstance(retrieved.created_at, datetime.datetime)
    assert isinstance(retrieved.updated_at, datetime.datetime)


def test_unique_key_constraint(db_session):
    config1 = SystemConfig(key="api_key", value="abc123", type="string")
    config2 = SystemConfig(key="api_key", value="def456", type="string")

    db_session.add(config1)
    db_session.commit()

    db_session.add(config2)
    with pytest.raises(Exception):
        db_session.commit()


def test_editable_default_true(db_session):
    config = SystemConfig(key="readonly", value="yes", type="string")
    db_session.add(config)
    db_session.commit()

    retrieved = (
        db_session.query(SystemConfig).filter_by(key="readonly").first()
    )  # noqa: E501
    assert retrieved.editable is True


def test_update_timestamp_changes(db_session):
    config = SystemConfig(key="timeout", value="30", type="integer")
    db_session.add(config)
    db_session.commit()

    old_updated = config.updated_at

    # Simulate an update
    config.value = "60"
    db_session.commit()

    assert config.updated_at > old_updated
