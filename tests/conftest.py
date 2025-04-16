# tests/conftest.py

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from biofilter.db.base import Base
from biofilter.biofilter import Biofilter
from unittest.mock import MagicMock

# 👇 Force loading of models
import biofilter.db.models  # noqa: F401


@pytest.fixture(scope="function")
def db_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    # The tables will only be created if the models are loaded
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    yield session

    session.close()
    Base.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def biofilter_instance(tmp_path):
    """
    Create a Biofilter instance with a temporary SQLite database
    and a fully created base with initial data.
    """
    db_file = tmp_path / "test_biofilter.sqlite"
    db_uri = f"sqlite:///{db_file}"

    bf = Biofilter()
    bf.create_new_project(db_uri=db_uri, overwrite=True)

    return bf


@pytest.fixture(scope="function")
def mock_etl_manager(monkeypatch):
    mock_manager = MagicMock()
    monkeypatch.setattr(
        "biofilter.biofilter.ETLManager", lambda session: mock_manager
    )  # noqa: E501
    return mock_manager
