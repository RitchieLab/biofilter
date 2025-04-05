# # tests/conftest.py
# import sys
# import os

# sys.path.insert(
#     0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../biofilter"))
# )
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from biofilter.db.base import Base  # sua declarative base
from sqlalchemy.pool import StaticPool


@pytest.fixture(scope="function")
def db_session():
    # SQLite em memória
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    yield session

    session.close()
    Base.metadata.drop_all(engine)
