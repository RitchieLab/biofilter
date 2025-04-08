# tests/conftest.py

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from biofilter.db.base import Base
from biofilter.biofilter import Biofilter

# 👇 Força o carregamento dos modelos
import biofilter.db.models  # noqa: F401


@pytest.fixture(scope="function")
def db_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    # As tabelas só serão criadas se os modelos forem carregados
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    yield session

    session.close()
    Base.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def biofilter_instance(tmp_path):
    """
    Cria uma instância do Biofilter com banco SQLite temporário
    e base totalmente criada com dados iniciais.
    """
    db_file = tmp_path / "test_biofilter.sqlite"
    db_uri = f"sqlite:///{db_file}"

    bf = Biofilter()
    bf.create_new_project(db_uri=db_uri, overwrite=True)

    return bf
