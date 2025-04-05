import os
from pathlib import Path
import pytest
from sqlalchemy import inspect
# from biofilter.biofilter.db.database import Database
from biofilter.biofilter import Biofilter


@pytest.fixture
def temp_db_path(tmp_path):
    """Fixture que retorna o caminho para uma base SQLite temporária."""
    # return tmp_path / "test_biofilter.sqlite"
    return "sqlite:///tests/biofilter3R/data/new_biofilter.sqlite"


def test_create_db(temp_db_path):
    uri = temp_db_path
    bf = Biofilter()
    created = bf.create_db(uri, overwrite=True)
    assert created is True


    # # Criação da base
    # # created = biofilter.db.create_db(overwrite=True)
    # assert created is True

    # # Verifica se o arquivo foi realmente criado
    # assert temp_db_path.exists()

    # # Verifica se há conexão válida
    # biofilter.db.connect()
    # assert biofilter.db.engine is not None
    # assert biofilter.db.session is not None

    # # Verifica se algumas tabelas esperadas foram criadas
    # inspector = inspect(biofilter.db.engine)
    # tables = inspector.get_table_names()
    # assert "data_sources" in tables
    # assert "system_config" in tables
