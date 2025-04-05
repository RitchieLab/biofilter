# import pytest
# from biofilter.biofilter import Biofilter


# @pytest.fixture
# def sqlite_uri(tmp_path):
#     # Cria um caminho real de arquivo e retorna como URI do SQLite
#     db_file = tmp_path / "biofilter.sqlite"
#     return f"sqlite:///{db_file}", db_file


# def test_create_new_db():
#     bf = Biofilter()
#     from biofilter.biofilter.db.database import Database
#     bf.db = Database()
#     bf.biofilter.db.uri = "sqlite:///tests/biofilter3R/data/biofilter.sqlite"
#     bf.biofilter.db.create_db()
#     bf.biofilter.db.connect()
#     session = bf.biofilter.db.get_session()

#     bf.update()
#     assert 2 == 2


# NOTE: Futuramente rodar com o postgres
# import pytest
# from biofilter.core.biofilter import Biofilter
# import os
# from sqlalchemy import create_engine

# @pytest.mark.parametrize("db_uri", [
#     pytest.param("sqlite:///{db_path}", id="sqlite"),
#     pytest.param("postgresql://user:password@localhost:5432/testdb", id="postgres"),
# ])
# def test_create_db_parametrized(tmp_path, db_uri):
#     db_path = tmp_path / "biofilter.sqlite"
#     uri = db_uri.format(db_path=db_path) if "sqlite" in db_uri else db_uri

#     bf = Biofilter()
#     created = bf.create_db(db_path=str(uri))
    
#     # Para SQLite, checar se arquivo foi criado
#     if uri.startswith("sqlite:///"):
#         assert (tmp_path / "biofilter.sqlite").exists()

#     # Para Postgres, pode verificar se conexão está ativa
#     elif uri.startswith("postgresql://"):
#         engine = create_engine(uri)
#         with engine.connect() as conn:
#             assert conn.closed == False
