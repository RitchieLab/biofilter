import pytest
import sqlite3
from biofilter_modules.mixins.user_knowledge_retrieval_mixin import (
    UserKnowledgeRetrievalMixin,
)


class MockLoki:
    def __init__(self):
        # Banco de dados em memória com a estrutura de `user.source`
        self._db = sqlite3.connect(":memory:")
        self._db.execute("ATTACH DATABASE ':memory:' AS user")
        self._db.execute(
            "CREATE TABLE user.source (source_id INTEGER PRIMARY KEY, source TEXT)"  # noqa: E501
        )

        # Dados iniciais de exemplo
        self._db.execute(
            "INSERT INTO user.source (source_id, source) VALUES (1, 'Test Source 1')"  # noqa: E501
        )
        self._db.execute(
            "INSERT INTO user.source (source_id, source) VALUES (2, 'Test Source 2')"  # noqa: E501
        )
        self._db.execute(
            "INSERT INTO user.source (source_id, source) VALUES (3, 'Another Source')"  # noqa: E501
        )


class TestUserKnowledgeRetrievalMixin(UserKnowledgeRetrievalMixin):
    def __init__(self):
        self._loki = MockLoki()


@pytest.fixture
def retrieval_mixin():
    return TestUserKnowledgeRetrievalMixin()


def test_get_user_source_id(retrieval_mixin):
    # Testa getUserSourceID com uma fonte conhecida
    source_id = retrieval_mixin.getUserSourceID("Test Source 1")
    assert source_id == 1

    # Testa getUserSourceID com uma fonte desconhecida
    source_id = retrieval_mixin.getUserSourceID("Nonexistent Source")
    assert source_id is None


def test_get_user_source_ids_with_sources(retrieval_mixin):
    # Testa getUserSourceIDs com uma lista de fontes
    sources = ["Test Source 1", "Test Source 2", "Nonexistent Source"]
    result = retrieval_mixin.getUserSourceIDs(sources)

    # Verifica se o resultado contém os IDs corretos e None para a fonte desconhecida  # noqa: E501
    expected_result = {
        "Test Source 1": 1,
        "Test Source 2": 2,
        "Nonexistent Source": None,
    }
    assert result == expected_result


def test_get_user_source_ids_without_sources(retrieval_mixin):
    # Testa getUserSourceIDs sem uma lista de fontes (deve retornar todos)
    result = retrieval_mixin.getUserSourceIDs()

    # Verifica se o resultado contém todos os pares source -> source_id
    expected_result = {
        "Test Source 1": 1,
        "Test Source 2": 2,
        "Another Source": 3,
    }  # noqa: E501
    assert result == expected_result
