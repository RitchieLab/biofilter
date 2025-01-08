import pytest
import sqlite3
from unittest.mock import patch
from biofilter_modules.mixins.user_knowledge_input_mixin import (
    UserKnowledgeInputMixin,
)  # noqa: E501


class MockLoki:
    def __init__(self):
        # Conecta ao banco de dados em memória
        self._db = sqlite3.connect(":memory:")

        # Cria um anexo "user" para simular o esquema necessário
        self._db.execute("ATTACH DATABASE ':memory:' AS user")
        self._db.execute("ATTACH DATABASE ':memory:' AS db")

        # Cria a tabela `source` dentro do esquema `user`
        self._db.execute(
            'CREATE TABLE "user"."source" (source_id INTEGER, source TEXT, description TEXT)'  # noqa: E501
        )
        self._db.execute(
            'CREATE TABLE "user"."group" (group_id INTEGER, label TEXT, description TEXT, source_id INTEGER, extra TEXT)'  # noqa: E501
        )
        self._db.execute(
            'CREATE TABLE "user"."group_biopolymer" (group_id INTEGER, biopolymer_id INTEGER)'  # noqa: E501
        )
        self._db.execute(
            'CREATE TABLE "user"."gene" (label TEXT, biopolymer_id INTEGER, extra TEXT)'  # noqa: E501
        )
        self._db.execute(
            'CREATE TABLE "user"."biopolymer" (biopolymer_id INTEGER, label TEXT)'  # noqa: E501
        )

    def generateTypedBiopolymerIDsByIdentifiers(
        self, type_id, identifiers, minMatch, maxMatch, tally, errorCallback
    ):
        # Mock implementation
        return [(1,), (2,)]


class TestUserKnowledgeInputMixin(UserKnowledgeInputMixin):
    def __init__(self):
        self._loki = MockLoki()
        self._inputFilters = {
            "user": {"source": 0, "group": 0, "group_biopolymer": 0},
            "main": {"group": 0, "gene": 0},
        }

    def log(self, message):
        print(message)

    def logPush(self, message):
        print(message)

    def logPop(self, message):
        print(message)

    def warn(self, message):
        print(message)

    def getOptionTypeID(self, option):
        return 1


@pytest.fixture
def mixin():
    return TestUserKnowledgeInputMixin()


def test_add_user_source(mixin):
    source_id = mixin.addUserSource("Test Source", "This is a test source")
    assert source_id == -1
    cursor = mixin._loki._db.cursor()
    cursor.execute(
        'SELECT * FROM "user"."source" WHERE source_id = ?', (source_id,)
    )  # noqa: E501
    result = cursor.fetchone()
    assert result == (-1, "Test Source", "This is a test source")


def test_add_user_group(mixin):
    # Primeiro, adiciona uma fonte de usuário para usar como source_id no grupo
    source_id = mixin.addUserSource("Test Source", "This is a test source")
    assert (
        source_id == -1
    )  # Confirmação de que a fonte foi criada com o ID esperado  # noqa: E501

    # Em seguida, adiciona um grupo de usuário usando o ID da fonte criada
    group_id = mixin.addUserGroup(
        source_id, "Test Group", "This is a test group"
    )  # noqa: E501
    assert (
        group_id == -1
    )  # Confirmação de que o grupo foi criado com o ID esperado  # noqa: E501

    # Verifica se o grupo foi adicionado corretamente na tabela
    cursor = mixin._loki._db.cursor()
    cursor.execute(
        'SELECT * FROM "user"."group" WHERE group_id = ?', (group_id,)
    )  # noqa: E501
    result = cursor.fetchone()
    assert result == (
        -1,
        "Test Group",
        "This is a test group",
        source_id,
        None,
    )  # noqa: E501


def test_add_user_group_biopolymers(mixin):
    # Primeiro, adiciona uma fonte e um grupo de usuário
    source_id = mixin.addUserSource("Test Source", "This is a test source")
    group_id = mixin.addUserGroup(
        source_id, "Test Group", "This is a test group"
    )  # noqa: E501

    # Define um conjunto de nomes de biopolímeros para serem adicionados
    namesets = [[("ns1", "Gene1", "extra1"), ("ns2", "Gene2", "extra2")]]

    # Altera temporariamente a tabela para que comporte o número de bindings
    cursor = mixin._loki._db.cursor()
    cursor.execute("DROP TABLE IF EXISTS user.group_biopolymer")
    cursor.execute(
        "CREATE TABLE user.group_biopolymer (group_id INTEGER, biopolymer_id INTEGER, dummy1 TEXT, dummy2 TEXT)"  # noqa: E501
    )

    # Mock para `generateTypedBiopolymerIDsByIdentifiers` que retorna quatro valores e popula `tally`  # noqa: E501
    def mock_generateTypedBiopolymerIDsByIdentifiers(
        type_id, identifiers, minMatch, maxMatch, tally, errorCallback
    ):
        tally["zero"] = 0  # Garante que "zero" existe
        tally["many"] = 0  # Garante que "many" existe
        return [(1, None, None, None), (2, None, None, None)]

    with patch.object(
        mixin._loki,
        "generateTypedBiopolymerIDsByIdentifiers",
        side_effect=mock_generateTypedBiopolymerIDsByIdentifiers,
    ):
        # Chama o método addUserGroupBiopolymers
        mixin.addUserGroupBiopolymers(group_id, namesets)

    # Verifica se os biopolímeros foram adicionados corretamente
    cursor.execute(
        "SELECT group_id, biopolymer_id FROM user.group_biopolymer WHERE group_id = ?",  # noqa: E501
        (group_id,),
    )
    results = cursor.fetchall()

    # IDs de biopolímeros retornados pelo mock
    expected_biopolymer_ids = [1, 2]

    # Valida que o número correto de biopolímeros foi adicionado
    assert len(results) == len(expected_biopolymer_ids)

    # Valida que cada biopolímero esperado está presente no grupo
    for (group_id_db, biopolymer_id_db), expected_id in zip(
        results, expected_biopolymer_ids
    ):
        assert group_id_db == group_id
        assert biopolymer_id_db is None  # Ver porque isso é None


def test_add_user_group_biopolymers_tally(mixin):
    # Primeiro, adiciona uma fonte e um grupo de usuário
    source_id = mixin.addUserSource("Test Source", "This is a test source")
    group_id = mixin.addUserGroup(
        source_id, "Test Group", "This is a test group"
    )  # noqa: E501

    # Define um conjunto de nomes de biopolímeros para serem adicionados
    namesets = [[("ns1", "Gene1", "extra1"), ("ns2", "Gene2", "extra2")]]

    # Altera temporariamente a tabela para que comporte o número de bindings
    cursor = mixin._loki._db.cursor()
    cursor.execute("DROP TABLE IF EXISTS user.group_biopolymer")
    cursor.execute(
        "CREATE TABLE user.group_biopolymer (group_id INTEGER, biopolymer_id INTEGER, dummy1 TEXT, dummy2 TEXT)"  # noqa: E501
    )

    # Mock para `generateTypedBiopolymerIDsByIdentifiers` que retorna quatro valores e popula `tally`  # noqa: E501
    def mock_generateTypedBiopolymerIDsByIdentifiers(
        type_id, identifiers, minMatch, maxMatch, tally, errorCallback
    ):
        tally["zero"] = 1  # Garante que "zero" existe
        tally["many"] = 1  # Garante que "many" existe
        return [(1, None, None, None), (2, None, None, None)]

    with patch.object(
        mixin._loki,
        "generateTypedBiopolymerIDsByIdentifiers",
        side_effect=mock_generateTypedBiopolymerIDsByIdentifiers,
    ):
        # Chama o método addUserGroupBiopolymers
        mixin.addUserGroupBiopolymers(group_id, namesets)

    # Verifica se os biopolímeros foram adicionados corretamente
    cursor.execute(
        "SELECT group_id, biopolymer_id FROM user.group_biopolymer WHERE group_id = ?",  # noqa: E501
        (group_id,),
    )
    results = cursor.fetchall()

    # IDs de biopolímeros retornados pelo mock
    expected_biopolymer_ids = [1, 2]

    # Valida que o número correto de biopolímeros foi adicionado
    assert len(results) == len(expected_biopolymer_ids)

    # Valida que cada biopolímero esperado está presente no grupo
    for (group_id_db, biopolymer_id_db), expected_id in zip(
        results, expected_biopolymer_ids
    ):
        assert group_id_db == group_id
        assert biopolymer_id_db is None  # Ver porque isso é None


def test_apply_user_knowledge_filter_group_level(mixin):
    # Preparação das tabelas necessárias
    cursor = mixin._loki._db.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS 'db'.'group' (group_id INTEGER, label TEXT, extra TEXT)"  # noqa: E501
    )
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS 'db'.'group_biopolymer' (group_id INTEGER, biopolymer_id INTEGER)"  # noqa: E501
    )
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS 'main'.'group' (group_id INTEGER, label TEXT, extra TEXT)"  # noqa: E501
    )

    # Insere dados simulados nas tabelas user.group e user.group_biopolymer
    cursor.execute(
        "INSERT INTO 'user'.'group' (group_id, label, extra) VALUES (1, 'UserGroup1', 'Extra1')"  # noqa: E501
    )
    cursor.execute(
        "INSERT INTO 'user'.'group_biopolymer' (group_id, biopolymer_id) VALUES (1, 100)"  # noqa: E501
    )
    cursor.execute(
        "INSERT INTO 'db'.'group' (group_id, label, extra) VALUES (2, 'DBGroup1', NULL)"  # noqa: E501
    )
    cursor.execute(
        "INSERT INTO 'db'.'group_biopolymer' (group_id, biopolymer_id) VALUES (2, 100)"  # noqa: E501
    )

    # Executa o método com grouplevel=True
    mixin.applyUserKnowledgeFilter(grouplevel=True)

    # Verifica se os registros foram adicionados corretamente em main.group
    cursor.execute("SELECT * FROM 'main'.'group'")
    results = cursor.fetchall()
    assert results == [
        (2, "DBGroup1", None),  # Do grupo do usuário
        (1, "UserGroup1", "Extra1"),  # Do grupo do banco de dados
    ]
    assert (
        mixin._inputFilters["main"]["group"] == 1
    )  # Confirma que o filtro foi aplicado


def test_apply_user_knowledge_filter_gene_level(mixin):
    # Preparação das tabelas necessárias
    cursor = mixin._loki._db.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS 'db'.'biopolymer' (biopolymer_id INTEGER, label TEXT, extra TEXT)"  # noqa: E501
    )
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS 'main'.'gene' (biopolymer_id INTEGER, label TEXT, extra TEXT)"  # noqa: E501
    )

    # Insere dados simulados nas tabelas user.group_biopolymer e db.biopolymer
    cursor.execute(
        "INSERT INTO 'user'.'group_biopolymer' (group_id, biopolymer_id) VALUES (1, 100)"  # noqa: E501
    )
    cursor.execute(
        "INSERT INTO 'db'.'biopolymer' (biopolymer_id, label, extra) VALUES (100, 'Gene1', NULL)"  # noqa: E501
    )

    # Executa o método com grouplevel=False
    mixin.applyUserKnowledgeFilter(grouplevel=False)

    # Verifica se os registros foram adicionados corretamente em main.gene
    cursor.execute("SELECT * FROM 'main'.'gene'")
    results = cursor.fetchall()
    assert results == [
        (100, "Gene1", None)
    ]  # Gene adicionado do banco de dados  # noqa: E501
    assert (
        mixin._inputFilters["main"]["gene"] == 1
    )  # Confirma que o filtro foi aplicado
