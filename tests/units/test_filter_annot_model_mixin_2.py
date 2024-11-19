import pytest
from unittest.mock import MagicMock
from biofilter_modules.mixins.filter_annot_model_mixin import FilterAnnotModelMixin


class TestFilterAnnotModelMixin:
    @pytest.fixture
    def mixin(self):
        class MockOptions:
            def __init__(self):
                self.allow_duplicate_output = "no"
                self.debug_query = False
                self.all_pairwise_models = "no"
                self.maximum_model_count = 0
                self.minimum_model_score = 0
                self.sort_models = "no"
                self.maximum_model_group_size = 0

        class TestClass(FilterAnnotModelMixin):
            _queryColumnSources = {
                "group_id": [("table1", "col1", "expression")],
                "gene_id": [("table2", "col2", "expression")],
                "biopolymer_id": [("table3", "col3", "expression")],
            }
            _inputFilters = {
                "user": {"source": False},
                "main": {"group": 0, "source": 0},
                "alt": {"group": 0, "source": 0},
                "cand": {
                    "main_biopolymer": 0,
                    "alt_biopolymer": 0,
                    "group": 0,
                },  # noqa: E501
            }
            _options = MockOptions()  # Mock options with attributes
            _onlyGeneModels = False
            _geneModels = None

            def __init__(self):
                self._loki = MagicMock()
                self._loki._db = MagicMock()
                # Define o cursor com um valor padrão para evitar a sequência vazia
                cursor_mock = MagicMock()
                cursor_mock.execute.return_value = [(1,)]
                cursor_mock.executemany.return_value = None
                self._loki._db.cursor.return_value = cursor_mock
                self._cursor = cursor_mock  # Guardar o mock para asserções

            def buildQuery(
                self,
                mode,
                focus,
                select,
                applyOffset=False,
                userKnowledge=False,
                where=None,
                having=None,
            ):
                return {
                    "SELECT": {
                        "biopolymer_id_L": "biopolymer_id_L",
                        "biopolymer_id_R": "biopolymer_id_R",
                        "source_id": "source_id",
                        "group_id": "group_id",
                    },
                    "_columns": select,
                    "_rowid": {"alias": ["rowid"]},
                    "FROM": ["table"],
                    "GROUP BY": [],
                    "HAVING": set(),
                    "ORDER BY": [],
                    "LIMIT": None,
                }

            def getQueryText(
                self,
                query,
                splitRowIDs=False,
                noRowIDs=False,
                sortRowIDs=False,  # noqa: E501
            ):
                return "SELECT * FROM table"

            def prepareTablesForQuery(self, query):
                pass

            def generateQueryResults(
                self, query, allowDupes=False, query2=None
            ):  # noqa: E501
                return iter([(1,), (2,)])

            def prepareTableForUpdate(self, db, table):
                self._inputFilters[db][table] = 1

            def log(self, message):
                pass

            def warn(self, message):
                pass

        return TestClass()

    # -- Tests to identifyCandidateModelGroups method --

    def test_identifyCandidateModelGroups(self, mixin):
        mixin._loki._db.cursor.return_value.execute.return_value = [(1,)]
        # Run the method to test
        mixin.identifyCandidateModelGroups()
        # Checl if _inputFilters was updated correctlyq
        assert mixin._inputFilters["cand"]["group"] == 1

    def test_identify_candidate_model_groups_main_filter_active(self, mixin):
        mixin._inputFilters["main"]["group"] = 1
        mixin._inputFilters["cand"]["group"] = 0
        mixin.identifyCandidateModelGroups()
        assert mixin._inputFilters["cand"]["group"] == 1

    def test_identify_candidate_model_groups_alt_filter_active(self, mixin):
        mixin._inputFilters["alt"]["group"] = 1
        mixin._inputFilters["cand"]["group"] = 0
        mixin.identifyCandidateModelGroups()
        assert mixin._inputFilters["cand"]["group"] == 1

    def test_identify_candidate_model_groups_existing_candidates(self, mixin):
        mixin._inputFilters["cand"]["group"] = 1
        mixin.identifyCandidateModelGroups()
        assert mixin._inputFilters["cand"]["group"] == 1

    def test_identify_candidate_model_groups_maximum_model_group_size(self, mixin):
        mixin._options.maximum_model_group_size = 5
        mixin.identifyCandidateModelGroups()
        assert mixin._inputFilters["cand"]["group"] == 1

    def test_identify_candidate_model_groups_only_gene_models(self, mixin):
        mixin._onlyGeneModels = True
        mixin.identifyCandidateModelGroups()
        assert mixin._inputFilters["cand"]["group"] == 1

    def test_identify_candidate_model_groups_no_main_or_alt_filters(self, mixin):
        mixin._inputFilters["main"]["group"] = 0
        mixin._inputFilters["alt"]["group"] = 0
        mixin._inputFilters["cand"]["group"] = 0
        mixin.identifyCandidateModelGroups()
        assert mixin._inputFilters["cand"]["group"] == 1

    # def test_identify_candidate_model_groups_main_filter_else_insert(self, mixin):
    #     # Configura _inputFilters para simular o caminho para o primeiro else com INSERT
    #     mixin._inputFilters["main"]["group"] = 1
    #     mixin._inputFilters["cand"]["group"] = 0  # Força o else
    #     mixin.identifyCandidateModelGroups()

    #     # Verifica se o SQL de inserção foi chamado
    #     mixin._cursor.executemany.assert_any_call(
    #         "INSERT OR IGNORE INTO `cand`.`group` (group_id, flag) VALUES (?,0)",
    #         list(mixin.generateQueryResults(mixin.buildQuery("modelgroup", "main", ["group_id"]), allowDupes=True)),
    #     )

    # def test_identify_candidate_model_groups_alt_filter_else_insert(self, mixin):
    #     # Configura _inputFilters para simular o caminho para o segundo else com INSERT
    #     mixin._inputFilters["alt"]["group"] = 1
    #     mixin._inputFilters["cand"]["group"] = 0  # Força o else
    #     mixin.identifyCandidateModelGroups()

    #     # Verifica se o SQL de inserção foi chamado
    #     mixin._cursor.executemany.assert_any_call(
    #         "INSERT OR IGNORE INTO `cand`.`group` (group_id, flag) VALUES (?,0)",
    #         list(mixin.generateQueryResults(mixin.buildQuery("modelgroup", "alt", ["group_id"]), allowDupes=True)),
    #     )
