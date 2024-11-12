import pytest
from unittest.mock import MagicMock
from biofilter_modules.mixins.filter_annot_model_mixin import FilterAnnotModelMixin  # noqa: E501


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
                "snp": ["snp_label"],
                "position": ["position_chr", "position_label", "position_pos"],
                "gene": ["gene_label"],
                "generegion": [
                    "biopolymer_chr",
                    "gene_label",
                    "biopolymer_start",
                    "biopolymer_stop",
                ],
                "upstream": ["upstream_label", "upstream_distance"],
                "downstream": ["downstream_label", "downstream_distance"],
                "region": ["region_chr", "region_label", "region_start", "region_stop"],  # noqa: E501
                "group": ["group_label"],
                "source": ["source_label"],
                "gwas": [
                    "gwas_trait",
                    "gwas_snps",
                    "gwas_orbeta",
                    "gwas_allele95ci",
                    "gwas_riskAfreq",
                    "gwas_pubmed",
                ],
                "snpinput": ["snp_label"],
                "positioninput": ["position_label"],
                "geneinput": ["gene_label"],
                "regioninput": ["region_label"],
                "groupinput": ["group_label"],
                "sourceinput": ["source_label"],
                "group_id": ["group_id"],
                "source_id": ["source_id"],
                "biopolymer_id": ["biopolymer_id"],
            }
            _inputFilters = {
                "user": {"source": False},
                "main": {"group": 0, "source": 0},
                "alt": {"group": 0, "source": 0},
                "cand": {"main_biopolymer": 0, "alt_biopolymer": 0, "group": 0},
            }
            _options = {
                "allow_duplicate_output": "no",
                "debug_query": False,
                "all_pairwise_models": "no",
                "maximum_model_count": 0,
                "minimum_model_score": 0,
                "sort_models": "no",
                "maximum_model_group_size": 0,
            }
            _options = MockOptions()  # Mock options with attributes
            _onlyGeneModels = False
            _geneModels = None

            def __init__(self):
                # Mock the _loki attribute
                self._loki = MagicMock()
                self._loki._db = MagicMock()
                self._loki._db.cursor.return_value = MagicMock()

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
                self, query, splitRowIDs=False, noRowIDs=False, sortRowIDs=False
            ):
                return "SELECT * FROM table"

            def prepareTablesForQuery(self, query):
                pass

            def generateQueryResults(self, query, allowDupes=False, query2=None):
                return iter([("result1",), ("result2",)])

            # Mock prepareTableForUpdate for testing
            def prepareTableForUpdate(self, db, table):
                # pass
                self._inputFilters[db][table] = 1

            def log(self, message):
                pass

            def warn(self, message):
                pass

        return TestClass()

    def test_populateColumnsFromTypes(self, mixin):
        types = ["snp", "gene"]
        columns = mixin._populateColumnsFromTypes(types)
        assert columns == ["snp_label", "gene_label"]

    def test_generateFilterOutput(self, mixin):
        types = ["snp", "gene"]
        result = list(mixin.generateFilterOutput(types))
        assert result == [("#snp", "gene"), ("result1",), ("result2",)]

    def test_generateAnnotationOutput(self, mixin):
        typesF = ["snp"]
        typesA = ["gene"]
        result = list(mixin.generateAnnotationOutput(typesF, typesA))
        assert result == [("#snp", "gene")]

    def test_identifyCandidateModelBiopolymers(self, mixin):
        mixin.identifyCandidateModelBiopolymers()
        assert mixin._inputFilters["cand"]["main_biopolymer"] == 1
        assert mixin._inputFilters["cand"]["alt_biopolymer"] == 1

    def test_identifyCandidateModelGroups(self, mixin):
        mixin._loki._db.cursor.return_value.execute.return_value = [(1,)]
        # Run the method to test
        mixin.identifyCandidateModelGroups()
        # Checl if _inputFilters was updated correctlyq
        assert mixin._inputFilters["cand"]["group"] == 1

    def test_getGeneModels(self, mixin):
        # Simulate a return value to avoid empty sequence in `max()`
        mixin._loki._db.cursor.return_value.execute.return_value = [(1,)]
        # Executa o método para testar
        # Run the method to test
        models = mixin.getGeneModels()
        # Check if the return value is correct
        assert models == [("result1",), ("result2",)]

    def test_generateModelOutput(self, mixin):
        mixin._loki._db.cursor.return_value.execute.return_value = [(1,)]
        mixin.getGeneModels = MagicMock(return_value=[("result1", "result2", 1, 2)])  # noqa: E501
        typesL = ["snp"]
        typesR = ["gene"]
        result = list(mixin.generateModelOutput(typesL, typesR))
        assert result == [("#snp1", "gene2", "score(src-grp)"), ("1-2",)]
