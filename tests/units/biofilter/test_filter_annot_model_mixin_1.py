import pytest
from unittest.mock import MagicMock, patch
from biofilter_modules.mixins.filter_annot_model_mixin import (
    FilterAnnotModelMixin,
)  # noqa: E501


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
                "region": [
                    "region_chr",
                    "region_label",
                    "region_start",
                    "region_stop",
                ],  # noqa: E501
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
                "cand": {
                    "main_biopolymer": 0,
                    "alt_biopolymer": 0,
                    "group": 0,
                },  # noqa: E501
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

    # -- Tests to _populateColumnsFromTypes method --
    def test_populateColumnsFromTypes(self, mixin):
        types = [
            "snp",
            "position",
            "gene",
            "generegion",
            "upstream",
            "downstream",
            "region",
            "group",
            "source",
            "gwas",
            "snpinput",
            "positioninput",
            "geneinput",
            "regioninput",
            "groupinput",
            "sourceinput",
        ]  # noqa: E501
        columns = mixin._populateColumnsFromTypes(types)
        assert columns == [
            "snp_label",
            "position_chr",
            "position_label",
            "position_pos",
            "gene_label",
            "biopolymer_chr",
            "gene_label",
            "biopolymer_start",
            "biopolymer_stop",
            "upstream_label",
            "upstream_distance",
            "downstream_label",
            "downstream_distance",
            "region_chr",
            "region_label",
            "region_start",
            "region_stop",
            "group_label",
            "source_label",
            "gwas_trait",
            "gwas_snps",
            "gwas_orbeta",
            "gwas_allele95ci",
            "gwas_riskAfreq",
            "gwas_pubmed",
            "snp_label",
            "position_label",
            "gene_label",
            "region_label",
            "group_label",
            "source_label",
        ]

    def test_populateColumnsFromTypes_queryColumnSource(self, mixin):
        types = [
            "biopolymer_id",
        ]
        columns = mixin._populateColumnsFromTypes(types)
        assert columns == ["biopolymer_id"]

    def test_populateColumnsFromTypes_exception(self, mixin):
        types = [
            "dummy",
        ]
        with pytest.raises(
            Exception, match="ERROR: unsupported output type 'dummy'"
        ):  # noqa: E501
            mixin._populateColumnsFromTypes(types)

    # -------------------------------------------------------------------------

    # -- Tests to generateFilterOutput method --

    def test_generateFilterOutput(self, mixin):
        types = ["snp", "gene"]
        result = list(mixin.generateFilterOutput(types))
        assert result == [("#snp", "gene"), ("result1",), ("result2",)]

    def test_generateFilterOutput_with_user_source(self, mixin):
        # Define `self._inputFilters["user"]["source"]` as `True` to
        # run the query2
        mixin._inputFilters["user"]["source"] = True
        # Call the method to be tested
        types = ["snp", "gene"]
        result = list(mixin.generateFilterOutput(types))
        # Check if the result contains the expected values
        assert result == [("#snp", "gene"), ("result1",), ("result2",)]

    # -------------------------------------------------------------------------

    # -- Tests to generateAnnotationOutput method --

    def test_generateAnnotationOutput(self, mixin):
        typesF = ["snp"]
        typesA = ["gene"]
        result = list(mixin.generateAnnotationOutput(typesF, typesA))
        assert result == [("#snp", "gene")]

    def test_generateAnnotationOutput_with_debug_query(self, mixin):
        # Configures `debug_query` to `True` to trigger the debug block
        mixin._options.debug_query = True

        # Mock for `warn` to capture the warning messages
        with patch.object(mixin, "warn") as mock_warn:
            # Mock for `cursor` and `execute`
            mock_cursor = MagicMock()
            mock_cursor.execute.return_value = [
                ("plan_row_1",),
                ("plan_row_2",),
            ]  # noqa: E501

            # Mock for `generateQueryResults` to avoid real execution
            with patch.object(
                mixin,
                "generateQueryResults",
                return_value=iter([("result1",), ("result2",)]),
            ), patch.object(
                mixin._loki._db, "cursor", return_value=mock_cursor
            ):  # noqa: E501

                # Run the method with simulated `typesF` and `typesA`
                typesF = ["snp"]
                typesA = ["gene"]
                list(
                    mixin.generateAnnotationOutput(typesF, typesA)
                )  # Coleta todos os resultados

                # Check if `warn` was called with the expected debug messages
                mock_warn.assert_any_call(
                    "========== annotation : filter step ==========\n"
                )
                mock_warn.assert_any_call(
                    "========== annotation : annotate step ==========\n"
                )

                # Check if `cursorF.execute` was called for the query plans
                sqlF = mixin.getQueryText(
                    mixin.buildQuery(
                        mode="filter",
                        focus="main",
                        select=[],
                        applyOffset=False,
                    ),
                    splitRowIDs=True,
                )

                sqlA = mixin.getQueryText(
                    mixin.buildQuery(
                        mode="filter",
                        focus="main",
                        select=[],
                        applyOffset=False,
                        userKnowledge=False,
                    )
                )
                mock_cursor.execute.assert_any_call(
                    "EXPLAIN QUERY PLAN " + sqlF
                )  # noqa: E501
                mock_cursor.execute.assert_any_call(
                    "EXPLAIN QUERY PLAN " + sqlA,
                    (0,) * (len(typesF) + len(typesA)),  # noqa: E501
                )

    def test_generateAnnotationOutput_with_allow_duplicate_output(self, mixin):
        # Set `allow_duplicate_output` to "yes" to enter the `elif` block
        mixin._options.allow_duplicate_output = "yes"

        # Mock to cursor and `execute` with simulated values
        mock_cursorF = MagicMock()
        mock_cursorA = MagicMock()

        # Each call to `execute` should return a new iterator to
        # avoid `StopIteration`
        mock_cursorF.execute.side_effect = [
            iter([("filter_result_1", 1), ("filter_result_2", 2)]),  # 1 call
        ]
        mock_cursorA.execute.side_effect = [
            iter(
                [("annotate_result_1", None), ("annotate_result_2", None)]
            ),  # 2 call  # noqa: E501
            iter(
                [("annotate_result_1", None), ("annotate_result_2", None)]
            ),  # 3 call (if needed)
        ]

        # Mock for `generateQueryResults` to avoid real execution
        with patch.object(
            mixin._loki._db, "cursor", side_effect=[mock_cursorF, mock_cursorA]
        ):
            # Run the method with simulated `typesF` and `typesA`
            typesF = ["snp"]
            typesA = ["gene"]

            # Collect all results
            result = list(mixin.generateAnnotationOutput(typesF, typesA))

            # Check if the header was generated correctly with `#`
            assert result[0][0].startswith("#")

            # Check the content of the result generated by the yield
            expected_results = [
                ("#snp", "gene"),
                ("filter_result_1", "annotate_result_1"),
                ("filter_result_2", "annotate_result_1"),
            ]
            assert result == expected_results

    def test_generateAnnotationOutput_with_no_idsA(self, mixin):
        # Set `allow_duplicate_output` to "yes" to enter the `elif` block
        mixin._options.allow_duplicate_output = "yes"

        # Mock para o cursor e `execute` com valores simulados
        mock_cursorF = MagicMock()
        mock_cursorA = MagicMock()

        # Define `mock_cursorA.execute` to return an empty iterator to
        # simulate no `idsA`
        mock_cursorF.execute.side_effect = [
            iter([("filter_result_1", 1), ("filter_result_2", 2)])
        ]
        # Define `mock_cursorA.execute` to return empty iterators instead of
        # empty lists directly
        mock_cursorA.execute.side_effect = [
            iter([]),  # Returns an empty list, simulating no match for `idsA`
            iter([]),  # Another empty ls to ensure `StopIteration` isnt raised
        ]

        # Mock for `generateQueryResults` to avoid real execution
        with patch.object(
            mixin._loki._db, "cursor", side_effect=[mock_cursorF, mock_cursorA]
        ):
            # Run the method with simulated `typesF` and `typesA`
            typesF = ["snp"]
            typesA = ["gene"]

            # Collect all results
            result = list(mixin.generateAnnotationOutput(typesF, typesA))
            # Check if `emptyA` was yield when `idsA` is empty
            expected_results = [
                ("#snp", "gene"),  # Header with `#`
                ("filter_result_1", None),  # `emptyA` should be yield
                ("filter_result_2", None),  # `emptyA` should be yield
            ]
            assert result == expected_results

    def test_generateAnnotationOutput_with_else_path(self, mixin):
        # Configura `_options` para não entrar no `if` nem no `elif`
        mixin._options.debug_query = False
        mixin._options.allow_duplicate_output = "no"

        # Mock to cursor and `execute` with simulated values
        mock_cursorF = MagicMock()
        mock_cursorA = MagicMock()

        # Define `mock_cursorA.execute` to return a set of results
        mock_cursorF.execute.side_effect = [
            iter([("filter_result_1", 1), ("filter_result_2", 2)]),
        ]

        # Define `mock_cursorA.execute to return a set of annotation results
        mock_cursorA.execute.side_effect = [
            iter([("annotate_result_1", None), ("annotate_result_2", None)]),
            iter([("annotate_result_3", None)]),  # simule if `idsA`
        ]

        # Mock for `generateQueryResults` to avoid real execution
        with patch.object(
            mixin._loki._db, "cursor", side_effect=[mock_cursorF, mock_cursorA]
        ):
            # Run the method with simulated `typesF` and `typesA`
            typesF = ["snp"]
            typesA = ["gene"]

            # Collect all results
            result = list(mixin.generateAnnotationOutput(typesF, typesA))

            # Check if the header was generated correctly with emptyA` is
            # yield when `idsA` is empty
            expected_results = [
                ("#snp", "gene"),
                ("filter_result_1", "annotate_result_1"),
                ("filter_result_2", "annotate_result_3"),
            ]
            assert result == expected_results

    def test_generateAnnotationOutput_with_else_path_and_empty_idsA(
        self, mixin
    ):  # noqa: E501
        # setting `_options` to not go into `if` or `elif`
        mixin._options.debug_query = False
        mixin._options.allow_duplicate_output = "no"

        # Mock for cursor and `execute` with simulated values
        mock_cursorF = MagicMock()
        mock_cursorA = MagicMock()

        # `mock_cursorF.execute` returns a set of results
        mock_cursorF.execute.side_effect = [
            iter([("filter_result_1", 1), ("filter_result_2", 2)]),
        ]

        # `mock_cursorA.execute` returns an empty iterator to simulate that
        # `idsA` remains empty
        mock_cursorA.execute.side_effect = [
            iter([]),  # no results for "filter_result_1"
            iter([]),  # no results for "filter_result_2"
        ]

        # Mock to avoid real execution in `generateQueryResults`
        with patch.object(
            mixin._loki._db, "cursor", side_effect=[mock_cursorF, mock_cursorA]
        ):
            # Run the method with simulated `typesF` and `typesA`
            typesF = ["snp"]
            typesA = ["gene"]

            # Collect all results
            result = list(mixin.generateAnnotationOutput(typesF, typesA))

            # Check if the header was generated correctly with `#`
            expected_results = [
                ("#snp", "gene"),  # Header wint `#`
                (
                    "filter_result_1",
                    None,
                ),  # `emptyA` to "filter_result_1" without annotation results
                (
                    "filter_result_2",
                    None,
                ),  # `emptyA` to "filter_result_2" without annotation results
            ]
            assert result == expected_results

    # -------------------------------------------------------------------------

    # -- Tests to identifyCandidateModelBiopolymers method --

    def test_identifyCandidateModelBiopolymers(self, mixin):
        mixin.identifyCandidateModelBiopolymers()
        assert mixin._inputFilters["cand"]["main_biopolymer"] == 1
        assert mixin._inputFilters["cand"]["alt_biopolymer"] == 1

    def test_identifyCandidateModelBiopolymers_with_main_filters(self, mixin):
        # Setting `_inputFilters["main"]["gene"]` to 1 to add a filter that is
        # not "group" or "source"
        mixin._inputFilters["main"][
            "gene"
        ] = 1  # adding a filter that is not "group" or "source"
        mixin._inputFilters["main"][
            "source"
        ] = 0  # "source" e "group" will be ignored  # noqa: E501

        mock_cursor = MagicMock()
        # Define `side_effect` to return a count of candidates as integers
        mock_cursor.execute.side_effect = [
            [
                ("filter_result_1", 1),
                ("filter_result_2", 2),
            ],  # call 1 for insertions
            iter([(2,)]),  # call 2 SELECT COUNT()
            iter([(2,)]),  # additional call to ensure `execute` has enough values
        ]

        # Mock para `generateQueryResults` avoid real execution
        with patch.object(
            mixin._loki._db, "cursor", return_value=mock_cursor
        ), patch.object(mixin, "log") as mock_log, patch.object(
            mixin, "generateQueryResults", return_value=[(1,), (2,)]
        ):
            # Run the method to test
            mixin.identifyCandidateModelBiopolymers()

            # Check if the log was called for "main"
            mock_log.assert_any_call("identifying main model candidiates ...")
            assert mixin._inputFilters["cand"]["main_biopolymer"] == 1

    def test_identifyCandidateModelBiopolymers_with_alt_filters(self, mixin):
        # Setting `_inputFilters["alt"]["gene"]` to the sum to add a filter that  # noqa: E501
        mixin._inputFilters["alt"][
            "gene"
        ] = 1  # adding a filter that is not "group" or "source"
        mixin._inputFilters["alt"][
            "group"
        ] = 0  # "group" e "source" will be ignored  # noqa: E501

        mock_cursor = MagicMock()
        # Define `side_effect` to return a count of candidates as integers
        mock_cursor.execute.side_effect = [
            [
                ("filter_result_1", 1),
                ("filter_result_2", 2),
            ],
            iter([(2,)]),
            iter([(2,)]),
        ]

        # Mock to avoid real execution in `generateQueryResults`
        with patch.object(
            mixin._loki._db, "cursor", return_value=mock_cursor
        ), patch.object(mixin, "log") as mock_log, patch.object(
            mixin, "generateQueryResults", return_value=[(1,), (2,)]
        ):
            # Run the method to test
            mixin.identifyCandidateModelBiopolymers()

            # Check if the log was called for "alt"
            mock_log.assert_any_call("identifying alternate model candidiates ...")
            assert mixin._inputFilters["cand"]["alt_biopolymer"] == 1

    # -------------------------------------------------------------------------

    # -- Tests to getGeneModels method --

    def test_getGeneModels(self, mixin):
        # Simulate a return value to avoid empty sequence in `max()`
        mixin._loki._db.cursor.return_value.execute.return_value = [(1,)]
        # Executa o método para testar
        # Run the method to test
        models = mixin.getGeneModels()
        # Check if the return value is correct
        assert models == [("result1",), ("result2",)]

    # -------------------------------------------------------------------------

    # -- Tests to generateModelOutput method --

    def test_generateModelOutput(self, mixin):
        mixin._loki._db.cursor.return_value.execute.return_value = [(1,)]
        mixin.getGeneModels = MagicMock(
            return_value=[("result1", "result2", 1, 2)]
        )  # noqa: E501
        typesL = ["snp"]
        typesR = ["gene"]
        result = list(mixin.generateModelOutput(typesL, typesR))
        assert result == [("#snp1", "gene2", "score(src-grp)"), ("1-2",)]
