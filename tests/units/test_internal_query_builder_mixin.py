import pytest
from unittest.mock import MagicMock
import collections
import itertools
from biofilter_modules.mixins.internal_query_builder_mixin import InternalQueryBuilderMixin


class TestInternalQueryBuilderMixin:
    @pytest.fixture
    def query_builder(self):
        class MockLoki:
            def getDatabaseSetting(self, setting):
                return 1000  # Mock value for zone_size

            def getLDProfileID(self, profile):
                return 1  # Mock value for ld_profile ID

            _db = None  # Mock database connection

        class MockOptions:
            debug_logic = False
            debug_query = False
            allow_unvalidated_snp_positions = "no"
            coordinate_base = 1
            regions_half_open = "no"
            region_position_margin = 500
            region_match_percent = 50
            region_match_bases = 100
            allow_ambiguous_knowledge = "no"
            reduce_ambiguous_knowledge = "no"
            alternate_model_filtering = "no"
            ld_profile = "default"

        class MockSchema:
            pass

        class MockInputFilters:
            pass

        class MockQueryBuilder(InternalQueryBuilderMixin):
            _loki = MockLoki()
            _options = MockOptions()
            _schema = {"main": {}, "alt": {}, "cand": {}, "user": {}, "db": {}}
            _inputFilters = {"main": {}, "alt": {}, "cand": {}}
            _queryAliasJoinConditions = []
            _queryColumnSources = {}
            _queryAliasTable = {}
            _queryAliasConditions = {}
            _queryAliasPairConditions = {}

            def warnPush(self, msg):
                pass

            def warn(self, msg):
                pass

            def log(self, msg):
                pass

            def getOptionTypeID(self, option, optional=False):
                return 1  # Mock value for typeID_gene

            def getOptionNamespaceID(self, option, optional=False):
                return 1  # Mock value for namespaceID_symbol

            def prepareTableForQuery(self, db, tbl):
                pass

        # Configuração para `_queryColumnSources`, `aliasAdjacent`, e `aliasJoinConditions`
        mock_query_builder = MockQueryBuilder()
        mock_query_builder._queryColumnSources = {
            "col1": [("alias1", "rowid1", "col1_expr")],
            "col2": [("alias2", "rowid2", "col2_expr")],
        }
        mock_query_builder._queryAliasTable = {
            "alias1": ("main", "table1"),
            "alias2": ("main", "table2"),
        }
        mock_query_builder._queryAliasJoinConditions = [
            (["alias1"], ["alias2"])
        ]
        mock_query_builder.aliasAdjacent = collections.defaultdict(set)
        mock_query_builder.aliasAdjacent["alias1"].add("alias2")
        mock_query_builder.aliasAdjacent["alias2"].add("alias1")

        return mock_query_builder

    def test_get_query_template(self, query_builder):
        template = query_builder.getQueryTemplate()
        assert isinstance(template, dict)
        assert "_columns" in template
        assert "SELECT" in template
        assert "_rowid" in template
        assert "FROM" in template
        assert "LEFT JOIN" in template
        assert "WHERE" in template
        assert "GROUP BY" in template
        assert "HAVING" in template
        assert "ORDER BY" in template
        assert "LIMIT" in template

    # Test to Build Query
    def test_build_query_invalid_mode(self, query_builder):
        with pytest.raises(AssertionError):
            query_builder.buildQuery(
                mode="invalid_mode",
                focus="main",
                select=["snp_id", "snp_label"]
            )

    def test_build_query_invalid_focus(self, query_builder):
        with pytest.raises(AssertionError):
            query_builder.buildQuery(
                mode="filter",
                focus="invalid_focus",
                select=["snp_id", "snp_label"]
            )

    def test_build_query_no_outputs_or_conditions(self, query_builder):
        query_builder._queryColumnSources = {"snp_id": [("alias", 1, "expr")]}
        with pytest.raises(Exception, match="internal query with no outputs or conditions"):
            query_builder.buildQuery(
                mode="filter",
                focus="main",
                select=[],
                having={},
                where={}
            )

    def test_build_query_unsupported_column(self, query_builder):
        query_builder._queryColumnSources = {}
        with pytest.raises(Exception, match="internal query with unsupported column"):
            query_builder.buildQuery(
                mode="filter",
                focus="main",
                select=["unsupported_column"]
            )

    # # news
    # def test_buildQuery_basic(self, query_builder):
    #     """Test buildQuery with basic parameters to check structure."""
    #     result = query_builder.buildQuery(
    #         mode="filter",
    #         focus="main",
    #         select=["col1", "col2"],
    #     )
    #     assert isinstance(result, dict)
    #     assert "SELECT" in result
    #     assert "FROM" in result
    #     assert "WHERE" in result

    # def test_buildQuery_with_having_conditions(self, query_builder):
    #     """Test buildQuery with having conditions."""
    #     result = query_builder.buildQuery(
    #         mode="filter",
    #         focus="main",
    #         select=["col1"],
    #         having={"col1": {"= 1"}},
    #     )
    #     assert "col1" in result["HAVING"]

    # def test_buildQuery_with_where_conditions(self, query_builder):
    #     """Test buildQuery with where conditions."""
    #     result = query_builder.buildQuery(
    #         mode="filter",
    #         focus="main",
    #         select=["col1"],
    #         where={("alias1", "col1"): {"= 1"}},
    #     )
    #     assert "WHERE" in result
    #     assert any("alias1.col1 = 1" in cond for cond in result["WHERE"])

    # def test_buildQuery_with_apply_offset(self, query_builder):
    #     """Test buildQuery with applyOffset set to True."""
    #     query_builder._options.coordinate_base = 2
    #     result = query_builder.buildQuery(
    #         mode="filter",
    #         focus="main",
    #         select=["col1"],
    #         applyOffset=True
    #     )
    #     assert "pMinOffset" in result["_offsets"]

    # def test_buildQuery_user_knowledge_filter(self, query_builder):
    #     """Test buildQuery with userKnowledge flag set to True."""
    #     result = query_builder.buildQuery(
    #         mode="filter",
    #         focus="main",
    #         select=["col1"],
    #         userKnowledge=True
    #     )
    #     assert "user" in result["FROM"]

    # def test_buildQuery_invalid_mode(self, query_builder):
    #     """Test buildQuery with an invalid mode should raise an error."""
    #     with pytest.raises(AssertionError):
    #         query_builder.buildQuery(
    #             mode="invalid",
    #             focus="main",
    #             select=["col1"]
    #         )

    # def test_buildQuery_invalid_focus(self, query_builder):
    #     """Test buildQuery with an invalid focus should raise an error."""
    #     with pytest.raises(AssertionError):
    #         query_builder.buildQuery(
    #             mode="filter",
    #             focus="invalid_focus",
    #             select=["col1"]
    #         )

    # def test_buildQuery_no_output_conditions(self, query_builder):
    #     """Test buildQuery to raise error if there are no outputs or conditions."""
    #     query_builder._queryColumnSources = {}
    #     with pytest.raises(Exception, match="no outputs or conditions"):
    #         query_builder.buildQuery(
    #             mode="filter",
    #             focus="main",
    #             select=[]
    #         )