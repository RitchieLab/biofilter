import pytest
from biofilter_modules.mixins.internal_query_builder_mixin import (
    InternalQueryBuilderMixin,
)

# class TestInternalQueryBuilderMixin:
#     @pytest.fixture
#     def query_builder(self):
#         class MockLoki:
#             def getDatabaseSetting(self, setting):
#                 return 1000  # Mock value for zone_size

#             def getLDProfileID(self, profile):
#                 return 1  # Mock value for ld_profile ID

#             _db = None  # Mock database connection

#         class MockOptions:
#             debug_logic = False
#             debug_query = False
#             allow_unvalidated_snp_positions = "no"
#             coordinate_base = 1
#             regions_half_open = "no"
#             region_position_margin = 500
#             region_match_percent = 50
#             region_match_bases = 100
#             allow_ambiguous_knowledge = "no"
#             reduce_ambiguous_knowledge = "no"
#             alternate_model_filtering = "no"
#             ld_profile = "default"

#         class MockSchema:
#             pass

#         class MockInputFilters:
#             pass

#         class MockQueryBuilder(InternalQueryBuilderMixin):
#             _loki = MockLoki()
#             _options = MockOptions()
#             _schema = {"main": {}, "alt": {}, "cand": {}, "user": {}, "db": {}}
#             _inputFilters = {"main": {}, "alt": {}, "cand": {}}

#             def warnPush(self, msg):
#                 pass

#             def warn(self, msg):
#                 pass

#             def log(self, msg):
#                 pass

#             def getOptionTypeID(self, option, optional=False):
#                 return 1  # Mock value for typeID_gene

#             def getOptionNamespaceID(self, option, optional=False):
#                 return 1  # Mock value for namespaceID_symbol

#             def prepareTableForQuery(self, db, tbl):
#                 pass

#         return MockQueryBuilder()

#     def test_get_query_template(self, query_builder):
#         template = query_builder.getQueryTemplate()
#         assert isinstance(template, dict)
#         assert "_columns" in template
#         assert "SELECT" in template
#         assert "_rowid" in template
#         assert "FROM" in template
#         assert "LEFT JOIN" in template
#         assert "WHERE" in template
#         assert "GROUP BY" in template
#         assert "HAVING" in template
#         assert "ORDER BY" in template
#         assert "LIMIT" in template

#     def test_build_query(self, query_builder):
#         query = query_builder.buildQuery(
#             mode="filter",
#             focus="main",
#             select=["snp_id", "snp_label"],
#             having={"snp_id": {"= 1"}},
#             where={("m_s", "rs"): {"= 1"}},
#             applyOffset=True,
#             fromFilter={"main": {"snp": True}},
#             joinFilter={"main": {"snp": True}},
#             userKnowledge=True,
#         )
#         assert isinstance(query, dict)
#         assert "SELECT" in query
#         assert "FROM" in query
#         assert "LEFT JOIN" in query
#         assert "WHERE" in query
#         assert "GROUP BY" in query
#         assert "HAVING" in query
#         assert "ORDER BY" in query
#         assert "LIMIT" in query

#     def test_get_query_text(self, query_builder):
#         query = query_builder.buildQuery(
#             mode="filter",
#             focus="main",
#             select=["snp_id", "snp_label"],
#             having={"snp_id": {"= 1"}},
#             where={("m_s", "rs"): {"= 1"}},
#         )
#         query_text = query_builder.getQueryText(query)
#         assert isinstance(query_text, str)
#         assert "SELECT" in query_text
#         assert "FROM" in query_text
#         assert "WHERE" in query_text

#     def test_prepare_tables_for_query(self, query_builder):
#         query = query_builder.buildQuery(
#             mode="filter",
#             focus="main",
#             select=["snp_id", "snp_label"],
#             having={"snp_id": {"= 1"}},
#             where={("m_s", "rs"): {"= 1"}},
#         )
#         query_builder.prepareTablesForQuery(query)
#         # No assertion needed, just ensure no exceptions are raised

#     def test_generate_query_results(self, query_builder):
#         query = query_builder.buildQuery(
#             mode="filter",
#             focus="main",
#             select=["snp_id", "snp_label"],
#             having={"snp_id": {"= 1"}},
#             where={("m_s", "rs"): {"= 1"}},
#         )
#         results = list(query_builder.generateQueryResults(query))
#         assert isinstance(results, list)
#         #
#  Since this is a mock, we don't expect actual results


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

        return MockQueryBuilder()

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

    def test_build_query(self, query_builder):
        query = query_builder.buildQuery(
            mode="filter",
            focus="main",
            select=["snp_id", "snp_label"],
            having={"snp_id": {"= 1"}},
            where={("m_s", "rs"): {"= 1"}},
            applyOffset=True,
            fromFilter={"main": {"snp": True}},
            joinFilter={"main": {"snp": True}},
            userKnowledge=True,
        )
        assert isinstance(query, dict)
        assert "SELECT" in query
        assert "FROM" in query
        assert "LEFT JOIN" in query
        assert "WHERE" in query
        assert "GROUP BY" in query
        assert "HAVING" in query
        assert "ORDER BY" in query
        assert "LIMIT" in query

    def test_build_query_invalid_mode(self, query_builder):
        with pytest.raises(AssertionError):
            query_builder.buildQuery(
                mode="invalid_mode", focus="main", select=["snp_id", "snp_label"]
            )

    def test_build_query_invalid_focus(self, query_builder):
        with pytest.raises(AssertionError):
            query_builder.buildQuery(
                mode="filter", focus="invalid_focus", select=["snp_id", "snp_label"]
            )

    def test_build_query_no_outputs_or_conditions(self, query_builder):
        query_builder._queryColumnSources = {"snp_id": [("alias", 1, "expr")]}
        with pytest.raises(
            Exception, match="internal query with no outputs or conditions"
        ):
            query_builder.buildQuery(
                mode="filter", focus="main", select=[], having={}, where={}
            )

    def test_build_query_unsupported_column(self, query_builder):
        query_builder._queryColumnSources = {}
        with pytest.raises(Exception, match="internal query with unsupported column"):
            query_builder.buildQuery(
                mode="filter", focus="main", select=["unsupported_column"]
            )

    def test_get_query_text(self, query_builder):
        query = query_builder.buildQuery(
            mode="filter",
            focus="main",
            select=["snp_id", "snp_label"],
            having={"snp_id": {"= 1"}},
            where={("m_s", "rs"): {"= 1"}},
        )
        query_text = query_builder.getQueryText(query)
        assert isinstance(query_text, str)
        assert "SELECT" in query_text
        assert "FROM" in query_text
        assert "WHERE" in query_text

    def test_prepare_tables_for_query(self, query_builder):
        query = query_builder.buildQuery(
            mode="filter",
            focus="main",
            select=["snp_id", "snp_label"],
            having={"snp_id": {"= 1"}},
            where={("m_s", "rs"): {"= 1"}},
        )
        query_builder.prepareTablesForQuery(query)
        # No assertion needed, just ensure no exceptions are raised

    def test_generate_query_results(self, query_builder):
        query = query_builder.buildQuery(
            mode="filter",
            focus="main",
            select=["snp_id", "snp_label"],
            having={"snp_id": {"= 1"}},
            where={("m_s", "rs"): {"= 1"}},
        )
        results = list(query_builder.generateQueryResults(query))
        assert isinstance(results, list)
        # Since this is a mock, we don't expect actual results
