import pytest
from unittest.mock import MagicMock
from biofilter_modules.mixins.input_source_mixin import SourceInputMixin


class TestSourceInputMixin:
    @pytest.fixture
    def mixin(self):
        mixin = SourceInputMixin()
        mixin._loki = MagicMock()
        mixin._inputFilters = {"test_db": {"source": 0}}
        return mixin

    def test_unionInputSources(self, mixin):
        db = "test_db"
        names = ["source1", "source2"]
        errorCallback = MagicMock()

        # Define return to `getSourceID` e `getUserSourceID`
        def get_source_id(source):
            return {"source1": 1, "source2": None}.get(source)

        mixin._loki.getSourceID = MagicMock(side_effect=get_source_id)
        mixin.getUserSourceID = MagicMock(return_value=2)
        mixin.prepareTableForUpdate = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin.warn = MagicMock()

        # Run method
        mixin.unionInputSources(db, names, errorCallback)

        # Chack mocks
        mixin.logPush.assert_called_once_with(
            "adding to test_db source filter ...\n"
        )  # noqa E501
        mixin.prepareTableForUpdate.assert_called_once_with(db, "source")
        mixin._loki._db.cursor().execute.assert_called()
        mixin.logPop.assert_called_once_with("... OK: added 2 sources\n")
        assert mixin._inputFilters[db]["source"] == 1

    def test_unionInputSources_with_invalid_sources(self, mixin):
        db = "test_db"
        names = ["invalid_source"]
        errorCallback = MagicMock()

        mixin._loki.getSourceID = MagicMock(return_value=None)
        mixin.getUserSourceID = MagicMock(return_value=None)
        mixin.prepareTableForUpdate = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin.warn = MagicMock()

        # Run method
        mixin.unionInputSources(db, names, errorCallback)

        # Check mocks
        mixin.warn.assert_called_once_with(
            "WARNING: ignored 1 unrecognized source identifier(s)\n"
        )
        mixin.logPop.assert_called_once_with("... OK: added 0 sources\n")
        assert mixin._inputFilters[db]["source"] == 1

    def test_intersectInputSources(self, mixin):
        db = "test_db"
        names = ["source1", "source2"]

        # Set the state so that the `source` filter is initialized
        mixin._inputFilters[db]["source"] = 1

        mixin._loki.getSourceID = MagicMock(return_value=1)
        mixin.getUserSourceID = MagicMock(return_value=None)
        mixin.prepareTableForQuery = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()

        # Set the cursor mock to return fictitious values for `changes`
        mixin._loki._db.cursor().getconnection().changes.return_value = 1

        # Run method
        mixin.intersectInputSources(db, names)

        # Check mocks
        mixin.logPush.assert_called_once_with(
            "reducing test_db source filter ...\n"
        )  # noqa E501
        mixin.prepareTableForQuery.assert_called_once_with(db, "source")
        mixin._loki._db.cursor().execute.assert_called()
        mixin.logPop.assert_called_once_with(
            "... OK: kept 0 sources (1 dropped)\n"
        )  # noqa E501
        assert mixin._inputFilters[db]["source"] == 2

    def test_intersectInputSources_triggers_union(self, mixin):
        db = "test_db"
        names = ["source1", "source2"]
        errorCallback = MagicMock()

        # Mock `unionInputSources` and filter state
        mixin.unionInputSources = MagicMock()
        mixin._inputFilters[db]["source"] = 0

        # Run Method
        mixin.intersectInputSources(db, names, errorCallback)

        # Check if `unionInputSources` was called
        mixin.unionInputSources.assert_called_once_with(
            db, names, errorCallback
        )  # noqa E501
