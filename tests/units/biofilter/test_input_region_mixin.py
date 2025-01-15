import pytest
from unittest.mock import MagicMock
from biofilter_modules.mixins.input_region_mixin import RegionInputMixin


class TestRegionInputMixin:
    @pytest.fixture
    def mixin(self):
        mixin = RegionInputMixin()
        mixin._loki = MagicMock()
        mixin._options = MagicMock()
        mixin._inputFilters = {"test_db": {"region": 0}}
        return mixin

    def test_unionInputRegions(self, mixin):
        db = "test_db"
        # TODO: Need to check if in integrated test what is this datatypes
        regions = [
            ("region1", "1", "100", "200", "extra1"),
            ("region2", "2", "300", "400", "extra2"),
        ]
        errorCallback = MagicMock()

        # Mock behavior for cursor executemany
        def cursor_executemany_side_effect(sql, data):
            return [
                (1, *region) for region in data
            ]  # Simulates rows returned for each region

        mixin._loki._db.cursor().executemany.side_effect = (
            cursor_executemany_side_effect
        )
        mixin.prepareTableForUpdate = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin.warn = MagicMock()

        mixin.unionInputRegions(db, regions, errorCallback)

        # Verify internal calls and warnings
        mixin.prepareTableForUpdate.assert_called_once_with(db, "region")
        mixin.logPush.assert_called_once_with(
            "adding to test_db region filter ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with("... OK: added 1 regions\n")
        assert mixin._inputFilters[db]["region"] == 1

    def test_unionInputRegions_with_invalid_regions(self, mixin):
        db = "test_db"
        # TODO: Need to check if in integrated test what is this datatypes
        regions = [
            ("region1", "1", "100", "200", "extra1"),
            ("region2", "2", "300", "400", "extra2"),
        ]
        errorCallback = MagicMock()

        def cursor_executemany_side_effect(sql, data):
            data_list = list(data)
            return [(1, *data_list[0]), (1, *data_list[1])]

        mixin._loki._db.cursor().executemany.side_effect = (
            cursor_executemany_side_effect
        )
        mixin.prepareTableForUpdate = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()
        mixin.warn = MagicMock()

        mixin.unionInputRegions(db, regions, errorCallback)

        mixin.warn.assert_called_once_with(
            "WARNING: ignored 1 invalid regions\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with("... OK: added 1 regions\n")
        assert mixin._inputFilters[db]["region"] == 1

    def test_intersectInputRegions(self, mixin):
        db = "test_db"
        regions = [
            ("region1", "1", "100", "200", "extra1"),
            ("region2", "2", "300", "400", "extra2"),
        ]
        errorCallback = MagicMock()

        mixin._inputFilters[db]["region"] = 1
        mixin._loki._db.cursor().getconnection().changes.return_value = 1
        mixin.prepareTableForQuery = MagicMock()
        mixin.logPush = MagicMock()
        mixin.logPop = MagicMock()

        mixin.intersectInputRegions(db, regions, errorCallback)

        mixin.prepareTableForQuery.assert_called_once_with(db, "region")
        mixin._loki._db.cursor().executemany.assert_called_once()
        mixin.logPush.assert_called_once_with(
            "reducing test_db region filter ...\n"
        )  # noqa E501
        mixin.logPop.assert_called_once_with(
            "... OK: kept 0 regions (1 dropped)\n"
        )  # noqa E501
        assert mixin._inputFilters[db]["region"] == 2

    def test_intersectInputRegions_triggers_union(self, mixin):
        db = "test_db"
        regions = [("region1", "1", "100", "200", "extra1")]
        errorCallback = MagicMock()

        mixin._inputFilters[db]["region"] = 0
        mixin.unionInputRegions = MagicMock()

        mixin.intersectInputRegions(db, regions, errorCallback)

        # Verifies that `unionInputRegions` was triggered when `_inputFilters` was 0  # noqa E501
        mixin.unionInputRegions.assert_called_once_with(
            db, regions, errorCallback
        )  # noqa E501
