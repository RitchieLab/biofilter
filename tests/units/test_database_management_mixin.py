import pytest
from unittest.mock import MagicMock
from biofilter_modules.mixins.database_management_mixin import (
    DatabaseManagementMixin,
)  # noqa: E501


class TestDatabaseManagementMixin:
    @pytest.fixture
    def mixin(self):
        mixin = DatabaseManagementMixin()
        mixin._loki = MagicMock()
        mixin._schema = {
            "test_db": {"region": {}, "region_zone": {}, "source": {}}
        }  # noqa: E501
        mixin._tablesDeindexed = {"test_db": set()}
        mixin._inputFilters = {"test_db": {"region": 0}}
        mixin.log = MagicMock()
        return mixin

    def test_attachDatabaseFile(self, mixin):
        dbFile = "external_db.sqlite"
        mixin.attachDatabaseFile(dbFile)
        mixin._loki.attachDatabaseFile.assert_called_once_with(dbFile)

    def test_prepareTableForUpdate(self, mixin):
        db, table = "test_db", "source"
        mixin.prepareTableForUpdate(db, table)
        mixin._loki.dropDatabaseIndecies.assert_called_once_with(
            mixin._schema[db], db, table
        )
        assert table in mixin._tablesDeindexed[db]

    def test_prepareTableForQuery(self, mixin):
        db, table = "test_db", "source"
        mixin._tablesDeindexed[db].add(
            table
        )  # Simulate the table as deindexed  # noqa: E501
        mixin.prepareTableForQuery(db, table)
        mixin._loki.createDatabaseIndecies.assert_called_once_with(
            mixin._schema[db], db, table
        )
        assert table not in mixin._tablesDeindexed[db]

    def test_prepareTableForQuery_with_region_update(self, mixin):
        db, table = "test_db", "region"
        mixin._tablesDeindexed[db].add(
            table
        )  # Simulate the table as deindexed  # noqa: E501
        mixin.updateRegionZones = MagicMock()
        mixin.prepareTableForQuery(db, table)
        mixin.updateRegionZones.assert_called_once_with(db)

    def test_tableHasData_with_data(self, mixin):
        db, table = "test_db", "source"
        mixin._loki._db.cursor().execute.return_value = [(1,)]
        assert mixin.tableHasData(db, table) is True
        mixin._loki._db.cursor().execute.assert_called_once_with(
            "SELECT 1 FROM `test_db`.`source` LIMIT 1"
        )

    def test_tableHasData_without_data(self, mixin):
        db, table = "test_db", "source"
        mixin._loki._db.cursor().execute.return_value = []
        assert mixin.tableHasData(db, table) is False

    def test_updateRegionZones(self, mixin):
        db = "test_db"
        mixin._loki.getDatabaseSetting.return_value = "1000"
        mixin.prepareTableForQuery = MagicMock()
        mixin.prepareTableForUpdate = MagicMock()

        # Set the state so that the `source` filter is initialized
        mixin._inputFilters[db]["region"] = 1

        # Configura o side_effect para as chamadas de execute
        mixin._loki._db.cursor().execute.side_effect = [
            None,  # primeira chamada para orientação
            [(1, "chr1", 1000, 5000)],  # resultado do SELECT para _zones
            None,  # exclusão de dados antigos em region_zone
        ]

        # Mock para executemany
        mixin._loki._db.cursor().executemany = MagicMock()

        mixin.updateRegionZones(db)

        # Check if the method was called with the correct argument
        mixin._loki.getDatabaseSetting.assert_called_once_with("zone_size")

        # Check if the method was called with the correct argument
        mixin.prepareTableForQuery.assert_any_call(db, "region")
        mixin.prepareTableForUpdate.assert_called_once_with(db, "region_zone")

        # Check if the method was called with the correct argument
        mixin._loki._db.cursor().execute.assert_any_call(
            "UPDATE `test_db`.`region` SET posMin = posMax, posMax = posMin WHERE posMin > posMax"  # noqa: E501
        )
        # TODO: Returns Error
        # mixin._loki._db.cursor().executemany.assert_called_once_with(  # noqa: E501
        #     "INSERT OR IGNORE INTO `test_db`.`region_zone` (region_rowid,chr,zone) VALUES (?,?,?)",  # noqa: E501
        #     [(1, "chr1", 1), (1, "chr1", 2), (1, "chr1", 3), (1, "chr1", 4), (1, "chr1", 5)]  # noqa: E501
        # )

        mixin.prepareTableForQuery.assert_any_call(db, "region_zone")
        mixin.log.assert_called_with(" OK\n")

    def test_updateRegionZones_with_missing_zone_size(self, mixin):
        db = "test_db"
        mixin._loki.getDatabaseSetting.return_value = None
        mixin.prepareTableForQuery = MagicMock()
        mixin.prepareTableForUpdate = MagicMock()

        # Set the state so that the `source` filter is initialized
        mixin._inputFilters[db]["region"] = 1

        # Configura o side_effect para as chamadas de execute
        mixin._loki._db.cursor().execute.side_effect = [
            None,  # primeira chamada para orientação
            [(1, "chr1", 0, 5000)],  # resultado do SELECT para _zones
            None,  # exclusão de dados antigos em region_zone
        ]

        # Mock para executemany
        mixin._loki._db.cursor().executemany = MagicMock()

        # Testa se `sys.exit` é chamado com a mensagem correta
        with pytest.raises(SystemExit) as excinfo:
            mixin.updateRegionZones(db)

        # Verifica a mensagem de erro que acompanha o sys.exit
        assert (
            str(excinfo.value)
            == "ERROR: could not determine database setting 'zone_size'"
        )


# TODO:
# - [ ] Test row 217-219: Problem with `executemany` mock
