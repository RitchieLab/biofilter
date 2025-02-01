# database_operations_set_mixin.py

class DbOperationsSetMixin:

    def addWarning(self, source_id, warning):
        """

        Args:

        Returns:
            None
        """
        self._db.cursor().execute(
            "INSERT INTO `db`.warning (source_id, warning) VALUES (?, ?)",  # noqa E501
            (source_id, warning),
        )

    def setDatabaseSetting(self, setting, value):
        """
        Sets a specific setting value in the database.

        Args:
            setting (str): The name of the setting to set.
            value: The value to set for the specified setting.

        Returns:
            None
        """
        self._db.cursor().execute(
            "INSERT OR REPLACE INTO `db`.`setting` (setting, value) VALUES (?, ?)",  # noqa E501
            (setting, value),
        )
