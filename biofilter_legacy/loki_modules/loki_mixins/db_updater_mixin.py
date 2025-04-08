# database_mixin.py


class DbUpdaterMixin:

    def updateDatabase(
        self,
        sources=None,
        sourceOptions=None,
        cacheOnly=False,
        forceUpdate=False,
        # noqa E501
    ):
        """
        Updates the database using the specified source modules and options.

        If the updater is not already initialized, it imports and initializes
        the updater module.

        Args:
            - sources (list, optional): A list of source modules to update
            from. Defaults to None, which updates from all sources.
            - sourceOptions (dict, optional): A dictionary of options for the
            source modules. Defaults to None.
            - cacheOnly (bool, optional): If True, only updates the cache.
            Defaults to False.
            - forceUpdate (bool, optional): If True, forces the update even if
            not necessary. Defaults to False.

        Returns:
            Any: The result of the update operation.

        Raises:
            Exception: If the database is finalized and cannot be updated.
        """
        if self.getDatabaseSetting("finalized", int):
            raise Exception("ERROR: cannot update a finalized database")
        if not self._updater:
            import loki_modules.loki_updater as loki_updater

            self._updater = loki_updater.Updater(self, self._is_test)

        return self._updater.updateDatabase(
            sources, sourceOptions, cacheOnly, forceUpdate
        )

    def prepareTableForUpdate(self, table):
        """
        Prepares a table for update by the updater.

        If the database is finalized, it raises an exception.

        Args:
            table (str): The name of the table to prepare for update.

        Returns:
            Any: The result of the preparation.

        Raises:
            Exception: If the database is finalized and cannot be updated.
        """
        if self.getDatabaseSetting("finalized", int):
            raise Exception("ERROR: cannot update a finalized database")
        if self._updater:
            return self._updater.prepareTableForUpdate(table)
        return None

    def prepareTableForQuery(self, table):
        """
        Prepares a table for query by the updater.

        Args:
            table (str): The name of the table to prepare for query.

        Returns:
            Any: The result of the preparation, or None if no updater is
            available.
        """
        if self._updater:
            return self._updater.prepareTableForQuery(table)
        return None
