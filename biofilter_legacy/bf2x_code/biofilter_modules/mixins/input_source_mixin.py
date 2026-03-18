# #################################################
# SOURCE INPUT MIXIN
# #################################################


class SourceInputMixin:
    """
    Mixin class for managing source input filters in a Loki database.

    IMPLEMENTED METHODS:
    - [unionInputSources]:
        Adds a list of sources to a filter in a specific database table.
    - [intersectInputSources]:
        Reduces a filter in a specific database table, retaining only sources
        that match those in the provided list.
    """

    def unionInputSources(self, db, names, errorCallback=None):
        """
        Adds a list of sources to a filter in a specific database table,
        ignoring unrecognized or invalid sources, which can be logged using
        `errorCallback`.

        Parameters:
        - db: Name of the database where the `source` table is located.
        - names: List of source names `[name, ...]` to be added.
        - errorCallback: Optional function called to log errors for
        unrecognized sources.

        Operation:
        - Logs the start of the source addition process.
        - Calls `prepareTableForUpdate` to optimize insertion by temporarily
        removing indexes on the `source` table.
        - Defines an `INSERT OR IGNORE` SQL query to add sources to the
        `source` table, avoiding duplicates.
        - For each source name:
            - Retrieves the `sourceID` using `getSourceID` or getUserSourceID.
            - If `sourceID` is valid, inserts the name and `sourceID` into the
            `source` table.
            - Otherwise, increments `numNull` and calls `errorCallback` with
            an error message.
        - Logs a warning for unrecognized sources (`numNull`) and the total
        number of sources added (`numAdd`).
        - Increments the filter counter for `source` in the database.

        Returns:
        - None. The function inserts sources into the specified table and logs
        the insertion details.

        This function is useful for adding sources to a table filter, ensuring
        only valid sources are included and enabling error handling
        for unknown sources as needed.
        """
        # names=[ name, ... ]
        self.logPush("adding to %s source filter ...\n" % db)
        cursor = self._loki._db.cursor()

        self.prepareTableForUpdate(db, "source")
        sql = (
            "INSERT OR IGNORE INTO `%s`.`source` (label,source_id) VALUES (?1,?2)"  # noqa E501
            % db  # noqa E501
        )
        n = numAdd = numNull = 0
        for source in names:
            n += 1
            sourceID = self._loki.getSourceID(source) or self.getUserSourceID(
                source
            )  # noqa E501
            if sourceID:
                numAdd += 1
                cursor.execute(sql, (source, sourceID))
            else:
                numNull += 1
                if errorCallback:
                    errorCallback(source, "invalid source at index %d" % (n,))
        if numNull:
            self.warn(
                "WARNING: ignored %d unrecognized source identifier(s)\n"
                % numNull  # noqa E501
            )
        self.logPop("... OK: added %d sources\n" % numAdd)

        self._inputFilters[db]["source"] += 1

    def intersectInputSources(self, db, names, errorCallback=None):
        """
        Reduces the source filter in a specified database table, retaining
        only the sources present in the provided list. If the `source` filter
        is uninitialized, it performs a union instead of an intersection.

        Parameters:
        - db: Name of the database where the `source` table is located.
        - names: List of source names `[name, ...]` to retain in the filter.
        - errorCallback: Optional function called in case of an error while
        processing sources.

        Operation:
        - If the `source` filter is not yet initialized
        (`_inputFilters[db]['source']` is 0), calls `unionInputSources` to
        create the filter with all provided sources.
        - Otherwise:
        - Logs the beginning of the source filter reduction process.
        - Calls `prepareTableForQuery` to ensure the `source` table is ready
        for queries.
        - Sets all records in the `source` table as "not retained" (flag = 0`.
        - Counts the number of sources before reduction (`numBefore`).
        - For each source in the list:
            - Obtains the `sourceID` using `getSourceID` or `getUserSourceID`.
            - If `sourceID` is valid, marks the source as "retained"
            (`flag = 1`) using `source_id`.
        - Deletes sources not retained (i.e., with `flag = 0`).
        - Counts the number of sources removed (`numDrop`).
        - Logs the final count of retained sources and the number of discarded
        sources.
        - Increments the filter counter for `source` in the database.

        Returns:
        - None. The method performs the intersection of sources in the
        specified table and logs the result.

        This function is useful for managing a set of sources in a database
        table by intersecting existing records with a provided list and
        retaining only valid matches.
        """
        # names=[ name, ... ]
        if not self._inputFilters[db]["source"]:
            return self.unionInputSources(db, names, errorCallback)
        self.logPush("reducing %s source filter ...\n" % db)
        cursor = self._loki._db.cursor()

        self.prepareTableForQuery(db, "source")
        cursor.execute("UPDATE `%s`.`source` SET flag = 0" % db)
        numBefore = cursor.getconnection().changes()
        sql = "UPDATE `%s`.`source` SET flag = 1 WHERE source_id = ?1" % db
        for source in names:
            sourceID = self._loki.getSourceID(source) or self.getUserSourceID(
                source
            )  # noqa E501
            if sourceID:
                cursor.execute(sql, (sourceID,))
        cursor.execute("DELETE FROM `%s`.`source` WHERE flag = 0" % db)
        numDrop = cursor.getconnection().changes()
        self.logPop(
            "... OK: kept %d sources (%d dropped)\n"
            % (numBefore - numDrop, numDrop)  # noqa E501
        )

        self._inputFilters[db]["source"] += 1
