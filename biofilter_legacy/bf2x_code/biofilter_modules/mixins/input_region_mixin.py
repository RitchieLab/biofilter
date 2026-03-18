# #################################################
# REGION INPUT MIXIN
# #################################################


class RegionInputMixin:
    """
    Mixin class for managing region input filters in a Loki database.

    IMPLEMENTED METHODS:
    - [unionInputRegions]:
        Adds a list of chromosomal regions to a region filter in a specific
        database table.
    - [intersectInputRegions]:
        Reduces a region filter in a specific database table, retaining only
        regions that match those in the provided list.
    """

    def unionInputRegions(self, db, regions, errorCallback=None):
        """
        Adds a list of chromosomal regions to a region filter in a specific
        database table. Uses `INSERT OR IGNORE` to continue inserting even if
        data is missing or invalid.

        Parameters:
        - db: Name of the database where the `region` table is located.
        - regions: List of tuples `[(label, chr, posMin, posMax, extra), ...]`
        , where:
            - `label`: Label of the region,
            - `chr`: Chromosome identifier,
            - `posMin` and `posMax`: Position range within the region,
            - `extra`: Additional information.
        - errorCallback: Optional function called if there's an error with the
        region data input.

        Operation:
        - Logs the beginning of the process to add regions to the region
        filter.
        - Calls `prepareTableForUpdate` to temporarily remove indexes from the
        `region` table, improving insertion performance.
        - Defines an SQL `INSERT OR IGNORE` query to insert each region into
        the database's `region` table.
            - Uses `OR IGNORE` to avoid halting on invalid or missing data.
        - Executes the query for each duplicated entry in `regions` (necessary
        for `SELECT LAST_INSERT_ROWID()`).
        - Counts:
            - `numAdd`: Number of valid regions added.
            - `numNull`: Number of ignored regions due to invalid data.
        - Issues a warning if any invalid data was ignored and logs the final
        count of added regions.
        - Increments the `region` filter count in the database.

        Returns:
        - None. The method inserts regions into the specified table and logs
        information about the insertion process.

        This function is useful for managing and adding regions to a database
        table, applying an insertion process that resiliently handles missing
        or incorrect data.
        """
        # regions=[ (label,chr,posMin,posMax,extra), ... ]
        self.logPush("adding to %s region filter ...\n" % db)
        cursor = self._loki._db.cursor()

        # use OR IGNORE to continue on data error, i.e. missing chr or pos
        self.prepareTableForUpdate(db, "region")
        sql = (
            "INSERT OR IGNORE INTO `%s`.`region` (label,chr,posMin,posMax,extra) VALUES (?1,?2,?3,?4,?5); SELECT LAST_INSERT_ROWID(),?1,?2,?3,?4,?5"  # noqa
            % db
        )
        n = lastID = numAdd = numNull = 0
        for row in cursor.executemany(sql, (2 * region for region in regions)):
            n += 1
            if lastID != row[0]:
                numAdd += 1
                lastID = row[0]
            else:
                numNull += 1
                if errorCallback:
                    errorCallback(
                        "\t".join(row[1:]), "invalid data at index %d" % (n,)
                    )  # noqa E501
        if numNull:
            self.warn("WARNING: ignored %d invalid regions\n" % numNull)
        self.logPop("... OK: added %d regions\n" % numAdd)

        self._inputFilters[db]["region"] += 1

    def intersectInputRegions(self, db, regions, errorCallback=None):
        """
        Reduces a region filter in a specific database table, retaining only
        regions that match those in the provided list. If the `region` filter
        is not yet initialized, performs a union instead of an intersection.

        Parameters:
        - db: The name of the database where the `region` table is located.
        - regions: list of tuples [(label, chr, posMin, posMax, extra),]:
            - `label` is the region label,
            - `chr` represents the chromosome,
            - `posMin` and `posMax` define the regions positional range,
            - `extra` contains additional information.
        - errorCallback: Optional function called in case of errors while
        processing regions.

        Operation:
        - If the `region` filter is not yet initialized
        (`_inputFilters[db]['region']` is 0), calls `unionInputRegions`
        to create the filter with all provided regions.
        - Otherwise:
            - Logs the beginning of the region filter reduction process.
            - Calls `prepareTableForQuery` to ensure the `region` table is
            ready for querying.
            - Sets all regions in the table as "not retained" (`flag = 0`).
            - Counts the number of regions before reduction (`numBefore`).
            - Updates the `region` table, setting `flag = 1` for regions that
            match chromosome (`chr`),
            minimum position (`posMin`), and maximum position (`posMax`) in
            the provided list.
            - Deletes regions not present in the provided list
            (i.e., with `flag = 0`).
            - Counts the number of regions removed (`numDrop`).
            - Logs the final count of retained regions and the number
            discarded.
            - Increments the filter counter for `region` in the database.

        Returns:
        - None. This method performs an intersection on regions in the
        specified table and logs the result.

        This function allows for managing a set of regions in a database table
        by intersecting existing records with a provided list and retaining
        only matching entries.
        """
        # regions=[ (label,chr,posMin,posMax,extra), ... ]
        if not self._inputFilters[db]["region"]:
            return self.unionInputRegions(db, regions, errorCallback)
        self.logPush("reducing %s region filter ...\n" % db)
        cursor = self._loki._db.cursor()

        self.prepareTableForQuery(db, "region")
        cursor.execute("UPDATE `%s`.`region` SET flag = 0" % db)
        numBefore = cursor.getconnection().changes()
        sql = (
            "UPDATE `%s`.`region` SET flag = 1 WHERE (1 OR ?1) AND chr = ?2 AND posMin = ?3 AND posMax = ?4 AND (1 OR ?5)"  # noqa E501
            % db
        )
        cursor.executemany(sql, regions)
        cursor.execute("DELETE FROM `%s`.`region` WHERE flag = 0" % db)
        numDrop = cursor.getconnection().changes()
        self.logPop(
            "... OK: kept %d regions (%d dropped)\n"
            % (numBefore - numDrop, numDrop)  # noqa E501
        )

        self._inputFilters[db]["region"] += 1
