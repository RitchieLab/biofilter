# #################################################
# LOCUS/POSITION INPUT MIXIN
# #################################################


class LocusPositionInputMixin:
    """
    The `LocusPositionInputMixin` class provides methods for handling locus
    positions within the database, supporting union and intersection
    operations on locus data. These operations are useful for managing large
    datasets, allowing the user to combine or reduce locus positions based
    on specific criteria.

    IMPLEMENTED METHODS
    - [unionInputLoci]:
        Combines (unions) a list of locus positions with the existing
        positions in the specified database table, adding new entries as
        needed.
    - [intersectInputLoci]:
        Reduces the locus positions in the specified database table to only
        those present in the provided list.

    This mixin allows for efficient data management of locus positions, with
    options for either expanding (union) or reducing (intersection) the data
    set as needed.
    """

    def unionInputLoci(self, db, loci, errorCallback=None):
        """
        Adds a list of loci (chromosomal positions) to a position filter
        within a specific database table.
        Uses an `INSERT OR IGNORE` statement to proceed without interruption
        in cases of missing or invalid data.

        Parameters:
        - `db`: Name of the database where the `locus` table is located.
        - `loci`: List of tuples `[(label, chr, pos, extra), ...]`, where
        `label` is the locus label, `chr` is the chromosome, `pos` is the
        position on the chromosome, and `extra` contains additional inform.
        - `errorCallback`: Optional function to be called in case of errors
        with the locus input data.

        Operation:
        - Logs the start of the locus addition process to the position filter.
        - Calls `prepareTableForUpdate` to remove indexes from the `locus`
        table, improving insertion efficiency.
        - Defines an `INSERT OR IGNORE` SQL query to insert each locus into
        the `locus` table of the database.
        - Uses `OR IGNORE` to avoid interruptions in cases of invalid or
        missing data.
        - Executes the query for each duplicated input pair in `loci`
        (required for `SELECT LAST_INSERT_ROWID()` use).
        - Tracks the counts of:
        - `numAdd`: Number of valid loci added.
        - `numNull`: Number of loci ignored due to invalid data.
        - Logs a warning if invalid data is encountered and logs the final
        count of loci added.
        - Increments the `locus` filter counter in the database.

        Returns:
        - None. The method inserts loci into the specified table and logs
        information about the insertion process.

        This function is useful for managing and adding loci to a database
        table, applying a resilient insertion process that gracefully handles
        missing or incorrect data.
        """
        # loci=[ (label,chr,pos,extra), ... ]
        self.logPush("adding to %s position filter ...\n" % db)
        cursor = self._loki._db.cursor()

        # use OR IGNORE to continue on data error, i.e. missing chr or pos
        self.prepareTableForUpdate(db, "locus")
        sql = (
            "INSERT OR IGNORE INTO `%s`.`locus` (label,chr,pos,extra) VALUES (?1,?2,?3,?4); SELECT LAST_INSERT_ROWID(),?1,?2,?3,?4"  # noqa E501
            % db
        )
        n = lastID = numAdd = numNull = 0
        for row in cursor.executemany(sql, (2 * locus for locus in loci)):
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
            self.warn("WARNING: ignored %d invalid positions\n" % numNull)
        self.logPop("... OK: added %d positions\n" % numAdd)

        self._inputFilters[db]["locus"] += 1

    def intersectInputLoci(self, db, loci, errorCallback=None):
        """
        Reduces a locus filter in a specific database table by retaining only
        loci present in a provided list.
        If the `locus` filter is not initialized, performs a union instead of
        an intersection.

        Parameters:
        - db: Name of the database where the `locus` table is located.
        - loci: List of tuples `[(label, chr, pos, extra), ...]`, where
        `label` is the locus label, `chr` is the chromosome, `pos` is the
        position on the chromosome, and `extra` holds additional information.
        - errorCallback: Optional function invoked if an error occurs during
        loci processing.

        Operation:
        - If the `locus` filter is uninitialized (`_inputFilters[db]['locus']`
        is 0), calls `unionInputLoci` to create the filter with all provided
        loci.
        - Otherwise:
        - Logs the start of the locus filter reduction process.
        - Calls `prepareTableForQuery` to ensure the `locus` table is ready
        for queries.
        - Sets all loci in the table to "not retained" (`flag = 0`).
        - Counts the number of loci before reduction (`numBefore`).
        - Updates the `locus` table, setting `flag = 1` for loci matching the
        chromosomes (`chr`) and positions (`pos`) from the provided list.
        - Deletes loci not in the provided list (`flag = 0`).
        - Counts the number of loci removed (`numDrop`).
        - Logs the final count of retained loci and the number of discarded
        loci.
        - Increments the `locus` filter counter for the database.

        Returns:
        - None. The method intersects loci in the specified table and logs the
        outcome.

        This method enables management of a set of loci in a database table by
        performing an intersection with a provided list, retaining only
        matches.
        """

        # loci=[ (label,chr,pos,extra), ... ]
        if not self._inputFilters[db]["locus"]:
            return self.unionInputLoci(db, loci, errorCallback)
        self.logPush("reducing %s position filter ...\n" % db)
        cursor = self._loki._db.cursor()

        self.prepareTableForQuery(db, "locus")
        cursor.execute("UPDATE `%s`.`locus` SET flag = 0" % db)
        numBefore = cursor.getconnection().changes()
        sql = (
            "UPDATE `%s`.`locus` SET flag = 1 WHERE (1 OR ?1) AND chr = ?2 AND pos = ?3 AND (1 OR ?4)"  # noqa E501
            % db
        )
        cursor.executemany(sql, loci)
        cursor.execute("DELETE FROM `%s`.`locus` WHERE flag = 0" % db)
        numDrop = self._loki._db.changes()
        self.logPop(
            "... OK: kept %d positions (%d dropped)\n"
            % (numBefore - numDrop, numDrop)  # noqa E501
        )

        self._inputFilters[db]["locus"] += 1
