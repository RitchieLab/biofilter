# #################################################
# DATABASE MANAGEMENT MIXIN
# #################################################
import sys


class DatabaseManagementMixin:
    """
    The `DatabaseManagementMixin` class provides essential methods for managing
    and interacting with the underlying database in a bioinformatics system.
    Designed to be integrated as a mixin, it supplements the main class with
    database-focused functionalities, particularly useful for managing tables,
    checking data presence, and handling temporary updates to database indexes.

    MAIN FUNCTIONALITY:
    - Database Attachment: Support for attaching external database files.
    - Table Preparation: Functions for preparing tables for querying and
    updates, including temporary index removal to enhance insertion
    performance.
    - Data Presence Checking: Verify if specific tables contain data.
    - Zone Updates: Specialized method to update and manage region zones for
    efficient query processing in regional data.

    IMPLEMENTED METHODS:
    - [attachDatabaseFile]:
        Attaches an external database file to the current database instance.
    - [prepareTableForUpdate]:
        Prepares a specific table for updates by temporarily removing indexes
        to optimize insertion operations.
    - [prepareTableForQuery]:
        Ensures a table is ready for querying by recreating indexes if they
        were removed.
    - [tableHasData]:
        Checks if a specified table contains any data, returning a boolean
        result.
    - [updateRegionZones]:
        Updates the `region_zone` table with calculated zone coverage based on
        the configured zone size, which helps optimize regional queries.

    By abstracting these database management tasks, the mixin improves
    modularity and allows the primary class to manage complex database
    interactions more effectively.
    """

    def attachDatabaseFile(self, dbFile):
        """
        Attaches an external SQLite database file to extend or access
        additional data.

        Parameters:
        - `dbFile`: The file path of the SQLite database to be attached.

        Operation:
        - Delegates the task to `_loki.attachDatabaseFile(dbFile)`, which
        attaches the specified SQLite file to the current session, making its
        tables and data available for queries.

        Returns:
        - Result of the attachment operation from `_loki`. This allows
        seamless integration of external databases without merging data into
        the main schema.
        """
        return self._loki.attachDatabaseFile(dbFile)

    def prepareTableForUpdate(self, db, table):
        """
        Prepares a specific table for updating by temporarily removing its
        indexes to improve performance during data insertion.

        Parameters:
        - db: Name of the database where the table is located.
        - table: Name of the table to prepare for updating.

        Operation:
        - Checks if the database (`db`) and table (`table`) are defined in the
        class schema (`_schema`).
        - Verifies if the table has already been deindexed:
        - If not, adds the table to the `_tablesDeindexed[db]` set to mark its
        indexes as removed.
        - Calls `dropDatabaseIndecies` on `_loki`, passing the schema and `db`
        to drop the table's indexes.

        Returns:
        - Nothing. The method optimizes the table for bulk insertions without
        indexes.

        This method is useful for optimizing large insertion operations, as
        removing indexes temporarily can significantly improve insertion
        performance. The indexes can be recreated after all updates or
        insertions are complete.
        """
        assert (db in self._schema) and (table in self._schema[db])
        if table not in self._tablesDeindexed[db]:
            self._tablesDeindexed[db].add(table)
            self._loki.dropDatabaseIndecies(self._schema[db], db, table)

    def prepareTableForQuery(self, db, table):
        """
        Restores indexes for a specified table to prepare it for efficient
        querying.

        Parameters:
        - `db`: The name of the database where the table is located.
        - `table`: The name of the table to prepare for querying.

        Operation:
        - Checks that the specified `db` and `table` exist within the class
        schema (`_schema`).
        - If the table is marked as deindexed:
        - Removes the table from `_tablesDeindexed[db]` to mark its indexes as
        restored.
        - Calls `createDatabaseIndecies` on `_loki` to reapply the indexes for
        the table.
        - If the table is named `"region"`, calls `updateRegionZones(db)` to
        refresh region zone data.

        Returns:
        - Nothing. This method reindexes a table, optimizing it for query
        operations after bulk updates or insertions.
        """
        assert (db in self._schema) and (table in self._schema[db])
        if table in self._tablesDeindexed[db]:
            self._tablesDeindexed[db].remove(table)
            self._loki.createDatabaseIndecies(self._schema[db], db, table)
            if table == "region":
                self.updateRegionZones(db)

    def tableHasData(self, db, table):
        """
        Checks if a specified table in the given database contains any rows of
        data.

        Parameters:
        - `db`: The name of the database where the table is located.
        - `table`: The name of the table to check for data presence.

        Operation:
        - Executes a query on the specified table to count rows, limiting to
        one row for efficiency.
        - Returns `True` if any row is found, indicating the table contains
        data, otherwise returns `False`.

        Returns:
        - A boolean indicating whether the table has any data (`True` if it
        contains at least one row, `False` otherwise).

        This method provides a quick way to determine if a table is empty or
        populated without retrieving all data.
        """
        return (
            sum(
                row[0]
                for row in self._loki._biofilter.db.cursor().execute(
                    "SELECT 1 FROM `%s`.`%s` LIMIT 1" % (db, table)
                )
            )
            > 0
        )

    def updateRegionZones(self, db):
        """
        Updates the `region_zone` table in the specified database to reflect
        coverage zones for each region based on a predefined zone size.

        **Parameters**:
        - `db`: The name of the database where `region` and `region_zone`
        tables are located.

        **Operation**:
        1. **Validation**: Asserts that the database and both `region` and
        `region_zone` tables are present in the schema.
        2. **Logging**: Logs the start of the region zone update process.
        3. **Zone Size Retrieval**: Retrieves the `zone_size` setting from the
        database. If unavailable, terminates the process with an error.
        4. **Orientation Correction**: Ensures `posMin` and `posMax` in the
        `region` table are correctly ordered.
        5. **Zone Generator (`_zones` function)**: Generates zone identifiers
        based on the `zone_size`, creating unique entries for each region's
        range within a defined zone.
        6. **Zone Assignment**: Deletes existing entries in `region_zone`,
        then inserts new records generated by `_zones`, ensuring each region
        is assigned to the appropriate zones.

        **Returns**:
        - None. The method updates the `region_zone` table in place.

        This method is essential for preparing spatial data within a region by
        segmenting it into zones. Each region is divided into zones based on
        the `zone_size` parameter, optimizing spatial queries that may depend
        on regional subdivisions.

        """
        # Check db integrid
        assert (
            (db in self._schema)
            and "region" in self._schema[db]
            and "region_zone" in self._schema[db]
        )
        self.log("calculating %s region zone coverage ..." % db)
        cursor = self._loki._biofilter.db.cursor()

        size = self._loki.getDatabaseSetting("zone_size")
        if not size:
            sys.exit("ERROR: could not determine database setting 'zone_size'")
        size = int(size)

        # make sure all regions are correctly oriented
        cursor.execute(
            "UPDATE `%s`.`region` SET posMin = posMax, posMax = posMin WHERE posMin > posMax"  # noqa E501
            % db
        )

        # define zone generator
        def _zones(size, regions):
            # regions=[ (id,chr,posMin,posMax),... ]
            # yields:[ (id,chr,zone),... ]
            for rowid, chm, posMin, posMax in regions:
                for z in range(int(posMin / size), int(posMax / size) + 1):
                    yield (rowid, chm, z)

        # feed all regions through the zone generator
        # (use a separate cursor to iterate both results simultaneously)
        self.prepareTableForQuery(db, "region")
        self.prepareTableForUpdate(db, "region_zone")
        cursor.execute("DELETE FROM `%s`.`region_zone`" % db)
        cursor.executemany(
            "INSERT OR IGNORE INTO `%s`.`region_zone` (region_rowid,chr,zone) VALUES (?,?,?)"  # noqa E501
            % db,
            _zones(
                size,
                self._loki._biofilter.db.cursor().execute(
                    "SELECT rowid,chr,posMin,posMax FROM `%s`.`region`" % db
                ),
            ),
        )
        self.prepareTableForQuery(db, "region_zone")

        self._inputFilters[db]["region_zone"] = self._inputFilters[db][
            "region"
        ]  # noqa E501
        self.log(" OK\n")
