# #################################################
# DATABASE MANAGEMENT MIXIN
# #################################################
import apsw


class DatabaseConfigMixin:
    def getDatabaseMemoryUsage(self, resetPeak=False):
        """
        Retrieves the current and peak memory usage of the database.

        Args:
            resetPeak (bool, optional): If True, resets the peak memory
            sage after retrieving it. Defaults to False.

        Returns:
            tuple: A tuple containing the current memory usage (int) and
            the peak memory usage (int) in bytes.
        """
        return (apsw.memoryused(), apsw.memoryhighwater(resetPeak))

    # getDatabaseMemoryUsage()

    def getDatabaseMemoryLimit(self):
        """
        Retrieves the current memory limit for the database.

        Returns:
            int: The current soft heap limit in bytes.
        """
        return apsw.softheaplimit(-1)

    # getDatabaseMemoryLimit()

    def setDatabaseMemoryLimit(self, limit=0):
        """
        Sets a new memory limit for the database.

        Args:
            limit (int, optional): The new memory limit in bytes. Defaults to
            0, which sets no limit.
        """
        apsw.softheaplimit(limit)

    # setDatabaseMemoryLimit()

    def configureDatabase(self, db=None, tempMem=False):
        """
        Configures database settings for performance and behavior.

        Args:
            db (str, optional): The name of the database to configure.
            Defaults to None. tempMem (bool, optional): If True, configures
            the temporary storage to use memory. Defaults to False.

        The function sets various PRAGMA settings to optimize performance for
        typical usage scenarios.
        """
        cursor = self._db.cursor()
        db = ("%s." % db) if db else ""

        # linux VFS doesn't usually report actual disk cluster size,
        # so sqlite ends up using 1KB pages by default; we prefer 4KB
        cursor.execute("PRAGMA %spage_size = 4096" % (db,))

        # cache_size is pages if positive, kibibytes if negative;
        # seems to only affect write performance
        cursor.execute("PRAGMA %scache_size = -65536" % (db,))

        # for typical read-only usage, synchronization behavior is moot anyway,
        # and while updating we're not that worried about a power failure
        # corrupting the database file since the user could just start the
        # update over from the beginning; so, we'll take the performance gain
        cursor.execute("PRAGMA %ssynchronous = OFF" % (db,))

        # the journal isn't that big, so keeping it in memory is faster; the
        # cost is that a system crash will corrupt the database rather than
        # leaving it recoverable with the on-disk journal (a program crash
        # should be fine since sqlite rollback transactions before exiting)
        cursor.execute("PRAGMA %sjournal_mode = MEMORY" % (db,))

        # the temp store is used for all of sqlite's internal scratch space
        # needs, such as the TEMP database, indexing, etc; keeping it in memory
        # is much faster, but it can get quite large
        if tempMem and not db:
            cursor.execute("PRAGMA temp_store = MEMORY")

        # we want EXCLUSIVE while updating since the data shouldn't be read
        # until ready and we want the performance gain; for normal read usage,
        # NORMAL is better so multiple users can share a database file
        cursor.execute(
            "PRAGMA %slocking_mode = %s"
            % (db, ("EXCLUSIVE" if self._updating else "NORMAL"))
        )

    def attachTempDatabase(self, db):
        """
        Attaches a temporary database with the given name.

        Args:
            db (str): The name of the temporary database to attach.

        The function first detaches any existing temporary database with the
        same name, then attaches a new one.
        """
        cursor = self._db.cursor()

        # detach the current db, if any
        try:
            cursor.execute("DETACH DATABASE `%s`" % db)
        except apsw.SQLError as e:
            if not str(e).startswith("SQLError: no such database: "):
                raise e

        # attach a new temp db
        cursor.execute("ATTACH DATABASE '' AS `%s`" % db)
        self.configureDatabase(db)

    def attachDatabaseFile(self, dbFile, quiet=False):
        """
        Attaches a new database file and configures it.

        Args:
            dbFile (str): The path to the database file to attach.
            quiet (bool, optional): If True, suppresses log messages. Defaults
            to False.

        The function detaches any currently attached database file, then
        attaches the new one and configures it. It also establishes or audits
        the database schema.
        """
        cursor = self._db.cursor()

        # detach the current db file, if any
        if self._dbFile and not quiet:
            self.log(
                "unloading knowledge database file '%s' ..." % self._dbFile
            )  # noqa E501
        try:
            cursor.execute("DETACH DATABASE `db`")
        except apsw.SQLError as e:
            if not str(e).startswith("SQLError: no such database: "):
                raise e
        if self._dbFile and not quiet:
            self.log("unloading knowledge database file completed\n")

        # reset db info
        self._dbFile = None
        self._dbNew = None

        # attach the new db file, if any
        if dbFile:
            if not quiet:
                self.logPush(
                    "loading knowledge database file '%s' ..." % dbFile
                )  # noqa E501
            cursor.execute("ATTACH DATABASE ? AS `db`", (dbFile,))
            self._dbFile = dbFile
            self._dbNew = 0 == max(
                row[0]
                for row in cursor.execute(
                    "SELECT COUNT(1) FROM `db`.`sqlite_master`"
                )  # noqa E501
            )
            self.configureDatabase("db")

            # establish or audit database schema
            err_msg = ""
            with self._db:
                if self._dbNew:
                    self.createDatabaseObjects(None, "db")
                    ok = True
                else:
                    self.updateDatabaseSchema()
                    ok = self.auditDatabaseObjects(None, "db")
                    if not ok:
                        err_msg = "Audit of database failed"

                if ok and self._updating:
                    ok = self._checkTesting()
                    if not ok:
                        err_msg = "Testing settings dont match loaded database"

            if ok:
                if not quiet:
                    self.logPop("loading knowledge database file completed\n")
            else:
                self._dbFile = None
                self._dbNew = None
                cursor.execute("DETACH DATABASE `db`")
                if not quiet:
                    self.logPop("... ERROR (" + err_msg + ")\n")

    def detachDatabaseFile(self, quiet=False):
        """
        Detaches the currently attached database file.

        Args:
            quiet (bool,opt): If True, suppresses messages. Defaults False

        Returns:
            None
        """
        return self.attachDatabaseFile(None, quiet=quiet)

    def testDatabaseWriteable(self):
        """
        Tests if the current database file is writeable.

        Raises:
            Exception: If no database file is loaded or if the database is
            read-only.

        Returns:
            bool: True if the database file is writeable.
        """
        if self._dbFile is None:
            raise Exception("ERROR: no knowledge database file is loaded")
        try:
            if self._db.readonly("db"):
                raise Exception(
                    "ERROR: knowledge database file cannot be modified"
                )  # noqa E501
        except AttributeError:  # apsw.Connection.readonly() added in 3.7.11
            try:
                self._db.cursor().execute(
                    "UPDATE `db`.`setting` SET value = value"
                )  # noqa E501
            except apsw.ReadOnlyError:
                raise Exception(
                    "ERROR: knowledge database file cannot be modified"
                )  # noqa E501
        return True
