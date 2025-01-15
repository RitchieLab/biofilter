# #################################################
# DATABASE MANAGEMENT MIXIN
# #################################################
import apsw


class DatabaseSchemaMixin:

    def createDatabaseObjects(
        self,
        schema,
        dbName,
        tblList=None,
        doTables=True,
        idxList=None,
        doIndecies=True,  # noqa: E501
    ):
        """
        Creates tables and indices in the database based on the provided
        schema.

        Args:
            schema (dict): The schema definition for the database objects.
            dbName (str): The name of the database to create objects in.
            tblList (list, optional): List of tables to create. Defaults to
                None, which creates all tables in the schema.
            doTables (bool, optional): If True, creates tables. Defaults to
                True.
            idxList (list, optional): List of indices to create. Defaults to
                None, which creates all indices in the schema.
            doIndecies (bool, optional): If True, creates indices. Defaults to
                True.

        The function creates the specified tables and indices, inserting
            initial data if provided in the schema.
        """
        cursor = self._db.cursor()
        schema = schema or self._schema[dbName]
        dbType = "TEMP " if (dbName == "temp") else ""
        if tblList and isinstance(tblList, str):
            tblList = (tblList,)
        if idxList and isinstance(idxList, str):
            idxList = (idxList,)
        for tblName in tblList or schema.keys():
            if doTables:
                cursor.execute(
                    "CREATE %sTABLE IF NOT EXISTS `%s`.`%s` %s"
                    % (dbType, dbName, tblName, schema[tblName]["table"])
                )
                if "data" in schema[tblName] and schema[tblName]["data"]:
                    sql = "INSERT OR IGNORE INTO `%s`.`%s` VALUES (%s)" % (
                        dbName,
                        tblName,
                        ("?," * len(schema[tblName]["data"][0]))[:-1],
                    )
                    # TODO: change how 'data' is defined so it can be tested
                    # without having to try inserting
                    try:
                        cursor.executemany(sql, schema[tblName]["data"])
                    except apsw.ReadOnlyError:
                        pass
            if doIndecies:
                for idxName in idxList or schema[tblName]["index"].keys():
                    if idxName not in schema[tblName]["index"]:
                        raise Exception(
                            "ERROR: no definition for index '%s' on table '%s'"
                            % (idxName, tblName)
                        )
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS `%s`.`%s` ON `%s` %s"
                        % (
                            dbName,
                            idxName,
                            tblName,
                            schema[tblName]["index"][idxName],
                        )  # noqa: E501
                    )
                # foreach idxName in idxList
                cursor.execute("ANALYZE `%s`.`%s`" % (dbName, tblName))
        # foreach tblName in tblList

        # this shouldn't be necessary since we don't manually modify the
        # sqlite_stat* tables
        # if doIndecies:
        # 	cursor.execute("ANALYZE `%s`.`sqlite_master`" % (dbName,))

    # createDatabaseObjects()

    def createDatabaseTables(self, schema, dbName, tblList, doIndecies=False):
        """
        Creates tables in the database based on the provided schema.

        Args:
            schema (dict): The schema definition for the database objects.
            dbName (str): The name of the database to create tables in.
            tblList (list): List of tables to create.
            doIndecies (bool, optional): If True, creates indices.
                Defaults to False.

        The function creates the specified tables and optionally creates
            indices for them.
        """
        return self.createDatabaseObjects(
            schema, dbName, tblList, True, None, doIndecies
        )

    # createDatabaseTables()

    def createDatabaseIndices(
        self, schema, dbName, tblList, doTables=False, idxList=None
    ):
        """
        Creates indices in the database based on the provided schema.

        Args:
            schema (dict): The schema definition for the database objects.
            dbName (str): The name of the database to create indices in.
            tblList (list): List of tables to create indices for.
            doTables (bool, optional): If True, creates tables as well.
                Defaults to False.
            idxList (list, optional): List of indices to create. Defaults to
                None, which creates all indices in the schema.

        The function creates the specified indices and optionally creates
            tables for them.
        """
        return self.createDatabaseObjects(
            schema, dbName, tblList, doTables, idxList, True
        )

    # createDatabaseIndices()

    def dropDatabaseObjects(
        self,
        schema,
        dbName,
        tblList=None,
        doTables=True,
        idxList=None,
        doIndecies=True,  # noqa: E501
    ):
        """
        Drops tables and indices in the database based on the provided schema.

        Args:
            schema (dict): The schema definition for the database objects.
            dbName (str): The name of the database to drop objects from.
            tblList (list, optional): List of tables to drop. Defaults to None,
                which drops all tables in the schema.
            doTables (bool, optional): If True, drops tables. Defaults to True.
            idxList (list, optional): List of indices to drop. Defaults to
                None, which drops all indices in the schema.
            doIndecies (bool, optional): If True, drops indices. Defaults to
                True.

        The function drops the specified tables and indices from the database.
        """
        cursor = self._db.cursor()
        schema = schema or self._schema[dbName]
        if tblList and isinstance(tblList, str):
            tblList = (tblList,)
        if idxList and isinstance(idxList, str):
            idxList = (idxList,)
        for tblName in tblList or schema.keys():
            if doTables:
                cursor.execute(
                    "DROP TABLE IF EXISTS `%s`.`%s`" % (dbName, tblName)
                )  # noqa: E501
            elif doIndecies:
                for idxName in idxList or schema[tblName]["index"].keys():
                    cursor.execute(
                        "DROP INDEX IF EXISTS `%s`.`%s`" % (dbName, idxName)
                    )  # noqa: E501
                # foreach idxName in idxList
        # foreach tblName in tblList

    # dropDatabaseObjects()

    def dropDatabaseTables(self, schema, dbName, tblList):
        """
        Drops tables in the database based on the provided schema.

        Args:
            schema (dict): The schema definition for the database objects.
            dbName (str): The name of the database to drop tables from.
            tblList (list): List of tables to drop.

        The function drops the specified tables from the database.
        """
        return self.dropDatabaseObjects(
            schema, dbName, tblList, True, None, True
        )  # noqa: E501

    # dropDatabaseTables()

    def dropDatabaseIndices(self, schema, dbName, tblList, idxList=None):
        """
        Drops indices in the database based on the provided schema.

        Args:
            schema (dict): The schema definition for the database objects.
            dbName (str): The name of the database to drop indices from.
            tblList (list): List of tables to drop indices for.
            idxList (list, optional): List of indices to drop. Defaults to
                None, which drops all indices in the schema.

        The function drops the specified indices from the database.
        """
        return self.dropDatabaseObjects(
            schema, dbName, tblList, False, idxList, True
        )  # noqa: E501

    # dropDatabaseIndices()

    def updateDatabaseSchema(self):
        """
        Updates the database schema to the latest version.

        The function checks the current schema version and applies necessary
        updates to bring it to the latest version.
        It logs the progress and results of each update step.

        Raises:
            Exception: If an error occurs during the schema update process.
        """
        cursor = self._db.cursor()

        if self.getDatabaseSetting("schema", int) < 2:
            self.logPush("updating database schema to version 2 ...\n")
            updateMap = {
                "snp_merge": "rsMerged,rsCurrent,source_id",
                "snp_locus": "rs,chr,pos,validated,source_id",
                "snp_entrez_role": "rs,entrez_id,role_id,source_id",
                "snp_biopolymer_role": "rs,biopolymer_id,role_id,source_id",
            }
            for tblName, tblColumns in updateMap.iteritems():
                self.log("%s ..." % (tblName,))
                cursor.execute(
                    "ALTER TABLE `db`.`%s` RENAME TO `___old_%s___`"
                    % (tblName, tblName)
                )
                self.createDatabaseTables(None, "db", tblName)
                cursor.execute(
                    "INSERT INTO `db`.`%s` (%s) SELECT %s FROM `db`.`___old_%s___`"  # noqa: E501
                    % (tblName, tblColumns, tblColumns, tblName)
                )
                cursor.execute("DROP TABLE `db`.`___old_%s___`" % (tblName,))
                self.createDatabaseIndices(None, "db", tblName)
                self.log(" OK\n")
            self.setDatabaseSetting("schema", 2)
            self.logPop("... OK\n")
        # schema<2

        if self.getDatabaseSetting("schema", int) < 3:
            self.log("updating database schema to version 3 ...")
            self.setDatabaseSetting(
                "optimized", self.getDatabaseSetting("finalized", int)
            )
            self.setDatabaseSetting("schema", 3)
            self.log(" OK\n")
        # schema<3

    # updateDatabaseSchema()

    def auditDatabaseObjects(
        self,
        schema,
        dbName,
        tblList=None,
        doTables=True,
        idxList=None,
        doIndecies=True,
        doRepair=True,
    ):
        """
        Audits the database objects against the provided schema and repairs
            discrepancies if specified.

        Args:
            schema (dict, optional): The schema definition for the database
                objects. Defaults to None, which uses the internal schema.
            dbName (str): The name of the database to audit.
            tblList (list, optional): List of tables to audit. Defaults to
                None, which audits all tables in the schema.
            doTables (bool, optional): If True, audits tables. Defaults to
                True.
            idxList (list, optional): List of indices to audit. Defaults to
                None, which audits all indices in the schema.
            doIndecies (bool, optional): If True, audits indices. Defaults to
                True.
            doRepair (bool, optional): If True, repairs discrepancies.
                Defaults to True.

        Returns:
            bool: True if the audit is successful and all objects match the
                schema, False otherwise.

        The function fetches the current database schema, compares it with the
            provided schema, and repairs any discrepancies if specified.
        It logs warnings and errors for mismatches and repairs.
        """
        # fetch current schema
        cursor = self._db.cursor()
        current = dict()
        dbMaster = (
            "`sqlite_temp_master`"
            if (dbName == "temp")
            else ("`%s`.`sqlite_master`" % (dbName,))
        )
        sql = (
            "SELECT tbl_name,type,name,COALESCE(sql,'') FROM %s WHERE type IN ('table','index')"  # noqa: E501
            % (dbMaster,)
        )
        for row in cursor.execute(sql):
            tblName, objType, idxName, objDef = row
            if tblName not in current:
                current[tblName] = {"table": None, "index": {}}
            if objType == "table":
                current[tblName]["table"] = " ".join(objDef.strip().split())
            elif objType == "index":
                current[tblName]["index"][idxName] = " ".join(
                    objDef.strip().split()
                )  # noqa: E501
        tblEmpty = dict()
        sql = None
        for tblName in current:
            tblEmpty[tblName] = True
            sql = "SELECT 1 FROM `%s`.`%s` LIMIT 1" % (dbName, tblName)
            for row in cursor.execute(sql):
                tblEmpty[tblName] = False
        # audit requested objects
        schema = schema or self._schema[dbName]
        if tblList and isinstance(tblList, str):
            tblList = (tblList,)
        if idxList and isinstance(idxList, str):
            idxList = (idxList,)
        ok = True
        for tblName in tblList or schema.keys():
            if doTables:
                if tblName in current:
                    if current[tblName]["table"] == (
                        "CREATE TABLE `%s` %s"
                        % (
                            tblName,
                            " ".join(schema[tblName]["table"].strip().split()),
                        )  # noqa: E501
                    ):
                        if (
                            "data" in schema[tblName]
                            and schema[tblName]["data"]  # noqa: E501
                        ):  # noqa: E501
                            sql = (
                                "INSERT OR IGNORE INTO `%s`.`%s` VALUES (%s)"
                                % (  # noqa: E501
                                    dbName,
                                    tblName,
                                    ("?," * len(schema[tblName]["data"][0]))[
                                        :-1
                                    ],  # noqa: E501
                                )
                            )
                            # TODO: change how 'data' is defined so it can be
                            # tested without having to try inserting
                            try:
                                cursor.executemany(
                                    sql, schema[tblName]["data"]
                                )  # noqa: E501
                            except apsw.ReadOnlyError:
                                pass
                    elif doRepair and tblEmpty[tblName]:
                        self.log(
                            "WARNING: table '%s' schema mismatch -- repairing ..."  # noqa: E501
                            % tblName
                        )
                        self.dropDatabaseTables(schema, dbName, tblName)
                        self.createDatabaseTables(schema, dbName, tblName)
                        current[tblName]["index"] = dict()
                        self.log(" OK\n")
                    elif doRepair:
                        self.log(
                            "ERROR: table '%s' schema mismatch -- cannot repair\n"  # noqa: E501
                            % tblName
                        )
                        ok = False
                    else:
                        self.log(
                            "ERROR: table '%s' schema mismatch\n" % tblName
                        )  # noqa: E501
                        ok = False
                    # if definition match
                elif doRepair:
                    self.log(
                        "WARNING: table '%s' is missing -- repairing ..."
                        % tblName  # noqa: E501
                    )
                    self.createDatabaseTables(
                        schema, dbName, tblName, doIndecies
                    )  # noqa: E501
                    self.log(" OK\n")
                else:
                    self.log("ERROR: table '%s' is missing\n" % tblName)
                    ok = False
                # if tblName in current
            # if doTables
            if doIndecies:
                for idxName in idxList or schema[tblName]["index"].keys():
                    if (tblName not in current) and not (
                        doTables and doRepair
                    ):  # noqa: E501
                        self.log(
                            "ERROR: table '%s' is missing for index '%s'\n"
                            % (tblName, idxName)
                        )
                        ok = False
                    elif (
                        tblName in current
                        and idxName in current[tblName]["index"]  # noqa: E501
                    ):  # noqa: E501
                        if current[tblName]["index"][idxName] == (
                            "CREATE INDEX `%s` ON `%s` %s"
                            % (
                                idxName,
                                tblName,
                                " ".join(
                                    schema[tblName]["index"][idxName]
                                    .strip()
                                    .split()  # noqa: E501
                                ),
                            )
                        ):
                            pass
                        elif doRepair:
                            self.log(
                                "WARNING: index '%s' on table '%s' schema mismatch -- repairing ..."  # noqa: E501
                                % (idxName, tblName)
                            )
                            self.dropDatabaseIndices(
                                schema, dbName, tblName, idxName
                            )  # noqa: E501
                            self.createDatabaseIndices(
                                schema, dbName, tblName, False, idxName
                            )
                            self.log(" OK\n")
                        else:
                            self.log(
                                "ERROR: index '%s' on table '%s' schema mismatch\n"  # noqa: E501
                                % (idxName, tblName)
                            )
                            ok = False
                        # if definition match
                    elif doRepair:
                        self.log(
                            "WARNING: index '%s' on table '%s' is missing -- repairing ..."  # noqa: E501
                            % (idxName, tblName)
                        )
                        self.createDatabaseIndices(
                            schema, dbName, tblName, False, idxName
                        )
                        self.log(" OK\n")
                    else:
                        self.log(
                            "ERROR: index '%s' on table '%s' is missing\n"
                            % (idxName, tblName)
                        )
                        ok = False
                    # if tblName,idxName in current
                # foreach idxName in idxList
            # if doIndecies
        # foreach tblName in tblList
        return ok

    # auditDatabaseObjects()

    def finalizeDatabase(self):
        """
        Finalizes the database by discarding intermediate data and setting
            finalization flags.

        The function drops intermediate tables, recreates them, and sets the
            database settings to indicate that the database is finalized and
            not optimized.

        Returns:
                None
        """
        self.log("discarding intermediate data ...")
        self.dropDatabaseTables(
            None,
            "db",
            (
                "snp_entrez_role",
                "biopolymer_name_name",
                "group_member_name",
            ),  # noqa: E501
        )
        self.createDatabaseTables(
            None,
            "db",
            ("snp_entrez_role", "biopolymer_name_name", "group_member_name"),
            True,
        )
        self.log(" OK\n")
        self.setDatabaseSetting("finalized", 1)
        self.setDatabaseSetting("optimized", 0)

    # finalizeDatabase()

    def optimizeDatabase(self):
        """
        Optimizes the database by updating optimizer statistics and compacting
        the database file.

        The function updates the database statistics for query optimization
        and compacts the database to free up space.

        Returns:
                None
        """
        self._db.cursor().execute("ANALYZE `db`")
        self.log("updating optimizer statistics completed\n")
        self.defragmentDatabase()
        self.setDatabaseSetting("optimized", 1)
        self.log("compacting knowledge database file completed\n")

    # optimizeDatabase()

    def defragmentDatabase(self):
        """
        Defragments the database to compact it and free up space.

        The function detaches the current database file, performs a VACUUM
        operation to compact it, and then re-attaches the database file.

        Returns:
                None
        """
        # unfortunately sqlite's VACUUM doesn't work on attached databases,
        # so we have to detach, make a new direct connection, then re-attach
        if self._dbFile:
            dbFile = self._dbFile
            self.detachDatabaseFile(quiet=True)
            db = apsw.Connection(dbFile)
            db.cursor().execute("VACUUM")
            db.close()
            self.attachDatabaseFile(dbFile, quiet=True)

    # defragmentDatabase()
