# database_operations_mixin.py


class DatabaseOperationsMixin:
    """
    Mixin for data manipulation operations (CRUD).
    """

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

    def getDatabaseSetting(self, setting, type=None):
        """
        Retrieves a specific setting value from the database.

        Args:
            setting (str): The name of the setting to retrieve.
            type (type, optional): The type to cast the setting value to.
            Defaults to None.

        Returns:
            The setting value, cast to the specified type if provided.
        """
        value = None
        if self._dbFile:
            for row in self._db.cursor().execute(
                "SELECT value FROM `db`.`setting` WHERE setting = ?",
                (setting,),  # noqa E501
            ):
                value = row[0]
        if type:
            value = type(value) if (value is not None) else type()
        return value

    # getDatabaseSetting()

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

    # setDatabaseSetting()

    def getSourceModules(self):
        """
        Retrieves the source modules available for updating the database.

        If the updater is not already initialized, it imports and initializes
        the updater module.

        Returns:
            list: A list of available source modules.
        """
        if not self._updater:
            import loki_modules.loki_updater as loki_updater

            self._updater = loki_updater.Updater(self, self._is_test)
        return self._updater.getSourceModules()

    # getSourceModules()

    def getSourceModuleVersions(self, sources=None):
        """
        Retrieves the versions of the specified source modules.

        If the updater is not already initialized, it imports and initializes
        the updater module.

        Args:
            sources (list, optional): A list of source modules to get
            versions for. Defaults to None, which retrieves versions for all
            modules.

        Returns:
            dict: A dictionary mapping source modules to their versions.
        """
        if not self._updater:
            import loki_modules.loki_updater as loki_updater

            self._updater = loki_updater.Updater(self, self._is_test)
        return self._updater.getSourceModuleVersions(sources)

    # getSourceModuleVersions()

    def getSourceModuleOptions(self, sources=None):
        """
        Retrieves the options for the specified source modules.

        If the updater is not already initialized, it imports and initializes
        the updater module.

        Args:
            sources (list, optional): A list of source modules to get options
            for. Defaults to None, which retrieves options for all modules.

        Returns:
            dict: A dictionary mapping source modules to their options.
        """
        if not self._updater:
            import loki_modules.loki_updater as loki_updater

            self._updater = loki_updater.Updater(self, self._is_test)
        return self._updater.getSourceModuleOptions(sources)

    # getSourceModuleOptions()

    def updateDatabase(
        self,
        sources=None,
        sourceOptions=None,
        cacheOnly=False,
        forceUpdate=False,  # noqa E501
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

    # updateDatabase()

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

    # prepareTableForUpdate()

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

    # prepareTableForQuery()

    ##################################################
    # metadata retrieval

    def generateGRChByUCSChg(self, ucschg):
        """
        Generates GRCh values based on a given UCSC chain identifier.

        Args:
            ucschg (str): The UCSC chain identifier.

        Returns:
            generator: A generator yielding GRCh values corresponding to
            the given UCSC chain identifier.
        """
        return (
            row[0]
            for row in self._db.cursor().execute(
                "SELECT grch FROM grch_ucschg WHERE ucschg = ?", (ucschg,)
            )
        )

    # generateGRChByUCSChg()

    def getUCSChgByGRCh(self, grch):
        """
        Retrieves the UCSC chain identifier for a given GRCh value.

        Args:
            grch (str): The GRCh value.

        Returns:
            str: The UCSC chain identifier corresponding to the given GRCh
            value, or None if not found.
        """
        ucschg = None
        for row in self._db.cursor().execute(
            "SELECT ucschg FROM grch_ucschg WHERE grch = ?", (grch,)
        ):
            ucschg = row[0]
        return ucschg

    # getUCSChgByGRCh()

    def getLDProfileID(self, ldprofile):
        """
        Retrieves the identifier for a given LD profile.

        Args:
            ldprofile (str): The LD profile name.

        Returns:
            int: The identifier of the LD profile, or None if not found.
        """
        return self.getLDProfileIDs([ldprofile])[ldprofile]

    # getLDProfileID()

    def getLDProfileIDs(self, ldprofiles):
        """
        Retrieves the identifiers for a list of LD profiles.

        Args:
            ldprofiles (list): A list of LD profile names.

        Returns:
            dict: A dictionary mapping LD profile names to their
            identifiers.
        """
        if not self._dbFile:
            return {lc: None for lc in ldprofiles}
        sql = "SELECT i.ldprofile, l.ldprofile_id FROM (SELECT ? AS ldprofile) AS i LEFT JOIN `db`.`ldprofile` AS l ON LOWER(TRIM(l.ldprofile)) = LOWER(TRIM(i.ldprofile))"  # noqa E501
        with self._db:
            ret = {
                row[0]: row[1]
                for row in self._db.cursor().executemany(sql, zip(ldprofiles))
            }
        return ret

    # getLDProfileIDs()

    def getLDProfiles(self, ldprofiles=None):
        """
        Retrieves detailed information about LD profiles.

        Args:
            ldprofiles (list, optional): A list of LD profile names.
            Defaults to None, which retrieves information for all profiles.

        Returns:
            dict: A dictionary mapping LD profile names to a tuple
            containing their identifier, description, metric, and value.
        """
        if not self._dbFile:
            return {lc: None for lc in (ldprofiles or list())}
        with self._db:
            if ldprofiles:
                sql = "SELECT i.ldprofile, l.ldprofile_id, l.description, l.metric, l.value FROM (SELECT ? AS ldprofile) AS i LEFT JOIN `db`.`ldprofile` AS l ON LOWER(TRIM(l.ldprofile)) = LOWER(TRIM(i.ldprofile))"  # noqa E501
                ret = {
                    row[0]: row[1:]
                    for row in self._db.cursor().executemany(
                        sql, zip(ldprofiles)
                    )  # noqa E501
                }
            else:
                sql = "SELECT l.ldprofile, l.ldprofile_id, l.description, l.metric, l.value FROM `db`.`ldprofile` AS l"  # noqa E501
                ret = {
                    row[0]: row[1:] for row in self._db.cursor().execute(sql)
                }  # noqa E501
        return ret

    # getLDProfiles()

    def getNamespaceID(self, namespace):
        """
        Retrieves the identifier for a given namespace.

        Args:
            namespace (str): The namespace name.

        Returns:
            int: The identifier of the namespace, or None if not found.
        """
        return self.getNamespaceIDs([namespace])[namespace]

    # getNamespaceID()

    def getNamespaceIDs(self, namespaces):
        """
        Retrieves the identifiers for a list of namespaces.

        Args:
            namespaces (list): A list of namespace names.

        Returns:
            dict: A dictionary mapping namespace names to their identifiers.
        """
        if not self._dbFile:
            return {n: None for n in namespaces}
        sql = "SELECT i.namespace, n.namespace_id FROM (SELECT ? AS namespace) AS i LEFT JOIN `db`.`namespace` AS n ON n.namespace = LOWER(i.namespace)"  # noqa E501
        with self._db:
            ret = {
                row[0]: row[1]
                for row in self._db.cursor().executemany(sql, zip(namespaces))
            }
        return ret

    # getNamespaceIDs()

    def getRelationshipID(self, relationship):
        """
        Retrieves the identifier for a given relationship.

        Args:
                relationship (str): The relationship name.

        Returns:
                int: The identifier of the relationship, or None if not found.
        """
        return self.getRelationshipIDs([relationship])[relationship]

    # getRelationshipID()

    def getRelationshipIDs(self, relationships):
        """
        Retrieves the identifiers for a list of relationships.

        Args:
            relationships (list): A list of relationship names.

        Returns:
            dict: A dictionary mapping relationship names to their identifiers.
        """
        if not self._dbFile:
            return {r: None for r in relationships}
        sql = "SELECT i.relationship, r.relationship_id FROM (SELECT ? AS relationship) AS i LEFT JOIN `db`.`relationship` AS r ON r.relationship = LOWER(i.relationship)"  # noqa E501
        with self._db:
            ret = {
                row[0]: row[1]
                for row in self._db.cursor().executemany(
                    sql, zip(relationships)
                )  # noqa E501
            }
        return ret

    # getRelationshipIDs()

    def getRoleID(self, role):
        """
        Retrieves the identifier for a given role.

        Args:
            role (str): The role name.

        Returns:
            int: The identifier of the role, or None if not found.
        """
        return self.getRoleIDs([role])[role]

    # getRoleID()

    def getRoleIDs(self, roles):
        """
        Retrieves the identifiers for a list of roles.

        Args:
            roles (list): A list of role names.

        Returns:
            dict: A dictionary mapping role names to their identifiers.
        """
        if not self._dbFile:
            return {r: None for r in roles}
        sql = "SELECT i.role, role_id FROM (SELECT ? AS role) AS i LEFT JOIN `db`.`role` AS r ON r.role = LOWER(i.role)"  # noqa E501
        with self._db:
            ret = {
                row[0]: row[1]
                for row in self._db.cursor().executemany(sql, zip(roles))  # noqa E501
            }
        return ret

    # getRoleIDs()

    def getSourceID(self, source):
        """
        Retrieves the identifier for a given data source.

        Args:
            source (str): The name of the data source.

        Returns:
            int: The identifier of the data source, or None if not found.
        """
        return self.getSourceIDs([source])[source]

    # getSourceID()

    def getSourceIDs(self, sources=None):
        """
        Retrieves the identifiers for a list of data sources.

        Args:
            sources (list, optional): A list of data source names.
            Defaults to None, which retrieves information for all sources.

        Returns:
            dict: A dictionary mapping data source names to their
            identifiers.
        """
        if not self._dbFile:
            return {s: None for s in (sources or list())}
        if sources:
            sql = "SELECT i.source, s.source_id FROM (SELECT ? AS source) AS i LEFT JOIN `db`.`source` AS s ON s.source = LOWER(i.source)"  # noqa E501
            with self._db:
                ret = {
                    row[0]: row[1]
                    for row in self._db.cursor().executemany(sql, zip(sources))
                }
        else:
            sql = "SELECT source, source_id FROM `db`.`source`"
            with self._db:
                ret = {
                    row[0]: row[1] for row in self._db.cursor().execute(sql)
                }  # noqa E501
        return ret

    # getSourceIDs()

    def getSourceIDVersion(self, sourceID):
        """
        Retrieves the version of a data source given its identifier.

        Args:
                sourceID (int): The identifier of the data source.

        Returns:
                str: The version of the data source, or None if not found.
        """
        sql = "SELECT version FROM `db`.`source` WHERE source_id = ?"
        ret = None
        with self._db:
            for row in self._db.cursor().execute(sql, (sourceID,)):
                ret = row[0]
        return ret

    # getSourceIDVersion()

    def getSourceIDOptions(self, sourceID):
        """
        Retrieves the options associated with a data source given its
        identifier.

        Args:
            sourceID (int): The identifier of the data source.

        Returns:
            dict: A dictionary mapping option names to their values for the
            given data source.
        """
        sql = "SELECT option, value FROM `db`.`source_option` WHERE source_id = ?"  # noqa E501
        with self._db:
            ret = {
                row[0]: row[1]
                for row in self._db.cursor().execute(sql, (sourceID,))  # noqa E501
            }
        return ret

    # getSourceIDOptions()

    def getSourceIDFiles(self, sourceID):
        """
        Retrieves information about files associated with a data source given
        its identifier.

        Args:
            sourceID (int): The identifier of the data source.

        Returns:
            dict: A dictionary mapping filenames to tuples containing their
            modified date, size, and md5 hash.
        """
        sql = "SELECT filename, COALESCE(modified,''), COALESCE(size,''), COALESCE(md5,'') FROM `db`.`source_file` WHERE source_id = ?"  # noqa E501
        with self._db:
            ret = {
                row[0]: tuple(row[1:])
                for row in self._db.cursor().execute(sql, (sourceID,))
            }
        return ret

    # getSourceIDFiles()

    def getTypeID(self, type):
        """
        Retrieves the identifier for a given type.

        Args:
            type (str): The name of the type.

        Returns:
            int: The identifier of the type, or None if not found.
        """
        return self.getTypeIDs([type])[type]

    # getTypeID()

    def getTypeIDs(self, types):
        """
        Retrieves the identifiers for a list of types.

        Args:
                types (list): A list of type names.

        Returns:
                dict: A dictionary mapping type names to their identifiers.
        """
        if not self._dbFile:
            return {t: None for t in types}
        sql = "SELECT i.type, t.type_id FROM (SELECT ? AS type) AS i LEFT JOIN `db`.`type` AS t ON t.type = LOWER(i.type)"  # noqa E501
        with self._db:
            ret = {
                row[0]: row[1]
                for row in self._db.cursor().executemany(sql, zip(types))  # noqa E501
            }
        return ret

    # getTypeIDs()

    def getSubtypeID(self, subtype):
        """
        Retrieves the identifier for a given subtype.

        Args:
                subtype (str): The name of the subtype.

        Returns:
                int: The identifier of the subtype, or None if not found.
        """
        return self.getSubtypeIDs([subtype])[subtype]

    # getSubtypeID()

    def getSubtypeIDs(self, subtypes):
        """
        Retrieves subtype IDs for given subtype names from the database.

        Args:
            subtypes (list): A list of subtype names.

        Returns:
            dict: A dictionary where keys are subtype names and values are
                their corresponding subtype IDs.
                If a subtype is not found in the database, its value in the
                dictionary will be None.
        """
        if not self._dbFile:
            return {t: None for t in subtypes}
        sql = "SELECT i.subtype, t.subtype_id FROM (SELECT ? AS subtype) AS i LEFT JOIN `db`.`subtype` AS t ON t.subtype = LOWER(i.subtype)"  # noqa E501
        with self._db:
            ret = {
                row[0]: row[1]
                for row in self._db.cursor().executemany(sql, zip(subtypes))
            }
        return ret

    # getSubtypeIDs()
