# database_query_mixin.py
import itertools


class DatabaseQueryMixin:
    """
    ...
    """

    ##################################################
    # snp data retrieval

    def generateCurrentRSesByRSes(self, rses, tally=None):
        """
        Generates current RS IDs by merging RS IDs from the database.

        Args:
            rses (list): A list of tuples, where each tuple contains
                (rsMerged, extra).
            tally (dict, optional): A dictionary to store tally counts for
                'merge' and 'match'. Defaults to None.

        Yields:
                tuple: A tuple containing (rsMerged, extra, rsCurrent).
        """
        # rses=[ (rsInput,extra), ... ]
        # tally=dict()
        # yield:[ (rsInput,extra,rsCurrent), ... ]
        sql = """
            SELECT i.rsMerged, i.extra,
            COALESCE(sm.rsCurrent, i.rsMerged) AS rsCurrent
            FROM (SELECT ? AS rsMerged, ? AS extra) AS i
            LEFT JOIN `db`.`snp_merge` AS sm USING (rsMerged)
            """
        with self._db:
            if tally is not None:
                numMerge = numMatch = 0
                for row in self._db.cursor().executemany(sql, rses):
                    if row[2] != row[0]:
                        numMerge += 1
                    else:
                        numMatch += 1
                    yield row
                tally["merge"] = numMerge
                tally["match"] = numMatch
            else:
                for row in self._db.cursor().executemany(sql, rses):
                    yield row

    # generateCurrentRSesByRSes()

    def generateSNPLociByRSes(
        self,
        rses,
        minMatch=1,
        maxMatch=1,
        validated=None,
        tally=None,
        errorCallback=None,
    ):
        """
        Generates SNP loci by RS IDs from the database.

        Args:
            rses (list): A list of tuples, where each tuple contains
                (rs, extra).
            minMatch (int, optional): Minimum number of matches required.
                Defaults to 1.
            maxMatch (int, optional): Maximum number of matches allowed.
                Defaults to 1.
            validated (bool, optional): Flag to filter validated SNP loci.
                Defaults to None.
            tally (dict, optional): A dictionary to store tally counts for
                'zero', 'one', and 'many'. Defaults to None.
            errorCallback (callable, optional): A callable function for error
                handling. Defaults to None.

        Yields:
            tuple: A tuple containing (rs, extra, chr, pos) for each SNP locus.
        """
        # rses=[ (rs,extra), ... ]
        # tally=dict()
        # yield:[ (rs,extra,chr,pos), ... ]
        sql = """
            SELECT i.rs, i.extra, sl.chr, sl.pos
            FROM (SELECT ? AS rs, ? AS extra) AS i
            LEFT JOIN `db`.`snp_locus` AS sl
            ON sl.rs = i.rs
            ORDER BY sl.chr, sl.pos
            """
        if validated is not None:
            sql += "  AND sl.validated = %d" % (1 if validated else 0)

        minMatch = int(minMatch) if (minMatch is not None) else 0
        maxMatch = int(maxMatch) if (maxMatch is not None) else None
        tag = matches = None
        n = numZero = numOne = numMany = 0
        with self._db:
            for row in itertools.chain(
                self._db.cursor().executemany(sql, rses),
                [(None, None, None, None)],  # noqa E501
            ):
                if tag != row[0:2]:
                    if tag:
                        if not matches:
                            numZero += 1
                        elif len(matches) == 1:
                            numOne += 1
                        else:
                            numMany += 1

                        if (
                            minMatch
                            <= len(matches)
                            <= (
                                maxMatch
                                if (maxMatch is not None)
                                else len(matches)  # noqa E501
                            )  # noqa E501
                        ):
                            for match in matches or [tag + (None, None)]:
                                yield match
                        elif errorCallback:
                            errorCallback(
                                "\t".join((t or "") for t in tag),
                                "%s match%s at index %d"
                                % (
                                    (len(matches) or "no"),
                                    ("" if len(matches) == 1 else "es"),
                                    n,
                                ),
                            )
                    tag = row[0:2]
                    matches = list()
                    n += 1
                if row[2] and row[3]:
                    matches.append(row)
            # foreach row
        if tally is not None:
            tally["zero"] = numZero
            tally["one"] = numOne
            tally["many"] = numMany

    # generateSNPLociByRSes()

    ##################################################
    # biopolymer data retrieval

    def generateBiopolymersByIDs(self, ids):
        """
        Generates biopolymers by their IDs from the database.

        Args:
            ids (list): A list of tuples, where each tuple contains
                (id, extra).

        Yields:
            tuple: A tuple containing (biopolymer_id, extra, type_id, label,
                description) for each biopolymer.
        """
        # ids=[ (id,extra), ... ]
        # yield:[ (id,extra,type_id,label,description), ... ]
        sql = "SELECT biopolymer_id, ?2 AS extra, type_id, label, description FROM `db`.`biopolymer` WHERE biopolymer_id = ?1"  # noqa E501
        return self._db.cursor().executemany(sql, ids)

    # generateBiopolymersByIDs()

    def _lookupBiopolymerIDs(
        self, typeID, identifiers, minMatch, maxMatch, tally, errorCallback
    ):
        """
        Looks up biopolymer IDs based on identifiers from the database.

        Args:
            typeID (int or Falseish): Type ID of the biopolymer, or Falseish
                for any type.
            identifiers (list): A list of tuples, where each tuple contains
                (namespace, name, extra).
            minMatch (int or Falseish): Minimum number of matches required, or
                Falseish for none.
            maxMatch (int or Falseish): Maximum number of matches allowed, or
                Falseish for none.
            tally (dict or None): A dictionary to store tally counts for
                'zero', 'one', and 'many'. Defaults to None.
            errorCallback (callable): A callable function for error handling.

        Yields:
            tuple: A tuple containing (namespace, name, extra, id) for each
                matched biopolymer.
        """
        # typeID=int or Falseish for any
        # identifiers=[ (namespace,name,extra), ... ]
        #   namespace='' or '*' for any, '-' for labels, '=' for biopolymer_id
        # minMatch=int or Falseish for none
        # maxMatch=int or Falseish for none
        # tally=dict() or None
        # errorCallback=callable(position,input,error)
        # yields (namespace,name,extra,id)

        sql = """
            SELECT i.namespace, i.identifier, i.extra,
                COALESCE(bID.biopolymer_id,
                            bLabel.biopolymer_id,
                            bName.biopolymer_id) AS biopolymer_id
            FROM (SELECT ?1 AS namespace,
                        ?2 AS identifier,
                        ?3 AS extra) AS i
            LEFT JOIN `db`.`biopolymer` AS bID
                ON i.namespace = '='
                AND bID.biopolymer_id = 1 * i.identifier
                AND (({0} IS NULL) OR (bID.type_id = {0}))
            LEFT JOIN `db`.`biopolymer` AS bLabel
                ON i.namespace = '-'
                AND bLabel.label = i.identifier
                AND (({0} IS NULL) OR (bLabel.type_id = {0}))
            LEFT JOIN `db`.`namespace` AS n
                ON i.namespace NOT IN ('=', '-')
                AND n.namespace = COALESCE(
                    NULLIF(NULLIF(LOWER(TRIM(i.namespace)), ''), '*'),
                    n.namespace
                )
            LEFT JOIN `db`.`biopolymer_name` AS bn
                ON i.namespace NOT IN ('=', '-')
                AND bn.name = i.identifier
                AND bn.namespace_id = n.namespace_id
            LEFT JOIN `db`.`biopolymer` AS bName
                ON i.namespace NOT IN ('=', '-')
                AND bName.biopolymer_id = bn.biopolymer_id
                AND (({0} IS NULL) OR (bName.type_id = {0}))
        """.format(
            int(typeID) if typeID else "NULL"
        )

        minMatch = int(minMatch) if (minMatch is not None) else 0
        maxMatch = int(maxMatch) if (maxMatch is not None) else None
        tag = matches = None
        n = numZero = numOne = numMany = 0
        with self._db:
            for row in itertools.chain(
                self._db.cursor().executemany(sql, identifiers),
                [(None, None, None, None)],
            ):
                if tag != row[0:3]:
                    if tag:
                        if not matches:
                            numZero += 1
                        elif len(matches) == 1:
                            numOne += 1
                        else:
                            numMany += 1

                        if (
                            minMatch
                            <= len(matches)
                            <= (
                                maxMatch
                                if (maxMatch is None)
                                else len(matches)  # noqa E501
                            )  # noqa E501
                        ):
                            for match in matches or [tag + (None,)]:
                                yield match
                        elif errorCallback:
                            errorCallback(
                                "\t".join((t or "") for t in tag),
                                "%s match%s at index %d"
                                % (
                                    (len(matches) or "no"),
                                    ("" if len(matches) == 1 else "es"),
                                    n,
                                ),
                            )
                    tag = row[0:3]
                    matches = set()
                    n += 1
                if row[3]:
                    matches.add(row)
            # foreach row
        if tally is not None:
            tally["zero"] = numZero
            tally["one"] = numOne
            tally["many"] = numMany

    # _lookupBiopolymerIDs()

    def generateBiopolymerIDsByIdentifiers(
        self,
        identifiers,
        minMatch=1,
        maxMatch=1,
        tally=None,
        errorCallback=None,  # noqa E501
    ):
        """
        Retrieve biopolymer IDs based on identifiers such as namespace and
        name.

        Parameters:
        -----------
        identifiers : list of tuples
            Each tuple contains (namespace, name, extra).
        minMatch : int, optional
            Minimum number of matches allowed (default is 1).
        maxMatch : int, optional
            Maximum number of matches allowed (default is 1).
        tally : dict, optional
            Dictionary to store match counts (default is None).
        errorCallback : callable, optional
            Function to handle errors.

        Returns:
        --------
        Generator object yielding biopolymer IDs based on the given
        identifiers.
        """
        # identifiers=[ (namespace,name,extra), ... ]
        return self._lookupBiopolymerIDs(
            None, identifiers, minMatch, maxMatch, tally, errorCallback
        )

    # generateBiopolymerIDsByIdentifiers()

    def generateTypedBiopolymerIDsByIdentifiers(
        self,
        typeID,
        identifiers,
        minMatch=1,
        maxMatch=1,
        tally=None,
        errorCallback=None,
    ):
        """
        Retrieve biopolymer IDs based on identifiers with a specific type.

        Parameters:
        -----------
        typeID : int or None
            Specific type ID for filtering.
        identifiers : list of tuples
            Each tuple contains (namespace, name, extra).
        minMatch : int, optional
            Minimum number of matches allowed (default is 1).
        maxMatch : int, optional
            Maximum number of matches allowed (default is 1).
        tally : dict, optional
            Dictionary to store match counts (default is None).
        errorCallback : callable, optional
            Function to handle errors.

        Returns:
        --------
        Generator object yielding biopolymer IDs based on the given
        identifiers and type ID.
        """
        # identifiers=[ (namespace,name,extra), ... ]
        return self._lookupBiopolymerIDs(
            typeID, identifiers, minMatch, maxMatch, tally, errorCallback
        )

    # generateTypedBiopolymerIDsByIdentifiers()

    def _searchBiopolymerIDs(self, typeID, texts):
        """
        Helper method to perform text-based search for biopolymer IDs.

        Parameters:
        -----------
        typeID : int or None
            Specific type ID for filtering.
        texts : list of tuples
            Each tuple contains (text, extra).

        Yields:
        -------
        Tuples containing biopolymer IDs based on the given search criteria
        and type ID.
        """
        # texts=[ (text,extra), ... ]
        # yields (extra,label,id)

        sql = """
            SELECT ?2 AS extra, b.label, b.biopolymer_id
            FROM `db`.`biopolymer` AS b
            LEFT JOIN `db`.`biopolymer_name` AS bn USING (biopolymer_id)
            WHERE
            (
                b.label LIKE '%'||?1||'%'
                OR b.description LIKE '%'||?1||'%'
                OR bn.name LIKE '%'||?1||'%'
            )
            """

        if typeID:
            sql += (
                """
                AND b.type_id = %d
                """
                % typeID
            )
        # if typeID

        sql += """
            GROUP BY b.biopolymer_id
            """

        return self._db.cursor().executemany(sql, texts)

    # _searchBiopolymerIDs()

    def generateBiopolymerIDsBySearch(self, searches):
        """
        Retrieve biopolymer IDs based on a text-based search.

        Parameters:
        -----------
        searches : list of tuples
            Each tuple contains (text, extra).

        Returns:
        --------
        Generator object yielding biopolymer IDs based on the given search
        criteria.
        """
        # searches=[ (text,extra), ... ]
        return self._searchBiopolymerIDs(None, searches)

    # generateBiopolymerIDsBySearch()

    def generateTypedBiopolymerIDsBySearch(self, typeID, searches):
        """
        Retrieve biopolymer IDs based on a text-based search with a specific
        type.

        Parameters:
        -----------
        typeID : int or None
            Specific type ID for filtering.
        searches : list of tuples
            Each tuple contains (text, extra).

        Returns:
        --------
        Generator object yielding biopolymer IDs based on the given search
        criteria and type ID.
        """
        # searches=[ (text,extra), ... ]
        return self._searchBiopolymerIDs(typeID, searches)

    # generateTypedBiopolymerIDsBySearch()

    def generateBiopolymerNameStats(self, namespaceID=None, typeID=None):
        """
        Generate statistics on biopolymer names, including counts of unique
        and ambiguous names.

        Parameters:
        -----------
        namespaceID : int or None, optional
            Optional namespace ID filter.
        typeID : int or None, optional
            Optional type ID filter.

        Yields:
        -------
        Tuples containing statistics for biopolymer names:
                - `namespace`: Name of the namespace.
                - `names`: Total number of names.
                - `unique`: Number of unique names.
                - `ambiguous`: Number of ambiguous names.
        """
        sql = """
            SELECT
            `namespace`,
            COUNT() AS `names`,
            SUM(CASE WHEN matches = 1 THEN 1 ELSE 0 END) AS `unique`,
            SUM(CASE WHEN matches > 1 THEN 1 ELSE 0 END) AS `ambiguous`
            FROM (
            SELECT bn.namespace_id, bn.name,
            COUNT(DISTINCT bn.biopolymer_id) AS matches
            FROM `db`.`biopolymer_name` AS bn
            """

        if typeID:
            sql += (
                """
                JOIN `db`.`biopolymer` AS b
                    ON b.biopolymer_id = bn.biopolymer_id AND b.type_id = %d
                """
                % typeID
            )

        if namespaceID:
            sql += (
                """
                WHERE bn.namespace_id = %d
                """
                % namespaceID
            )

        sql += """
            GROUP BY bn.namespace_id, bn.name
            )
            JOIN `db`.`namespace` AS n USING (namespace_id)
            GROUP BY namespace_id
            """

        for row in self._db.cursor().execute(sql):
            yield row

    # generateBiopolymerNameStats()

    ##################################################
    # group data retrieval

    def generateGroupsByIDs(self, ids):
        """
        Retrieve groups based on provided group IDs.

        Parameters:
        -----------
        ids : list of tuples
            Each tuple contains (group_id, extra).

        Yields:
        -------
        Tuples containing group information:
            (group_id, extra, type_id, subtype_id, label, description)
        """
        # ids=[ (id,extra), ... ]
        # yield:[ (id,extra,type_id,subtype_id,label,description), ... ]
        sql = "SELECT group_id, ?2 AS extra, type_id, subtype_id, label, description FROM `db`.`group` WHERE group_id = ?1"  # noqa E501
        return self._db.cursor().executemany(sql, ids)

    # generateGroupsByIDs()

    def _lookupGroupIDs(
        self, typeID, identifiers, minMatch, maxMatch, tally, errorCallback
    ):
        """
        Helper method to look up group IDs based on identifiers.

        Parameters:
        -----------
        typeID : int or None
            Specific type ID for filtering.
        identifiers : list of tuples
            Each tuple contains (namespace, name, extra).
        minMatch : int or None
            Minimum number of matches allowed.
        maxMatch : int or None
            Maximum number of matches allowed.
        tally : dict or None
            Dictionary to store match counts.
        errorCallback : callable or None
            Function to handle errors.

        Yields:
        -------
        Tuples containing (namespace, name, extra, group_id).
        """
        # typeID=int or Falseish for any
        # identifiers=[ (namespace,name,extra), ... ]
        #   namespace='' or '*' for any, '-' for labels, '=' for group_id
        # minMatch=int or Falseish for none
        # maxMatch=int or Falseish for none
        # tally=dict() or None
        # errorCallback=callable(input,error)
        # yields (namespace,name,extra,id)
        sql = """
                SELECT i.namespace, i.identifier, i.extra,
                    COALESCE(gID.group_id,
                            gLabel.group_id,
                            gName.group_id) AS group_id
                FROM (SELECT ?1 AS namespace,
                    ?2 AS identifier,
                    ?3 AS extra) AS i
                LEFT JOIN `db`.`group` AS gID
                    ON i.namespace = '='
                    AND gID.group_id = 1 * i.identifier
                    AND (({0} IS NULL) OR (gID.type_id = {0}))
                LEFT JOIN `db`.`group` AS gLabel
                    ON i.namespace = '-'
                    AND gLabel.label = i.identifier
                    AND (({0} IS NULL) OR (gLabel.type_id = {0}))
                LEFT JOIN `db`.`namespace` AS n
                    ON i.namespace NOT IN ('=', '-')
                    AND n.namespace = COALESCE(
                        NULLIF(NULLIF(LOWER(TRIM(i.namespace)), ''), '*'),
                        n.namespace
                    )
                LEFT JOIN `db`.`group_name` AS gn
                    ON i.namespace NOT IN ('=', '-')
                    AND gn.name = i.identifier
                    AND gn.namespace_id = n.namespace_id
                LEFT JOIN `db`.`group` AS gName
                    ON i.namespace NOT IN ('=', '-')
                    AND gName.group_id = gn.group_id
                    AND (({0} IS NULL) OR (gName.type_id = {0}))
        """.format(
            int(typeID) if typeID else "NULL"
        )

        minMatch = int(minMatch) if (minMatch is not None) else 0
        maxMatch = int(maxMatch) if (maxMatch is not None) else None
        tag = matches = None
        n = numZero = numOne = numMany = 0
        with self._db:
            for row in itertools.chain(
                self._db.cursor().executemany(sql, identifiers),
                [(None, None, None, None)],
            ):
                if tag != row[0:3]:
                    if tag:
                        if not matches:
                            numZero += 1
                        elif len(matches) == 1:
                            numOne += 1
                        else:
                            numMany += 1

                        if (
                            minMatch
                            <= len(matches)
                            <= (
                                maxMatch
                                if (maxMatch is not None)
                                else len(matches)  # noqa E501
                            )  # noqa E501
                        ):
                            for match in matches or [tag + (None,)]:
                                yield match
                        elif errorCallback:
                            errorCallback(
                                "\t".join((t or "") for t in tag),
                                "%s match%s at index %d"
                                % (
                                    (len(matches) or "no"),
                                    ("" if len(matches) == 1 else "es"),
                                    n,
                                ),
                            )
                    tag = row[0:3]
                    matches = set()
                    n += 1
                if row[3]:
                    matches.add(row)
            # foreach row
        if tally is not None:
            tally["zero"] = numZero
            tally["one"] = numOne
            tally["many"] = numMany

    # _lookupGroupIDs()

    def generateGroupIDsByIdentifiers(
        self,
        identifiers,
        minMatch=1,
        maxMatch=1,
        tally=None,
        errorCallback=None,  # noqa E501
    ):
        """
        Generate group IDs based on identifiers such as namespace and name.

        Parameters:
        -----------
        identifiers : list of tuples
            Each tuple contains (namespace, name, extra).
        minMatch : int, optional
            Minimum number of matches allowed (default is 1).
        maxMatch : int, optional
            Maximum number of matches allowed (default is 1).
        tally : dict, optional
            Dictionary to store match counts (default is None).
        errorCallback : callable, optional
            Function to handle errors.

        Yields:
        -------
        Tuples containing (namespace, name, extra, group_id).
        """
        # identifiers=[ (namespace,name,extra), ... ]
        return self._lookupGroupIDs(
            None, identifiers, minMatch, maxMatch, tally, errorCallback
        )

    # generateGroupIDsByIdentifiers()

    def generateTypedGroupIDsByIdentifiers(
        self,
        typeID,
        identifiers,
        minMatch=1,
        maxMatch=1,
        tally=None,
        errorCallback=None,
    ):
        """
        Generate group IDs based on identifiers with a specific type.

        Parameters:
        -----------
        typeID : int
            Specific type ID for filtering.
        identifiers : list of tuples
            Each tuple contains (namespace, name, extra).
        minMatch : int, optional
            Minimum number of matches allowed (default is 1).
        maxMatch : int, optional
            Maximum number of matches allowed (default is 1).
        tally : dict, optional
            Dictionary to store match counts (default is None).
        errorCallback : callable, optional
            Function to handle errors.

        Yields:
        -------
        Tuples containing (namespace, name, extra, group_id).
        """

        # identifiers=[ (namespace,name,extra), ... ]
        return self._lookupGroupIDs(
            typeID, identifiers, minMatch, maxMatch, tally, errorCallback
        )

    # generateTypedGroupIDsByIdentifiers()

    def _searchGroupIDs(self, typeID, texts):
        """
        Helper method to perform text-based search for group IDs.

        Parameters:
        -----------
        typeID : int or None
            Specific type ID for filtering.
        texts : list of tuples
            Each tuple contains (text, extra).

        Yields:
        -------
        Tuples containing group IDs based on the given search criteria and
        type ID.
        """
        # texts=[ (text,extra), ... ]
        # yields (extra,label,id)

        sql = """
            SELECT ?2 AS extra, g.label, g.group_id
            FROM `db`.`group` AS g
            LEFT JOIN `db`.`group_name` AS gn USING (group_id)
            WHERE
            (
                g.label LIKE '%'||?1||'%'
                OR g.description LIKE '%'||?1||'%'
                OR gn.name LIKE '%'||?1||'%'
            )
            """

        if typeID:
            sql += (
                """
                AND g.type_id = %d
                """
                % typeID
            )
        # if typeID

        sql += """
            GROUP BY g.group_id
            """

        return self._db.cursor().executemany(sql, texts)

    # _searchGroupIDs()

    def generateGroupIDsBySearch(self, searches):
        """
        Retrieve group IDs based on a text-based search.

        Parameters:
        -----------
        searches : list of tuples
            Each tuple contains (text, extra).

        Yields:
        -------
        Tuples containing group IDs based on the given search criteria.
            (extra, label, group_id)
        """
        # searches=[ (text,extra), ... ]
        return self._searchGroupIDs(None, searches)

    # generateGroupIDsBySearch()

    def generateTypedGroupIDsBySearch(self, typeID, searches):
        """
        Retrieve group IDs based on a text-based search with a specific type.

        Parameters:
        -----------
        typeID : int
            Specific type ID for filtering.
        searches : list of tuples
            Each tuple contains (text, extra).

        Yields:
        -------
        Tuples containing group IDs based on the given search criteria and
            type ID. (extra, label, group_id)
        """
        # searches=[ (text,extra), ... ]
        return self._searchGroupIDs(typeID, searches)

    # generateTypedGroupIDsBySearch()

    def generateGroupNameStats(self, namespaceID=None, typeID=None):
        """
        Generate statistics on group names.

        Parameters:
        -----------
        namespaceID : int or None, optional
            Namespace ID for filtering (default is None).
        typeID : int or None, optional
            Specific type ID for filtering (default is None).

        Yields:
        -------
        Tuples containing statistics on group names:
            (namespace, names, unique, ambiguous)
        """
        sql = """
            SELECT
            `namespace`,
            COUNT() AS `names`,
            SUM(CASE WHEN matches = 1 THEN 1 ELSE 0 END) AS `unique`,
            SUM(CASE WHEN matches > 1 THEN 1 ELSE 0 END) AS `ambiguous`
            FROM (
            SELECT gn.namespace_id, gn.name,
            COUNT(DISTINCT gn.group_id) AS matches
            FROM `db`.`group_name` AS gn
            """

        if typeID:
            sql += (
                """
                JOIN `db`.`group` AS g
                    ON g.group_id = gn.group_id AND g.type_id = %d
                """
                % typeID
            )

        if namespaceID:
            sql += (
                """
                WHERE gn.namespace_id = %d
                """
                % namespaceID
            )

        sql += """
            GROUP BY gn.namespace_id, gn.name
            )
            JOIN `db`.`namespace` AS n USING (namespace_id)
            GROUP BY namespace_id
            """

        for row in self._db.cursor().execute(sql):
            yield row

    # generateGroupNameStats()
