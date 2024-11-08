# #################################################
# GENE INPUT MIXIN
# #################################################


class GeneInputMixin:
    """
    Mixin class for managing gene input filters in a Loki database.

    IMPLEMENTED METHODS:
    - [unionInputGenes]:
        Adds a list of genes to a gene filter within a specific database table.
        Filters ambiguous or unrecognized genes based on provided options,
        allowing for multiple matches or ignoring ambiguous genes as specified.
    - [intersectInputGenes]:
        Reduces a gene filter in a specific database table, keeping only genes
        that are present in the provided list. If the `gene` filter is not
        initialized, it performs a union instead of an intersection.
    - [unionInputGeneSearch]:
        Adds genes to a gene filter in a specific database table based on a
        text search. This method searches for genes matching provided text
        terms and adds them to the filter.
    - [intersectInputGeneSearch]:
        Reduces a gene filter in a specific database table, retaining only
        genes that match a provided list based on text-based searches. If the
        `gene` filter has not been initialized, it performs a union rather
        than an intersection.
    """

    def unionInputGenes(self, db, names, errorCallback=None):
        """
        Adds a list of genes to a gene filter within a specific database table.
        Filters ambiguous or unrecognized genes based on provided options,
        allowing for multiple matches or ignoring ambiguous genes as specified.

        Parameters:
        - db: The database name where the `gene` table is located.
        - names: A list of tuples `[(namespace, name, extra), ...]`, where:
                - `namespace` is the gene's namespace,
                - `name` is the gene name,
                - `extra` holds additional information.
        - errorCallback: Optional function to be called in case of errors with
        gene input data.

        Operation:
        - Logs the start of the gene addition process to the filter.
        - Calls `prepareTableForUpdate` to drop indexes on the `gene` table,
        optimizing insertion efficiency.
        - Defines an `INSERT` SQL query to insert each gene into the database
        `gene` table.
        - Sets the maximum number of matches allowed for a gene (`maxMatch`)
        depending on the `allow_ambiguous_genes` option.
        - Executes the SQL query for each gene identifier generated by
        `generateTypedBiopolymerIDsByIdentifiers`.
        - Tracks:
                - `numAdd`: The number of valid genes added.
                - `tally['zero']`: The count of unrecognized genes, logged as
                a warning.
                - `tally['many']`: The count of ambiguous genes, logged as a
                warning depending on the `allow_ambiguous_genes` setting.
        - Logs the final count of genes added.
        - Increments the `gene` filter counter in the database.

        Returns:
        - None. The method inserts genes into the specified table and logs the
        insertion process details.

        This function is useful for managing and adding genes to a database
        table, applying flexible handling of ambiguous or missing data based
        on configuration settings.
        """

        # names=[ (namespace,name,extra), ... ]
        self.logPush("adding to %s gene filter ...\n" % db)
        cursor = self._loki._db.cursor()
        self.prepareTableForUpdate(db, "gene")
        sql = (
            "INSERT INTO `%s`.`gene` (label,extra,biopolymer_id) VALUES (?2,?3,?4); SELECT 1"  # noqa E501
            % db
        )
        maxMatch = None if self._options.allow_ambiguous_genes == "yes" else 1
        tally = dict()
        numAdd = 0
        for row in cursor.executemany(
            sql,
            self._loki.generateTypedBiopolymerIDsByIdentifiers(
                self.getOptionTypeID("gene"),
                names,
                minMatch=1,
                maxMatch=maxMatch,
                tally=tally,
                errorCallback=errorCallback,
            ),
        ):
            numAdd += 1
        if tally["zero"]:
            self.warn(
                "WARNING: ignored %d unrecognized gene identifier(s)\n"
                % tally["zero"]  # noqa E501
            )
        if tally["many"]:
            if self._options.allow_ambiguous_genes == "yes":
                self.warn(
                    "WARNING: added multiple results for %d ambiguous gene identifier(s)\n"  # noqa E501
                    % tally["many"]
                )
            else:
                self.warn(
                    "WARNING: ignored %d ambiguous gene identifier(s)\n"
                    % tally["many"]  # noqa E501
                )
        self.logPop("... OK: added %d genes\n" % numAdd)

        self._inputFilters[db]["gene"] += 1

    def intersectInputGenes(self, db, names, errorCallback=None):
        """
        Reduces a gene filter in a specific database table, keeping only genes
        that are present in the provided list. If the `gene` filter is not
        initialized, it performs a union instead of an intersection.

        Parameters:
        - db: The name of the database where the `gene` table is located.
        - names: A list of tuples `[(namespace, name), ...]`, where:
            - `namespace` is the gene's namespace,
            - `name` is the gene's name.
        - errorCallback: An optional function called in case of an error
        during gene processing.

        Operation:
        - If the `gene` filter is not initialized (`_inputFilters[db]['gene']`
        is 0), calls `unionInputGenes` to create the filter with all provided
        genes.
        - Otherwise:
            - Logs the start of the gene filter reduction process.
            - Calls `prepareTableForQuery` to ensure the `gene` table is ready
            for queries.
            - Sets all genes in the table as "not kept" (`flag = 0`).
            - Counts the number of genes before reduction (`numBefore`).
            - Updates the `gene` table, setting `flag = 1` for genes that
            match the `biopolymer_id` identifiers generated from names in the
            provided list.
            - Deletes genes not in the provided list,i.e., those with `flag=0`.
            - Counts the number of genes removed (`numDrop`).
            - Logs warning information for unrecognized (`tally['zero']`) and
            ambiguous (`tally['many']`) identifiers, depending on the
            `allow_ambiguous_genes` setting.
            - Logs the final count of retained genes and the count of
            discarded genes.
            - Increments the `gene` filter counter for the database.

        Returns:
        - None. The method performs an intersection of genes in the specified
        table and logs the result.

        This function helps manage a gene set in a database table by
        performing an intersection of existing records with a provided list,
        keeping only valid matches.
        """
        # names=[ (namespace,name), ... ]
        if not self._inputFilters[db]["gene"]:
            return self.unionInputGenes(db, names, errorCallback)
        self.logPush("reducing %s gene filter ...\n" % db)
        cursor = self._loki._db.cursor()

        self.prepareTableForQuery(db, "gene")
        cursor.execute("UPDATE `%s`.`gene` SET flag = 0" % db)
        numBefore = cursor.getconnection().changes()
        tally = dict()
        sql = "UPDATE `%s`.`gene` SET flag = 1 WHERE biopolymer_id = ?4" % db
        maxMatch = None if self._options.allow_ambiguous_genes == "yes" else 1
        cursor.executemany(
            sql,
            self._loki.generateTypedBiopolymerIDsByIdentifiers(
                self.getOptionTypeID("gene"),
                names,
                minMatch=1,
                maxMatch=maxMatch,
                tally=tally,
                errorCallback=errorCallback,
            ),
        )
        cursor.execute("DELETE FROM `%s`.`gene` WHERE flag = 0" % db)
        numDrop = cursor.getconnection().changes()
        if tally["zero"]:
            self.warn(
                "WARNING: ignored %d unrecognized gene identifier(s)\n"
                % tally["zero"]  # noqa E501
            )
        if tally["many"]:
            if self._options.allow_ambiguous_genes == "yes":
                self.warn(
                    "WARNING: kept multiple results for %d ambiguous gene identifier(s)\n"  # noqa E501
                    % tally["many"]
                )
            else:
                self.warn(
                    "WARNING: ignored %d ambiguous gene identifier(s)\n"
                    % tally["many"]  # noqa E501
                )
        self.logPop(
            "... OK: kept %d genes (%d dropped)\n"
            % (numBefore - numDrop, numDrop)  # noqa E501
        )

        self._inputFilters[db]["gene"] += 1

    def unionInputGeneSearch(self, db, texts):
        """
        Adds genes to a gene filter in a specific database table based on a
        text search. This method searches for genes matching provided text
        terms and adds them to the filter.

        Parameters:
        - db: Name of the database where the `gene` table is located.
        - texts: List of tuples `[(text, extra), ...]`, where:
            - `text` is the search term used to locate genes,
            - `extra` holds additional information for each search term.

        Operation:
        - Logs the start of the gene addition process based on text search.
        - Retrieves the biopolymer type identifier (`typeID`) for 'gene'.
        - Calls `prepareTableForUpdate` to temporarily drop indexes from the
        `gene` table, optimizing insertion.
        - Defines an SQL `INSERT` query to add each gene found using
        `generateTypedBiopolymerIDsBySearch` into the `gene` table of the
        specified database.
        - Executes the query for each `(text, extra)` pair provided, inserting
        genes that match the search terms.
        - Counts the number of valid genes added (`numAdd`) and logs the total
        genes added to the filter.
        - Increments the `gene` filter counter in the database.

        Returns:
        - None. The method inserts genes into the specified table based on
        text search and logs the number of insertions performed.

        This method is useful for managing and adding genes to a database
        table based on a text search, applying an optimized insertion process
        that enables flexible gene lookup.
        """
        # texts=[ (text,extra), ... ]
        self.logPush("adding to %s gene filter by text search ...\n" % db)
        cursor = self._loki._db.cursor()

        typeID = self.getOptionTypeID("gene")

        self.prepareTableForUpdate(db, "gene")
        sql = (
            "INSERT INTO `%s`.`gene` (extra,label,biopolymer_id) VALUES (?1,?2,?3); SELECT 1"  # noqa E501
            % db
        )
        numAdd = 0
        for row in cursor.executemany(
            sql, self._loki.generateTypedBiopolymerIDsBySearch(typeID, texts)
        ):
            numAdd += 1
        self.logPop("... OK: added %d genes\n" % numAdd)

        self._inputFilters[db]["gene"] += 1

    def intersectInputGeneSearch(self, db, texts):
        """
        Reduces a gene filter in a specific database table, retaining only
        genes that match a provided list based on text-based searches. If the
        `gene` filter has not been initialized, it performs a union rather
        than an intersection.

        Parameters:
        - db: Name of the database where the `gene` table is located.
        - texts: List of tuples `[(text, extra), ...]`, where:
            - `text` is the search term used to filter genes,
            - `extra` contains additional information for each search term.

        Operation:
        - If the `gene` filter is not yet initialized
        (`_inputFilters[db]['gene']` is 0), calls `unionInputGeneSearch`
        to create the filter with all genes found in the text-based search.
        - Otherwise:
        - Logs the start of the gene filter reduction by text search.
        - Retrieves the biopolymer type ID for 'gene' (`typeID`).
        - Ensures the `gene` table is prepared for querying with
        `prepareTableForQuery`.
        - Sets all genes in the table to "not retained" (`flag = 0`).
        - Counts the number of genes before the reduction (`numBefore`).
        - Uses `generateTypedBiopolymerIDsBySearch` to find genes matching the
        provided text terms.
        - Updates `flag = 1` only for genes whose `biopolymer_id` matches the
        search terms.
        - Deletes genes that do not match the search (i.e., `flag = 0`).
        - Counts the number of genes removed (`numDrop`).
        - Logs the final count of retained genes and the number of genes
        discarded.
        - Increments the `gene` filter counter for the database.

        Returns:
        - None. This method performs an intersection of genes based on
        text-based search in the specified table and logs the outcome.

        This function is useful for managing a set of genes in a database
        table, performing an intersection on existing records based on a
        search term list, retaining only valid matches.
        """
        # texts=[ (text,extra), ... ]
        if not self._inputFilters[db]["gene"]:
            return self.unionInputGeneSearch(db, texts)
        self.logPush("reducing %s gene filter by text search ...\n" % db)
        cursor = self._loki._db.cursor()

        typeID = self.getOptionTypeID("gene")

        self.prepareTableForQuery(db, "gene")
        cursor.execute("UPDATE `%s`.`gene` SET flag = 0" % db)
        numBefore = cursor.getconnection().changes()
        sql = "UPDATE `%s`.`gene` SET flag = 1 WHERE biopolymer_id = ?3" % db
        cursor.executemany(
            sql, self._loki.generateTypedBiopolymerIDsBySearch(typeID, texts)
        )
        cursor.execute("DELETE FROM `%s`.`gene` WHERE flag = 0" % db)
        numDrop = cursor.getconnection().changes()
        self.logPop(
            "... OK: kept %d genes (%d dropped)\n"
            % (numBefore - numDrop, numDrop)  # noqa E501
        )

        self._inputFilters[db]["gene"] += 1
