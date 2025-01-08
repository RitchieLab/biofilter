# #################################################
# USER KNOWLEDGE INPUT MIXIN
# #################################################
import itertools


class UserKnowledgeInputMixin:
    """
    Mixin class for managing user-defined knowledge input filters in a Loki
    database.

    IMPLEMENTED METHODS:
    - [addUserSource]:
        Adds a new user-defined source entry to the `user.source` table, which
        can be used to record additional data sources for analysis.
    - [addUserGroup]:
        Adds a new user-defined group entry to the `user.group` table,
        associating it with a specific data source.
    - [addUserGroupBiopolymers]:
        Adds biopolymers (genes) to a user-defined group in the
        `user.group_biopolymer` table, using a list of biopolymer identifiers
        to link to the group.
    - [applyUserKnowledgeFilter]:
        Applies user-defined knowledge to filter the main tables (`main.group`
        or `main.gene`) based on user-provided data in the `user.group` and
        `user.group_biopolymer` tables.
    """

    def addUserSource(self, label, description, errorCallback=None):
        """
        Adds a new user-defined source entry to the `user.source` table,
        allowing additional data sources for analysis to be recorded.

        Parameters:
        - label (str): Name or label of the new source to be added.
        - description (str): Detailed description of the source, providing
        extra context.
        - errorCallback (function, optional): Function to be called in case of
        an error during insertion.

        Operation:
        - Logs a message indicating a new source addition.
        - Increments the user source counter in `_inputFilters['user']
        ['source']`, generating a unique negative `source_id` for identifying
        the new source.
        - Inserts the new source entry into the `user.source` table with
        `source_id`, `label`, and `description`.
        - On success, logs a confirmation message and returns the `source_id`.

        Returns:
        - int: A unique negative identifier (`usourceID`) for the new source
        added, enabling tracking or reference to this entry in future
        operations.

        This method allows users to add custom data sources, supporting the
        expansion of the database with user-specific sources that are not
        predefined.
        """

        self.log("adding user-defined source '%s' ..." % (label,))
        self._inputFilters["user"]["source"] += 1
        usourceID = -self._inputFilters["user"]["source"]
        cursor = self._loki._db.cursor()
        cursor.execute(
            "INSERT INTO `user`.`source` (source_id,source,description) VALUES (?,?,?)",  # noqa: E501
            (usourceID, label, description),
        )
        self.log(" OK\n")
        return usourceID

    def addUserGroup(self, usourceID, label, description, errorCallback=None):
        """
        Adds a new user-defined group to the `user.group` table, associating
        it with a specific data source.

        Parameters:
        - usourceID (int): Identifier for the source associated with the group
        such as a predefined `source_id`.
        - label (str): Name or label of the group to be added.
        - description (str): Detailed description of the group, providing
        additional context.
        - errorCallback (function, optional): Optional function to be called
        if an error occurs during insertion.

        Operation:
        - Logs a message indicating that a new group is being added.
        - Increments the user group counter in `_inputFilters['user']['group'],
        creating a unique, negative `group_id`
        to identify the new group.
        - Inserts the new group into the `user.group` table with `group_id`,
        `label`, `description`, and `source_id`, associating the group with
        the specified source.
        - On success, logs a confirmation and returns the `group_id`.

        Returns:
        - `ugroupID` (int): A unique, negative identifier for the newly added
        group, allowing future tracking or referencing of this entry.

        This method enables the addition of user-defined groups and associates
        them with specific data sources, offering flexibility in organizing
        data by user-defined groups and sources.
        """

        self.log("adding user-defined group '%s' ..." % (label,))
        self._inputFilters["user"]["group"] += 1
        ugroupID = -self._inputFilters["user"]["group"]
        cursor = self._loki._db.cursor()
        cursor.execute(
            "INSERT INTO `user`.`group` (group_id,label,description,source_id) VALUES (?,?,?,?)",  # noqa: E501
            (ugroupID, label, description, usourceID),
        )
        self.log(" OK\n")
        return ugroupID

    def addUserGroupBiopolymers(self, ugroupID, namesets, errorCallback=None):
        """
        Adds biopolymers (genes) to a user-defined group in the
        `user.group_biopolymer` table, using a list of biopolymer identifiers
        to link to the group.

        Parameters:
        - ugroupID: The identifier of the user group to which genes will be
        added.
        - namesets: A list of lists of tuples `[(namespace, name, extra), ...]`
        where each tuple represents a biopolymer identifier, divided by
        `namespace` and `name`.
        - errorCallback (function): Optional function called in case of a
        matching error for an identifier.

        Operation:
        - Logs the addition of genes to the specified user group.
        - Defines and executes an SQL `INSERT OR IGNORE` query to add each
        resulting `biopolymer_id` into the `user.group_biopolymer` table for
        the provided `group_id` (`ugroupID`).
        - Uses `generateTypedBiopolymerIDsByIdentifiers` to generate valid
        `biopolymer_id`s from `namesets`, specifying the biopolymer type as
        `gene` and applying a minimum match threshold of 1.
        - Evaluates identifier matching:
        - Displays a warning if `tally['zero']` contains unknown identifiers.
        - Displays a warning if `tally['many']` contains identifiers with
        ambiguous matches.
        - Queries the total number of biopolymers added to the group using
        `SELECT COUNT()`, logging the result.
        - Increments the `group_biopolymer` count in `_inputFilters['user']`.

        Returns:
        - None. This method is used to add biopolymers to a user-defined group
        and logs success information and warnings.

        This method allows adding a list of genes to a user-defined group,
        applying matching rules and displaying warnings for unknown or
        ambiguous identifiers, with customizable error handling.
        """

        # TODO: apply ambiguity settings and heuristics?
        # namesets=[ [ (ns,name,extra), ...], ... ]
        self.logPush("adding genes to user-defined group ...\n")
        cursor = self._loki._db.cursor()

        sql = (
            "INSERT OR IGNORE INTO `user`.`group_biopolymer` (group_id,biopolymer_id) VALUES (%d,?4)"  # noqa: E501
            % (ugroupID,)
        )
        tally = dict()
        cursor.executemany(
            sql,
            self._loki.generateTypedBiopolymerIDsByIdentifiers(
                self.getOptionTypeID("gene"),
                itertools.chain(*namesets),
                minMatch=1,
                maxMatch=None,
                tally=tally,
                errorCallback=errorCallback,
            ),
        )
        if tally["zero"]:
            self.warn(
                "WARNING: ignored %d unrecognized gene identifier(s)\n"
                % tally["zero"]  # noqa: E501
            )
        if tally["many"]:
            self.warn(
                "WARNING: added multiple results for %d ambiguous gene identifier(s)\n"  # noqa: E501
                % tally["many"]
            )
        numAdd = sum(
            row[0]
            for row in cursor.execute(
                "SELECT COUNT() FROM `user`.`group_biopolymer` WHERE group_id = ?",  # noqa: E501
                (ugroupID,),
            )
        )

        self.logPop("... OK: added %d genes\n" % numAdd)
        self._inputFilters["user"]["group_biopolymer"] += 1

    def applyUserKnowledgeFilter(self, grouplevel=False):
        """
        Applies user-defined knowledge to filter primary tables (`main.group`
        or `main.gene`) based on user-provided data in `user.group` and
        `user.group_biopolymer`.

        Parameters:
        - grouplevel (bool): Specifies the filter level to be applied.
            - If True, applies filtering at the `group` level, adding groups
            to the `main.group` table.
            - If False (default), applies filtering at the `gene` level,
            adding genes to the `main.gene` table.

        Operation:
        - When `grouplevel` is True:
            - Logs the start of the group-level filtering process.
            - Inserts data into the `main.group` table from `user.group` and
            `user.group_biopolymer`, including:
                - Distinct user-defined groups from `user.group`.
                - Biopolymer groups (`group_biopolymer`) mapped within the
                database (`db.group_biopolymer`).
            - After insertion, counts and logs the number of groups added to
            `main.group`.
            - Increments the filter counter for `main.group`.

        - When `grouplevel` is False:
            - Logs the start of the gene-level filtering process.
            - Inserts data into the `main.gene` table from
            `user.group_biopolymer`, including:
                - Genes associated with biopolymers in `user.group_biopolymer`
                mapped within the database `db.biopolymer`.
            - After insertion, counts and logs the number of genes added to
            `main.gene`.
            - Increments the filter counter for `main.gene`.

        Returns:
        - None. The method applies the filtering rules and logs the results of
        the insertions.

        This function facilitates incorporating user-defined data and
        knowledge into primary
        `group` and `gene` tables, enabling custom expansion of reference data
        based on group
        or gene filters.
        """

        cursor = self._loki._db.cursor()
        if grouplevel:
            self.logPush(
                "applying user-defined knowledge to main group filter ...\n"
            )  # noqa: E501
            assert self._inputFilters["main"]["group"] == 0  # TODO
            sql = """
                INSERT INTO `main`.`group` (label,group_id,extra)
                SELECT DISTINCT u_g.label, u_g.group_id, u_g.extra
                FROM `user`.`group` AS u_g
                UNION
                SELECT DISTINCT d_g.label, d_g.group_id, NULL AS extra
                FROM `user`.`group_biopolymer` AS u_gb
                JOIN `db`.`group_biopolymer` AS d_gb
                ON d_gb.biopolymer_id = u_gb.biopolymer_id
                JOIN `db`.`group` AS d_g
                ON d_g.group_id = d_gb.group_id
                """
            cursor.execute(sql)
            num = sum(
                row[0]
                for row in cursor.execute(
                    "SELECT COUNT() FROM `main`.`group`"
                )  # noqa: E501
            )
            self.logPop("... OK: added %d groups\n" % (num,))
            self._inputFilters["main"]["group"] += 1
        else:
            self.logPush(
                "applying user-defined knowledge to main gene filter ...\n"
            )  # noqa: E501
            assert self._inputFilters["main"]["gene"] == 0  # TODO
            sql = """
                INSERT INTO `main`.`gene` (label,biopolymer_id,extra)
                SELECT DISTINCT d_b.label, d_b.biopolymer_id, NULL AS extra
                FROM `user`.`group_biopolymer` AS u_gb
                JOIN `db`.`biopolymer` AS d_b
                ON d_b.biopolymer_id = u_gb.biopolymer_id
                """
            cursor.execute(sql)
            num = sum(
                row[0]
                for row in cursor.execute(
                    "SELECT COUNT() FROM `main`.`gene`"
                )  # noqa: E501
            )
            self.logPop("... OK: added %d genes\n" % (num,))
            self._inputFilters["main"]["gene"] += 1
        # if grouplevel
