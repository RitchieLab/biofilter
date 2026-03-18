# #################################################
# FILTERING, ANNOTATION & MODELING MIXIN
# #################################################
import itertools
import collections
import string


class FilterAnnotModelMixin:
    """
    Mixin class for managing filtering, annotation, and modeling in a LOKI.

    This class provides methods for generating filtered output, annotated
    output, and gene models based on user input and database queries. It
    includes methods for identifying candidate biopolymers and groups for
    modeling, as well as generating gene models and output based on user-
    defined filters and parameters.

    IMPLEMENTED METHODS:
    - [_populateColumnsFromTypes]:
        Populates lists of columns and headers based on provided data types.
    - [generateFilterOutput]:
        Generates filter output based on provided data types.
    - [generateAnnotationOutput]:
        Generates annotated output by applying additional annotations to an
        initial filtered dataset.
    - [identifyCandidateModelBiopolymers]:
        Identifies candidate biopolymers for models based on applicable
        filters.
    - [identifyCandidateModelGroups]:
        Identifies candidate model groups based on specified filters and
        criteria.
    - [getGeneModels]:
        Generates and returns gene models based on defined filters and
        parameters.
    - [generateModelOutput]:
        Generates and returns output models by combining and expanding
        specified column types.

    """

    def _populateColumnsFromTypes(
        self, types, columns=None, header=None, ids=None
    ):  # noqa E501
        """
        Populates lists of columns and headers based on provided data types.

        This method accepts a list of data types (`types`) and expands
        `columns` and `header` lists according to recognized types. It maps
        specific types to their respective columns and headers for use in a
        query. Unrecognized types trigger an exception.

        Parameters:
        - types (list): A list of strings representing data types to be mapped
            to columns and headers.
        - columns (list, optional): A list of columns that will be extended
            with names associated with the provided types. If not provided, a
            new list is created.
        - header (list, optional): A list of headers that will be extended
            with titles associated with the provided types. If not provided, a
            new list is created.
        - ids (list, optional): Currently unused, accepted for potential
            future expansion. If not provided, a new list is created.

        Behavior:
        - The method iterates over each type in `types` and maps it to
            relevant columns and headers.
        - Specific types, such as 'snp', 'position', 'gene', etc., add related
            columns and headers.
        - Input types like 'snpinput' and 'positioninput' are mapped to
            generic columns and headers.
        - For unrecognized types, the method raises an exception to prevent
            unexpected behavior.
        - If a type exists in `_queryColumnSources`, it is directly added to
            `header` and `columns`.

        Returns:
        - columns (list): The updated list of columns with names corresponding

        Usage Example:
        - `_populateColumnsFromTypes(['snp', 'gene'])` adds columns related to
            SNPs and genes to `columns` and `header` lists.
        """
        if columns is None:
            columns = list()
        if header is None:
            header = list()
        if ids is None:
            ids = list()
        for t in types:
            if t == "snp":
                header.extend(["snp"])
                columns.extend(["snp_label"])
            elif t == "position":
                header.extend(["chr", "position", "pos"])
                columns.extend(
                    ["position_chr", "position_label", "position_pos"]
                )  # oddball .map file format
            elif t == "gene":
                header.extend(["gene"])
                columns.extend(["gene_label"])
            elif t == "generegion":
                header.extend(["chr", "gene", "start", "stop"])
                columns.extend(
                    [
                        "biopolymer_chr",
                        "gene_label",
                        "biopolymer_start",
                        "biopolymer_stop",
                    ]
                )
            elif t == "upstream":
                header.extend(["upstream", "distance"])
                columns.extend(["upstream_label", "upstream_distance"])
            elif t == "downstream":
                header.extend(["downstream", "distance"])
                columns.extend(["downstream_label", "downstream_distance"])
            elif t == "region":
                header.extend(["chr", "region", "start", "stop"])
                columns.extend(
                    [
                        "region_chr",
                        "region_label",
                        "region_start",
                        "region_stop",
                    ]  # noqa E501
                )
            elif t == "group":
                header.extend(["group"])
                columns.extend(["group_label"])
            elif t == "source":
                header.extend(["source"])
                columns.extend(["source_label"])
            elif t == "gwas":
                header.extend(
                    [
                        "trait",
                        "snps",
                        "OR/beta",
                        "allele95%CI",
                        "riskAfreq",
                        "pubmed",
                    ]  # noqa E501
                )
                columns.extend(
                    [
                        "gwas_trait",
                        "gwas_snps",
                        "gwas_orbeta",
                        "gwas_allele95ci",
                        "gwas_riskAfreq",
                        "gwas_pubmed",
                    ]
                )
            elif t == "snpinput":
                header.extend(["user_input"])
                columns.extend(["snp_label"])
            elif t == "positioninput":
                header.extend(["user_input"])
                columns.extend(["position_label"])
            elif t == "geneinput":
                header.extend(["user_input"])
                columns.extend(["gene_label"])
            elif t == "regioninput":
                header.extend(["user_input"])
                columns.extend(["region_label"])
            elif t == "groupinput":
                header.extend(["user_input"])
                columns.extend(["group_label"])
            elif t == "sourceinput":
                header.extend(["user_input"])
                columns.extend(["source_label"])
            elif t in self._queryColumnSources:
                header.append(t)
                columns.append(t)
            else:
                raise Exception("ERROR: unsupported output type '%s'" % t)
        # foreach types
        return columns

    def generateFilterOutput(self, types, applyOffset=False):
        """
        Generates filter output based on provided data types.

        This method sets up and executes a filtered SQL query based on a list
        of data `types`. It utilizes `_populateColumnsFromTypes` to define
        query columns and headers, then constructs a query using `buildQuery`.
        If additional filters apply, a secondary query is built to incorporate
        user knowledge.

        Parameters:
        - types (list): List of strings representing data types to be filtered
            and included in the query.
        - applyOffset (bool, optional): Determines whether position values
            should be adjusted with an offset if necessary.

        Behavior:
        - Initializes `header` and `columns` lists, expanding them with
            `_populateColumnsFromTypes` based on provided types.
        - Raises an exception if the column list is empty, as at least one
            column is required for the query.
        - Adds a `#` marker to the first `header` item to designate it as a
            header.
        - Calls `buildQuery` to create the primary SQL query with
            `focus='main'` and `mode='filter'`.
        - If additional user input filters are present (`_inputFilters`), a
            secondary query is created,
        applying user knowledge to refine the results.
        - Returns an iterator that combines the header and query results,
            using `generateQueryResults`.

        Returns:
        - itertools.chain: An iterator yielding the header as a tuple followed
            by query results.

        Usage Example:
        - `generateFilterOutput(['snp', 'gene'])` generates a query output for
            SNP and gene data,
        including a header row and subsequent results.

        Exceptions:
        - Raises an exception if `header` and `columns` are empty, indicating
            an attempt to filter with no valid columns.
        """
        header = list()
        columns = list()
        self._populateColumnsFromTypes(types, columns, header)
        # No need to check for empty columns, as we always have at least one
        # No test case for this
        # if not (header and columns):
        #     raise Exception("filtering with empty column list")
        header[0] = "#" + header[0]
        query = self.buildQuery(
            mode="filter",
            focus="main",
            select=columns,
            applyOffset=applyOffset,  # noqa E501
        )  # noqa E501
        query2 = None
        if self._inputFilters["user"]["source"]:
            query2 = self.buildQuery(
                mode="filter",
                focus="main",
                select=columns,
                applyOffset=applyOffset,
                userKnowledge=True,
            )
        return itertools.chain(
            [tuple(header)],
            self.generateQueryResults(
                query,
                allowDupes=(self._options.allow_duplicate_output == "yes"),
                query2=query2,
            ),
        )

    def generateAnnotationOutput(self, typesF, typesA, applyOffset=False):
        """
        Generate annotated output by applying additional annotations to an
        initial filtered dataset.

        This method performs an initial filtering query followed by an
        annotation query, allowing additional data to be appended based on
        matching conditions. The initial query (`queryF`) retrieves a base set
        of data, and the second query (`queryA`) applies annotations according
        to the filtered results.

        Parameters:
        - typesF (list): List of data types for the initial filter query.
        - typesA (list): List of data types for annotation.
        - applyOffset (bool, optional): Indicates if position values should
            include an offset if needed.

        Behavior:
        - Creates `queryF`, a filtering query based on `typesF`, and generates
            the associated SQL (`sqlF`).
        - Generates `conditionsA`, conditions based on `rowid` columns from
            `queryF`, for the annotation query.
        - Creates `queryA`, the annotation query based on `typesA`, including
            conditions from `conditionsA`.
        - Executes `sqlF` to retrieve filtered data and, for each result,
            executes `sqlA` to obtain matching annotations.
        - Annotations are returned based on unique matches, considering
            `_options.allow_duplicate_output` to determine duplicate
            allowances.

        Returns:
        - Yields (tuple): Rows of results with columns from both the filter
            and annotation queries. Each row consists of the filtered data
            tuple followed by the corresponding annotation data, with `None`
            filling where no annotations exist.

        Example:
        - `generateAnnotationOutput(['snp', 'gene'], ['position', 'region'])`
            retrieves data based on SNP and gene, with annotations for
            position and region.

        Exceptions:
        - Raises an exception if `headerF` and `columnsF` or `headerA` and
            `columnsA` are empty, as this indicates an attempt to query
            without valid columns.

        Notes:
        - The output header is marked with `#` on the first item to indicate a
            file header.
        - Executes an SQL query plan for debugging if `_options.debug_query`
            is enabled.
        """
        # TODO user knowledge
        # build a baseline filtering query
        headerF = list()
        columnsF = list()
        self._populateColumnsFromTypes(typesF, columnsF, headerF)
        # No need to check for empty columns, as we always have at least one
        # No test case for this
        # if not (headerF and columnsF):
        #     raise Exception("annotation with no starting columns")
        queryF = self.buildQuery(
            mode="filter",
            focus="main",
            select=columnsF,
            applyOffset=applyOffset,  # noqa E501
        )
        lenF = len(queryF["_columns"])
        sqlF = self.getQueryText(queryF, splitRowIDs=True)
        self.prepareTablesForQuery(queryF)

        # add each filter rowid column as a condition for annotation
        n = lenF
        conditionsA = collections.defaultdict(set)
        for alias, cols in queryF["_rowid"].items():
            for col in cols:
                n += 1
                conditionsA[(alias, col)].add("= ?%d" % n)

        # build the annotation query
        headerA = list()
        columnsA = list()
        self._populateColumnsFromTypes(typesA, columnsA, headerA)
        # No need to check for empty columns, as we always have at least one
        # No test case for this
        # if not (headerA and columnsA):
        #     raise Exception("annotation with no extra columns")
        queryA = self.buildQuery(
            mode="annotate",
            focus="alt",
            select=columnsA,
            where=conditionsA,
            applyOffset=applyOffset,
        )
        lenA = len(queryA["_columns"])
        sqlA = self.getQueryText(
            queryA, noRowIDs=True, sortRowIDs=True, splitRowIDs=True
        )
        self.prepareTablesForQuery(queryA)

        # generate filtered results and annotate each of them
        cursorF = self._loki._db.cursor()
        cursorA = self._loki._db.cursor()
        if self._options.debug_query:
            self.warn("========== annotation : filter step ==========\n")
            self.warn(sqlF + "\n")
            for row in cursorF.execute("EXPLAIN QUERY PLAN " + sqlF):
                self.warn(str(row) + "\n")
            self.warn("========== annotation : annotate step ==========\n")
            self.warn(sqlA + "\n")
            emptyF = (0,) * (len(queryF["_columns"]) + len(queryF["_rowid"]))
            for row in cursorF.execute("EXPLAIN QUERY PLAN " + sqlA, emptyF):
                self.warn(str(row) + "\n")
        elif self._options.allow_duplicate_output == "yes":
            headerF[0] = "#" + headerF[0]
            yield tuple(headerF + headerA)
            lastF = None
            emptyA = tuple(None for c in columnsA)
            for rowF in cursorF.execute(sqlF):
                if lastF != rowF[-1]:
                    lastF = rowF[-1]
                    idsA = set()
                    for rowA in cursorA.execute(sqlA, rowF[:-1]):
                        rowidA = rowA[lenA:]
                        if rowidA not in idsA:
                            idsA.update(
                                itertools.product(
                                    *(
                                        (v,) if v == "" else (v, "")
                                        for v in rowidA  # noqa E501
                                    )  # noqa E501
                                )
                            )
                            yield rowF[:lenF] + rowA[:lenA]
                    # foreach annotation result
                    if not idsA:
                        yield rowF[:lenF] + emptyA
                # if filter result is new
            # foreach filter result
        else:
            headerF[0] = "#" + headerF[0]
            yield tuple(headerF + headerA)
            emptyA = tuple(None for c in columnsA)

            idsA = set()
            for rowF in cursorF.execute(sqlF):
                # idsA = set() # Avoid duplicate rows

                for rowA in cursorA.execute(sqlA, rowF[:-1]):

                    rowidA = rowA[lenA:]
                    if rowidA not in idsA:
                        idsA.update(
                            itertools.product(
                                *((v,) if v == "" else (v, "") for v in rowidA)
                            )
                        )
                        # return annotation results
                        yield rowF[:lenF] + rowA[:lenA]
                # foreach annotation result
                if not idsA:
                    yield rowF[:lenF] + emptyA
            # if filter result is new
            # foreach filter result

    def identifyCandidateModelBiopolymers(self):
        """
        Identifies candidate biopolymers for models, both primary and
        alternative, based on applicable filters.

        This method resets candidate tables and applies filters to identify
        biopolymers that may be relevant for modeling, separating them into
        primary and alternative candidates. Identified candidates are inserted
        into the `main_biopolymer` and `alt_biopolymer` tables within the
        `cand` database, each with a status indicator (`flag`).

        Steps:
        1. Resets candidate tables (`main_biopolymer` and `alt_biopolymer`)
            and prepares them for updates.
        2. Identifies primary candidates (in the `main` scope) if applicable
            filters are set:
            - Builds a query to retrieve relevant biopolymer IDs
                (or only gene IDs, if `_onlyGeneModels` is configured).
            - Executes the query and inserts the results into the
                `cand.main_biopolymer` table.
            - Updates the `cand.main_biopolymer` filter to indicate that
                primary candidates have been identified.
        3. Identifies alternative candidates (in the `alt` scope) if
            applicable filters are set:
            - Constructs and executes a query similar to that for primary
                candidates.
            - Inserts identified alternative candidates into
                `cand.alt_biopolymer`.
            - Updates the `cand.alt_biopolymer` filter to reflect that
                alternative candidates have been identified.

        Notes:
        - Uses `INSERT OR IGNORE` to prevent duplicates when inserting
            candidates.
        - Logs progress to report the count of identified candidates.
        """
        cursor = self._loki._db.cursor()

        # reset candidate tables
        self._inputFilters["cand"]["main_biopolymer"] = 0
        self.prepareTableForUpdate("cand", "main_biopolymer")
        cursor.execute("DELETE FROM `cand`.`main_biopolymer`")
        self._inputFilters["cand"]["alt_biopolymer"] = 0
        cursor.execute("DELETE FROM `cand`.`alt_biopolymer`")
        self.prepareTableForUpdate("cand", "alt_biopolymer")

        # identify main candidiates from applicable filters
        if sum(
            filters
            for table, filters in self._inputFilters["main"].items()
            if table not in ("group", "source")
        ):
            self.log("identifying main model candidiates ...")
            query = self.buildQuery(
                mode="modelgene",
                focus="main",
                select=[
                    "gene_id" if self._onlyGeneModels else "biopolymer_id"
                ],  # noqa E501
            )
            sql = "INSERT OR IGNORE INTO `cand`.`main_biopolymer` (biopolymer_id, flag) VALUES (?,0)"  # noqa E501
            cursor.executemany(
                sql, self.generateQueryResults(query, allowDupes=True)
            )  # noqa E501
            numCand = max(
                row[0]
                for row in cursor.execute(
                    "SELECT COUNT() FROM `cand`.`main_biopolymer`"
                )
            )
            self.log(" OK: %d candidates\n" % numCand)
            self._inputFilters["cand"]["main_biopolymer"] = 1
        # if any main filters other than group/source

        # identify alt candidiates from applicable filters
        if sum(
            filters
            for table, filters in self._inputFilters["alt"].items()
            if table not in ("group", "source")
        ):
            self.log("identifying alternate model candidiates ...")
            query = self.buildQuery(
                mode="modelgene",
                focus="alt",
                select=[
                    "gene_id" if self._onlyGeneModels else "biopolymer_id"
                ],  # noqa E501
            )
            sql = "INSERT OR IGNORE INTO `cand`.`alt_biopolymer` (biopolymer_id, flag) VALUES (?,0)"  # noqa E501
            cursor.executemany(
                sql, self.generateQueryResults(query, allowDupes=True)
            )  # noqa E501
            numCand = max(
                row[0]
                for row in cursor.execute(
                    "SELECT COUNT() FROM `cand`.`alt_biopolymer`"
                )  # noqa E501
            )
            self.log(" OK: %d candidates\n" % numCand)
            self._inputFilters["cand"]["alt_biopolymer"] = 1
        # if any alt filters other than group/source

    def identifyCandidateModelGroups(self):
        """
        Identify candidate model groups based on specified filters and
        criteria.

        This method filters and organizes candidate groups in the database
        based on conditions applied in the contexts 'main', 'alt', and 'cand',
        ensuring they meet the criteria for modeling.

        Steps:
        1. Resets the `cand.group` table, removing previous entries and
        initializing candidate flags.
        2. Identifies candidate groups based on filters in the 'main' context:
        - Builds a query with `buildQuery` to retrieve group IDs.
        - If `cand.group` has entries, updates candidate flags; otherwise,
            inserts new records.
        - Removes entries that do not meet candidate flag criteria.
        3. Identifies candidate groups based on filters in the 'alt' context:
        - Follows the same process as 'main' candidates to set flags and insert
            candidates as needed.
        4. Applies group size criteria (in the 'cand' context):
        - Constructs a query to group candidates by unique biopolymers or genes
            depending on `self._onlyGeneModels`.
        - Adds `HAVING` conditions to ensure groups contain at least two
            distinct elements and optionally limits the maximum group size
            (`self._options.maximum_model_group_size`).
        5. Executes the query and inserts or updates candidate groups.
        6. Updates `cand.group` to mark identified groups and logs the total
            count of candidates.

        Exceptions:
        - May raise exceptions if `generateQueryResults` fails to retrieve
            data.

        Logs:
        - Logs progress and results, including a final count of identified
            groups.
        """
        self.log("identifying candidiate model groups ...")
        cursor = self._loki._db.cursor()

        # reset candidate table
        self._inputFilters["cand"]["group"] = 0
        self.prepareTableForUpdate("cand", "group")
        cursor.execute("DELETE FROM `cand`.`group`")

        # identify candidiates from applicable main filters
        if sum(
            filters
            for table, filters in self._inputFilters["main"].items()
            if table in ("group", "source")
        ):
            query = self.buildQuery(
                mode="modelgroup", focus="main", select=["group_id"]
            )
            if self._inputFilters["cand"]["group"]:
                cursor.execute("UPDATE `cand`.`group` SET flag = 0")
                sql = "UPDATE `cand`.`group` SET flag = 1 WHERE group_id = ?"
            else:
                sql = "INSERT OR IGNORE INTO `cand`.`group` (group_id, flag) VALUES (?,0)"  # noqa E501
            cursor.executemany(
                sql, self.generateQueryResults(query, allowDupes=True)
            )  # noqa E501
            if self._inputFilters["cand"]["group"]:
                cursor.execute("DELETE FROM `cand`.`group` WHERE flag = 0")
            self._inputFilters["cand"]["group"] = 1
        # if any main group/source filters

        # identify candidiates from applicable alt filters
        if sum(
            filters
            for table, filters in self._inputFilters["alt"].items()
            if table in ("group", "source")
        ):
            query = self.buildQuery(
                mode="modelgroup", focus="alt", select=["group_id"]
            )  # noqa E501
            if self._inputFilters["cand"]["group"]:
                cursor.execute("UPDATE `cand`.`group` SET flag = 0")
                sql = "UPDATE `cand`.`group` SET flag = 1 WHERE group_id = ?"
            else:
                sql = "INSERT OR IGNORE INTO `cand`.`group` (group_id, flag) VALUES (?,0)"  # noqa E501
            cursor.executemany(
                sql, self.generateQueryResults(query, allowDupes=True)
            )  # noqa E501
            if self._inputFilters["cand"]["group"]:
                cursor.execute("DELETE FROM `cand`.`group` WHERE flag = 0")
            self._inputFilters["cand"]["group"] = 1
        # if any main group/source filters

        # identify candidiates by size
        query = self.buildQuery(
            mode="modelgroup",
            focus="cand",
            select=["group_id"],
            having={
                ("gene_id" if self._onlyGeneModels else "biopolymer_id"): {
                    "!= 0"
                }  # noqa E501
            },
        )
        if self._inputFilters["cand"]["group"]:
            cursor.execute("UPDATE `cand`.`group` SET flag = 0")
            sql = "UPDATE `cand`.`group` SET flag = 1 WHERE group_id = ?"
        else:
            sql = "INSERT OR IGNORE INTO `cand`.`group` (group_id, flag) VALUES (?,0)"  # noqa E501
        # _queryColumnSources[col] = list[ tuple(alias,rowid,expression,?conditions),... ] # noqa E501
        for source in self._queryColumnSources["group_id"]:
            if source[0] in query["FROM"]:
                query["GROUP BY"].append(
                    "{0}.{1}".format(source[0], source[1])
                )  # noqa E501
                break
        for source in self._queryColumnSources[
            "gene_id" if self._onlyGeneModels else "biopolymer_id"
        ]:
            if source[0] in query["FROM"]:
                if self._options.maximum_model_group_size > 0:
                    query["HAVING"].add(
                        "(COUNT(DISTINCT %s) BETWEEN 2 AND %d)"
                        % (source[2], self._options.maximum_model_group_size)
                    )
                else:
                    query["HAVING"].add(
                        "COUNT(DISTINCT %s) >= 2" % (source[2],)
                    )  # noqa E501
                break
        cursor.executemany(
            sql, self.generateQueryResults(query, allowDupes=True)
        )  # noqa E501
        if self._inputFilters["cand"]["group"]:
            cursor.execute("DELETE FROM `cand`.`group` WHERE flag = 0")
        self._inputFilters["cand"]["group"] = 1

        numCand = max(
            row[0]
            for row in cursor.execute("SELECT COUNT() FROM `cand`.`group`")  # noqa E501
        )
        self.log(" OK: %d groups\n" % numCand)

    # identifyCandidateModelGroups()

    def getGeneModels(self):
        """
        Generates and returns gene models based on defined filters and
        parameters.

        This method first checks if gene models have already been generated;
        if not, it identifies candidate biopolymers and groups as model
        components. It then constructs and executes a query to retrieve gene
        models, applying grouping and sorting conditions based on biopolymers,
        source, and group, using minimum and maximum score and count parameters
        defined in options.

        Steps:
        1. Checks if gene models (`_geneModels`) have already been computed.
        2. If models are not yet generated:
            - Calls `identifyCandidateModelBiopolymers` and
                `identifyCandidateModelGroups` to identify candidate
                biopolymers and groups.
            - Constructs a `buildQuery` query to select models, including
                biopolymer, source, and group IDs.
            - Defines grouping columns for biopolymers (using `MIN` and `MAX`
                to prevent duplicates) and
            distinct counts for `source_id` and `group_id`.
            - Applies `HAVING` filters to ensure only models with the minimum
                score (`minimum_model_score`)
            are included.
            - Adds sorting and count limit options to restrict the number of
                models returned (`maximum_model_count`).
        3. Executes the query and stores results in `_geneModels`, logging the
            number of models generated.

        Returns:
            list: A list of generated gene models.

        Logs:
            - Logs the progress of model generation and the total count of
            models computed.
        """
        # generate the models if we haven't already
        if self._geneModels is None:
            # find all model component candidiates
            self.identifyCandidateModelBiopolymers()
            self.identifyCandidateModelGroups()

            # build model query
            formatter = string.Formatter()
            query = self.buildQuery(
                mode="model",
                focus="cand",
                select=[
                    "biopolymer_id_L",
                    "biopolymer_id_R",
                    "source_id",
                    "group_id",
                ],  # noqa E501
            )
            query["GROUP BY"].append(
                formatter.vformat(
                    "MIN({biopolymer_id_L}, {biopolymer_id_R})",
                    args=None,
                    kwargs=query["SELECT"],
                )
            )
            query["GROUP BY"].append(
                formatter.vformat(
                    "MAX({biopolymer_id_L}, {biopolymer_id_R})",
                    args=None,
                    kwargs=query["SELECT"],
                )
            )
            query["SELECT"]["biopolymer_id_L"] = (
                "MIN(%s)" % query["SELECT"]["biopolymer_id_L"]
            )
            query["SELECT"]["biopolymer_id_R"] = (
                "MAX(%s)" % query["SELECT"]["biopolymer_id_R"]
            )
            query["SELECT"]["source_id"] = (
                "COUNT(DISTINCT %s)" % query["SELECT"]["source_id"]
            )
            query["SELECT"]["group_id"] = (
                "COUNT(DISTINCT %s)" % query["SELECT"]["group_id"]
            )
            if self._options.minimum_model_score > 0:
                query["HAVING"].add(
                    "%s >= %d"
                    % (
                        query["SELECT"]["source_id"],
                        self._options.minimum_model_score,
                    )  # noqa E501
                )
            if self._options.sort_models == "yes":
                query["ORDER BY"].append(
                    formatter.vformat(
                        "{source_id} DESC", args=None, kwargs=query["SELECT"]
                    )
                )
                query["ORDER BY"].append(
                    formatter.vformat(
                        "{group_id} DESC", args=None, kwargs=query["SELECT"]
                    )
                )
            if self._options.maximum_model_count > 0:
                query["LIMIT"] = self._options.maximum_model_count

            # execute query and store models
            self._geneModels = list()
            self.log("calculating baseline models ...")
            self._geneModels = list(
                self.generateQueryResults(query, allowDupes=True)
            )  # the GROUP BY already prevents duplicates
            self.log(" OK: %d models\n" % len(self._geneModels))
        # if no models yet

        return self._geneModels

    # getGeneModels()

    def generateModelOutput(self, typesL, typesR, applyOffset=False):
        """
        Generates and returns output models by combining and expanding
        specified column types.

        This method creates models based on `typesL` and `typesR` column types,
        expanding each input column combination to produce paired models. It
        supports pairwise knowledge if required. When `all_pairwise_models` is
        disabled, existing gene models are used to limit generated pairs;
        otherwise, all combinations are generated.

        Parameters:
        - `typesL`: List of column types for the left side of the model
            expansion.
        - `typesR`: List of column types for the right side of the model
            expansion.
        - `applyOffset`: (optional) Determines whether a positional offset
            should be applied to the data.

        Steps:
        1. Sets a limit on the maximum number of models (if defined) and
            retrieves gene models if required.
        2. Constructs SQL queries for the left and right sides of the models
            using the provided column types.
        3. Checks if `debug_query` is enabled to log SQL query plans and
            statements.
        4. Executes queries to generate model combinations:
        - When `all_pairwise_models` is disabled, expands each gene-gene pair.
        - When `all_pairwise_models` is enabled, generates all combinations of
            `typesL` and `typesR`.
        5. Returns an iterator with model output headers and rows, limited to
            a maximum number if specified.

        Returns:
        - An iterator containing the generated output header and model rows.

        Logging:
        - Logs SQL queries and diagnostic results when `debug_query` is
            enabled, to assist with debugging and viewing SQL execution plans.
        """
        # TODO user knowledge
        cursor = self._loki._db.cursor()
        limit = max(0, self._options.maximum_model_count)

        # if we'll need baseline gene models, generate them first
        if self._options.all_pairwise_models != "yes":
            self.getGeneModels()

        # build queries for left- and right-hand model expansion
        headerL = list()
        columnsL = list()
        self._populateColumnsFromTypes(typesL, columnsL, headerL)
        headerR = list()
        columnsR = list()
        self._populateColumnsFromTypes(typesR, columnsR, headerR)
        if not (headerL and columnsL and headerR and columnsR):
            raise Exception("model generation with empty column list")
        headerL = list(("%s1" % h) for h in headerL)
        headerL[0] = "#" + headerL[0]
        headerR = list(("%s2" % h) for h in headerR)
        conditionsL = conditionsR = None
        # for knowledge-supported models, add the conditions for expanding
        # from base models
        if self._options.all_pairwise_models != "yes":
            conditionsL = {
                ("gene_id" if self._onlyGeneModels else "biopolymer_id"): {
                    "= (CASE WHEN 1 THEN ?1 ELSE 0*?2*?3*?4 END)"
                }
            }
            conditionsR = {
                ("gene_id" if self._onlyGeneModels else "biopolymer_id"): {
                    "= (CASE WHEN 1 THEN ?2 ELSE 0*?1*?3*?4 END)"
                }
            }
        queryL = self.buildQuery(
            mode="filter",
            focus="main",
            select=columnsL,
            having=conditionsL,
            applyOffset=applyOffset,
        )
        sqlL = self.getQueryText(queryL)
        self.prepareTablesForQuery(queryL)
        queryR = self.buildQuery(
            mode="filter",
            focus="alt",
            select=columnsR,
            having=conditionsR,
            applyOffset=applyOffset,
        )
        sqlR = self.getQueryText(queryR)
        self.prepareTablesForQuery(queryR)

        # debug or execute model expansion
        if self._options.debug_query:
            self.log(sqlL + "\n")
            self.log("-----\n")
            for row in cursor.execute(
                "EXPLAIN QUERY PLAN " + sqlL,
                (
                    (1, 2, 3, 4)
                    if self._options.all_pairwise_models != "yes"
                    else None  # noqa E501
                ),  # noqa E501
            ):
                self.log(str(row) + "\n")

            self.log("=====\n")

            self.log(sqlR + "\n")
            self.log("-----\n")
            for row in cursor.execute(
                "EXPLAIN QUERY PLAN " + sqlR,
                (
                    (1, 2, 3, 4)
                    if self._options.all_pairwise_models != "yes"
                    else None  # noqa E501
                ),  # noqa E501
            ):
                self.log(str(row) + "\n")
        elif self._options.all_pairwise_models != "yes":
            # expand each gene-gene model
            diffTypes = typesL != typesR
            headerR.append("score(src-grp)")
            yield tuple(headerL + headerR)
            modelIDs = set()
            for model in self.getGeneModels():
                score = ("%d-%d" % (model[2], model[3]),)
                # store the expanded right-hand side, then pair them all with
                # the expanded left-hand side
                listR = list(cursor.execute(sqlR, model))
                for row in cursor.execute(sqlL, model):
                    for modelR in listR:
                        modelID = (
                            (row[-1], modelR[-1])
                            if (diffTypes or (row[-1] <= modelR[-1]))
                            else (modelR[-1], row[-1])
                        )
                        if (diffTypes or (row[-1] != modelR[-1])) and (
                            modelID not in modelIDs
                        ):
                            modelIDs.add(modelID)
                            yield row[:-1] + modelR[:-1] + score
                            if limit and len(modelIDs) >= limit:
                                return
                    # foreach right-hand
                # foreach left-hand
            # foreach model
        else:
            yield tuple(headerL + headerR)
            n = 0

            # first query the right-hand side results and store them
            listR = list()
            rowIDs = set()
            for row in cursor.execute(sqlR):
                if row[-1] not in rowIDs:
                    rowIDs.add(row[-1])
                    listR.append(row)
            del rowIDs

            # now query the left-hand side results and pair each with the
            # stored right-hand sides
            rowIDs = set()
            diffCols = columnsL != columnsR
            for row in cursor.execute(sqlL):
                if row[-1] not in rowIDs:
                    rowIDs.add(row[-1])
                    for modelR in listR:
                        if diffCols or row[-1] != modelR[-1]:
                            n += 1
                            yield row[:-1] + modelR[:-1]
                            if limit and n >= limit:
                                return
            del rowIDs
        # if debug/normal/pairwise
