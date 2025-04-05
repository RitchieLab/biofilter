# #################################################
# BIOFILTER CLASS
# #################################################
import sys
import os

# Adiciona o diretório raiz do projeto ao `sys.path`
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  # noqa E501

from mixins import (  # noqa E402
    LokiDataRetrievalMixin,
    LoggerMixin,
    Schema,
    DatabaseManagementMixin,
    InputDataParsersMixin,
    SNPInputMixin,
    LocusPositionInputMixin,
    RegionInputMixin,
    GeneInputMixin,
    GroupInputMixin,
    SourceInputMixin,
    UserKnowledgeInputMixin,
    UserKnowledgeRetrievalMixin,
    ParisMixin,
    InternalQueryBuilderMixin,
    FilterAnnotModelMixin,
)  # noqa E402

from loki_modules import loki_db  # noqa E402


class Biofilter(
    LokiDataRetrievalMixin,
    LoggerMixin,
    Schema,
    DatabaseManagementMixin,
    InputDataParsersMixin,
    SNPInputMixin,
    LocusPositionInputMixin,
    RegionInputMixin,
    GeneInputMixin,
    GroupInputMixin,
    SourceInputMixin,
    UserKnowledgeInputMixin,
    UserKnowledgeRetrievalMixin,
    ParisMixin,
    InternalQueryBuilderMixin,
    FilterAnnotModelMixin,
):
    """
    Biofilter is a modular class designed for managing, processing, and
    querying biological data, especially genetic information. It extends
    multiple mixins that each encapsulate a particular aspect of data handling,
    from parsing various input formats (e.g., SNPs, loci) to managing
    databases and logging activities. The class integrates schema management,
    data filtering, and complex internal querying to support extensive data
    workflows.

    Attributes:
    - `_schema`: Schema definitions for databases, leveraging the Schema class
        and copying
    main schemas to alternate filters.
    - `_options`: Configuration options, defaulting to an empty, safe-to-
        access object.
    - `_loki`: Database instance, set with logger and configured to manage
        temporary databases.

    Mixins:
    The class relies on a series of mixins that handle specialized data
    operations:
    - `LokiDataRetrievalMixin`: Handles data retrieval using Loki-specific
        methods.
    - `LoggerMixin`: Provides structured logging mechanisms.
    - `Schema`: Defines schema structures used throughout the data operations.
    - `DatabaseManagementMixin`: Manages database connections, tables, and
        related operations.
    - `InputDataParsersMixin`: Parses diverse data input types and manages
        schema-specific parsing.
    - Additional mixins (`SNPInputMixin`, `RegionInputMixin`, etc.): Handle
        specific data types
    such as SNPs, gene loci, regions, sources, and user knowledge.

    Methods:
    - `getVersionTuple`: Returns the class version information as a tuple.
    - `getVersionString`: Formats and returns a readable version string.
    - `__init__`: Initializes the class with options, sets up logging and
        database configurations, and prepares necessary filters and schema
        management.

    Usage:
    Biofilter is intended for use within bioinformatics pipelines, where
    managing complex datasets and performing operations on large, structured
    genetic data is required. The class supports flexible configuration
    through options, streamlined database management, and robust error
    handling.
    """

    # FIXME: Replace _schema with Schema.schema
    _schema = Schema.schema
    # copy main schema for alternate input filters
    _schema["alt"] = _schema["main"]

    @classmethod
    def getVersionTuple(cls):
        # tuple = (major,minor,revision,dev,build,date)
        # dev must be in ('a','b','rc','release') for lexicographic comparison
        return (3, 0, 1, "release", "", "2025-01-01")

    @classmethod
    def getVersionString(cls):
        v = list(cls.getVersionTuple())
        # tuple = (major,minor,revision,dev,build,date)
        # dev must be > 'rc' for releases for lexicographic comparison,
        # but we don't need to actually print 'release' in the version string
        v[3] = "" if v[3] > "rc" else v[3]
        return "%d.%d.%d%s%s (%s)" % tuple(v)

    # #################################################
    # CONSTRUCTOR
    def __init__(self, options=None):

        # Force options object
        if not options:
            """
            If no options provided, creates an 'Empty' class instance that
            safely returns None for undefined attributes.
            """

            class Empty(object):
                def __getattr__(self, name):
                    # attributes default to None
                    if name == "prefix":
                        return "default_prefix"
                    elif name == "overwrite":
                        return "yes"
                    elif name == "stdout":
                        return "no"
                    elif name == "quiet":
                        return "no"
                    elif name == "verbose":
                        return "no"
                    return None

            options = Empty()
        self._options = options

        # # Set up logging
        self._quiet = options.quiet == "yes"
        self._verbose = options.verbose == "yes"
        self._logIndent = 0
        self._logHanging = False
        self._logFile = None
        if options.stdout != "yes":
            logPath = options.prefix + ".log"
            if (options.overwrite != "yes") and os.path.exists(logPath):
                sys.exit(
                    "ERROR: log file '%s' already exists, must specify --overwrite or a different --prefix"  # noqa E501
                    % logPath
                )  # noqa E501
            self._logFile = open(logPath, "w")

        self._tablesDeindexed = {db: set() for db in self._schema}
        self._inputFilters = {
            db: {tbl: 0 for tbl in self._schema[db]} for db in self._schema
        }
        self._geneModels = None
        self._onlyGeneModels = True  # TODO

        # verify loki_db version 'extra' input support in generateLiftOver*()
        minLoki = (2, 2, 1, "a", 2)
        if loki_biofilter.db.Database.getVersionTuple() < minLoki:
            # sys.exit(
            #     "ERROR: LOKI version %d.%d.%d%s%s later required; found %s"
            #     % minLoki  # noqa: E501
            #     + (loki_biofilter.db.Database.getVersionString(),)
            # )
            found_version = loki_biofilter.db.Database.getVersionString()
            sys.exit(
                "ERROR: LOKI version %d.%d.%d%s%s or later required; found %s"
                % (
                    *minLoki,
                    found_version,
                )  # Desempacota minLoki e adiciona found_version
            )

        # initialize instance database
        self._loki = loki_biofilter.db.Database()
        # self._loki.setLogger(self)
        for db in self._schema:
            if db != "main":
                # in SQLite 'main' is implicit, but the others must be attached as temp stores  # noqa: E501
                self._loki.attachTempDatabase(db)
            self._loki.createDatabaseTables(
                self._schema[db], db, None, doIndecies=True
            )  # noqa: E501
