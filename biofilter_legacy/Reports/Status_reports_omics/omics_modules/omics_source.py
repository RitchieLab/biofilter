import apsw
from omics_modules.logger import Logger
from omics_modules.mixins.omics_source_ingestion import SourceIngestionMixin
from omics_modules.mixins.omics_source_download import SourceConnectorMixin
from omics_modules.mixins.omics_source_utils import SourceUtilsMixin


class Source(SourceIngestionMixin, SourceConnectorMixin, SourceUtilsMixin):
    """
    Base class for data sources, providing APSW connection and utility methods.
    """

    def __init__(self, db):
        """
        Initializes an APSW connection for direct database insertions.

        Args:
            db (OmicsDB): The OmicsDB instance containing database configuration.
        """
        self.datasource_id = None
        self.logger = Logger()
        self._alchemy_db = db  # SQLAlchemy connection object
        self._dbFile = self._alchemy_biofilter.db._dbFile  # Extract database file path
        self._apsw_db = apsw.Connection(self._dbFile)  # APSW connection

        self.logger.log(
            f"[INFO] Source initialized with APSW for database: {self._dbFile}."
        )

    # def __init__(self, lokidb):
    #     assert isinstance(lokidb, loki_biofilter.db.Database)
    #     assert self.__class__.__name__.startswith("Source_")
    #     self._loki = lokidb
    #     self._db = lokibiofilter.db._db
    #     self._sourceID = self.addSource(self.getSourceName())
    #     assert self._sourceID > 0

    # @classmethod
    # def getVersionString(cls):
    #     # when checked out from SVN, these $-delimited strings are magically
    #     # kept updated
    #     rev = "$Revision$".split()
    #     date = "$Date$".split()
    #     stat = None

    #     if len(rev) > 2:
    #         version = "r%s" % rev[1:2]
    #     else:
    #         stat = stat or os.stat(sys.modules[cls.__module__].__file__)
    #         version = "%s" % (stat.st_size,)

    #     if len(date) > 3:
    #         version += " (%s %s)" % date[1:3]
    #     else:
    #         stat = stat or os.stat(sys.modules[cls.__module__].__file__)
    #         version += datetime.fromtimestamp(
    #             stat.st_mtime, tz=timezone.utc
    #         ).strftime(  # noqa E501
    #             " (%Y-%m-%d)" if len(rev) > 2 else " (%Y-%m-%d %H:%M:%S)"
    #         )

    #     return version

    # @classmethod
    # def getOptions(cls):
    #     return None

    # def validateOptions(self, options):
    #     for o in options:
    #         return "unexpected option '%s'" % o
    #     return True

    # def download(self, options):
    #     raise Exception(
    #         "invalid LOKI Source plugin: download() not implemented"  # noqa E501
    #     )

    # def update(self, options):
    #     raise Exception("invalid LOKI Source plugin: update() not implemented")

    # ##################################################
    # # context manager
    # def __enter__(self):
    #     return self._loki.__enter__()

    # def __exit__(self, excType, excVal, traceback):
    #     return self._loki.__exit__(excType, excVal, traceback)

    # ##################################################
    # # logging
    # # TODO (3.0.2) check if this is still needed
    # def log(self, message="", level=logging.INFO, indent=0):
    #     return self._loki.log(message=message, level=level, indent=indent)

    # def log_exception(self, error):
    #     return self._loki.log_exception(error)

    # ##################################################
    # # database update
    # def prepareTableForUpdate(self, table):
    #     return self._loki.prepareTableForUpdate(table)

    # def prepareTableForQuery(self, table):
    #     return self._loki.prepareTableForQuery(table)

    # def deleteAll(self):
    #     dbc = self._biofilter.db.cursor()
    #     tables = [
    #         "snp_merge",
    #         "snp_locus",
    #         "snp_entrez_role",
    #         "biopolymer",
    #         "biopolymer_name",
    #         "biopolymer_name_name",
    #         "biopolymer_region",
    #         "group",
    #         "group_name",
    #         "group_group",
    #         "group_biopolymer",
    #         "group_member_name",
    #         "chain",
    #         "chain_data",
    #         "gwas",
    #     ]
    #     for table in tables:
    #         dbc.execute(
    #             "DELETE FROM `db`.`%s` WHERE source_id = %d"
    #             % (table, self.getSourceID())
    #         )
