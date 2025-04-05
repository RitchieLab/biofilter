import apsw

# import sys
import logging

from loki_mixins import (
    LoggerMixin,
    DbSchemaMixin,
    DbVersionMixin,
    DbConfigMixin,
    DbSchemaMaintenanceMixin,
    DbOperationsGetMixin,
    DbOperationsSetMixin,
    DbUpdaterMixin,
    DbLiftOverMixin,
    DbQueryMixin,
)


class Database(
    LoggerMixin,
    DbVersionMixin,
    DbConfigMixin,
    DbSchemaMixin,
    DbSchemaMaintenanceMixin,
    DbOperationsGetMixin,
    DbOperationsSetMixin,
    DbUpdaterMixin,
    DbLiftOverMixin,
    DbQueryMixin,
):
    """
    A class to interact with a SQLite database using APSW.

    Attributes:
            chr_num (dict): A dictionary mapping chromosome names and numbers.
            chr_name (dict): A dictionary mapping chromosome numbers to names.
            _schema (dict): A dictionary containing the schema definition to DB
    """

    _schema = DbSchemaMixin.schema

    # hardcode translations between chromosome numbers and textual tags
    chr_num = {}
    chr_name = {}
    cnum = 0
    for cname in (
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
        "11",
        "12",
        "13",
        "14",
        "15",
        "16",
        "17",
        "18",
        "19",
        "20",
        "21",
        "22",
        "X",
        "Y",
        "XY",
        "MT",
    ):
        cnum += 1
        chr_num[cnum] = cnum
        chr_num["%s" % cnum] = cnum
        chr_num[cname] = cnum
        chr_name[cnum] = cname
        chr_name["%s" % cnum] = cname
        chr_name[cname] = cname
    chr_num["M"] = chr_num["MT"]
    chr_name["M"] = chr_name["MT"]

    ##################################################
    # constructor

    def __init__(
        self, dbFile=None, testing=False, updating=False, tempMem=False
    ):  # noqa: E501
        """
        Initializes a Database instance.

        Args:
            dbFile (str, optional): The database file to attach.
            testing (bool, optional): If True, runs in testing mode.
            updating (bool, optional): If True, runs in updating mode.
            tempMem (bool, optional): If True, uses memory for temporary
            storage.
        """

        # initialize instance properties
        self._is_test = testing
        self._updating = updating
        self._verbose = True
        self._db = apsw.Connection("")
        self._dbFile = None
        self._dbNew = None
        self._updater = None
        self._liftOverCache = dict()  # { (from,to) : [] }

        # set the verbosity level
        # self.setVerbose(self._verbose)

        # initialize logger
        self.init_logger(
            log_file="loki-build.log",
            log_level=logging.DEBUG if testing else logging.INFO,
        )
        # initialize the database
        self.log(
            "=========================================================",  # noqa: E501
            level=logging.CRITICAL,
        )

        # initialize the database
        self.log(
            "DATABASE INSTANCE CREATED AND LOGGING SYSTEM INITIALIZED.",  # noqa: E501
            level=logging.CRITICAL,
        )
        self.log(
            f"Log file: {self.get_log_file()}\n", level=logging.CRITICAL  # noqa: E501
        )

        self.configureDatabase(tempMem=tempMem)
        self.attachDatabaseFile(dbFile)

    ##################################################
    # context manager

    def __enter__(self):
        """
        Enters the context manager.

        Returns:
            Connection: The APSW connection object.
        """
        return self._biofilter.db.__enter__()

    def __exit__(self, excType, excVal, traceback):
        """
        Exits the context manager.

        Args:
            excType (type): Exception type.
            excVal (Exception): Exception value.
            traceback (traceback): Traceback object.

        Returns:
            bool: True if no exception occurred, otherwise False.
        """
        return self._biofilter.db.__exit__(excType, excVal, traceback)
