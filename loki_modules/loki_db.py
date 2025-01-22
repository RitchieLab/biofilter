import apsw

# import sys
import logging

from loki_mixins import (
    Schema,
    VersionMixin,
    LoggerMixin,
    DatabaseConfigMixin,
    DatabaseSchemaMixin,
    DatabaseOperationsMixin,
    DatabaseLiftOverMixin,
    DatabaseQueryMixin,
)


class Database(
    Schema,
    VersionMixin,
    LoggerMixin,
    DatabaseConfigMixin,
    DatabaseSchemaMixin,
    DatabaseOperationsMixin,
    DatabaseLiftOverMixin,
    DatabaseQueryMixin,
):
    """
    A class to interact with a SQLite database using APSW.

    Attributes:
            chr_num (dict): A dictionary mapping chromosome names and numbers.
            chr_name (dict): A dictionary mapping chromosome numbers to names.
            _schema (dict): A dictionary containing the schema definition to DB
    """

    _schema = Schema.schema

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
            "DATABASE INSTANCE CREATED AND LOGGING SYSTEM INITIALIZED.",  # noqa: E501
            level=logging.CRITICAL
            )
        self.log(
            f"Log file: {self.get_log_file()}\n",  # noqa: E501
            level=logging.CRITICAL
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
        return self._db.__enter__()

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
        return self._db.__exit__(excType, excVal, traceback)


# TODO: find a better place for this liftover testing code
"""
if __name__ == "__main__":
    inputFile = file(sys.argv[1])
    loki = Database(sys.argv[2])
    outputFile = file(sys.argv[3],'w')
    unmapFile = file(sys.argv[4],'w')
    oldHG = int(sys.argv[5]) if (len(sys.argv) > 5) else 18
    newHG = int(sys.argv[6]) if (len(sys.argv) > 6) else 19

    def generateInput():
        for line in inputFile:
            chrom,start,end = line.split()
            if chrom[:3].upper() in ('CHM','CHR'):
                chrom = chrom[3:]
            yield (None, loki.chr_num.get(chrom,-1), int(start), int(end))

    def errorCallback(region):
        print >> unmapFile, "chr"+loki.chr_name.get(region[1],'?'),
            region[2], region[3]

    for region in loki.generateLiftOverRegions(
        oldHG, newHG, generateInput(),
        errorCallback=errorCallback
        ):
        print >> outputFile, "chr"+loki.chr_name.get(region[1],'?'),
            region[2], region[3]`
"""
