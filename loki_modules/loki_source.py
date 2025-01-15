import os
import sys
from datetime import datetime, timezone

import loki_modules.loki_db as loki_db
from loki_mixins import SourceUtilityMethods, SourceDbOperations


class Source(SourceUtilityMethods, SourceDbOperations):  # noqa E501

    def __init__(self, lokidb):
        assert isinstance(lokidb, loki_db.Database)
        assert self.__class__.__name__.startswith("Source_")
        self._loki = lokidb
        self._db = lokidb._db
        self._sourceID = self.addSource(self.getSourceName())
        assert self._sourceID > 0

    @classmethod
    def getVersionString(cls):
        # when checked out from SVN, these $-delimited strings are magically
        # kept updated
        rev = "$Revision$".split()
        date = "$Date$".split()
        stat = None

        if len(rev) > 2:
            version = "r%s" % rev[1:2]
        else:
            stat = stat or os.stat(sys.modules[cls.__module__].__file__)
            version = "%s" % (stat.st_size,)

        if len(date) > 3:
            version += " (%s %s)" % date[1:3]
        else:
            stat = stat or os.stat(sys.modules[cls.__module__].__file__)
            version += datetime.fromtimestamp(
                stat.st_mtime, tz=timezone.utc
            ).strftime(  # noqa E501
                " (%Y-%m-%d)" if len(rev) > 2 else " (%Y-%m-%d %H:%M:%S)"
            )

        return version

    @classmethod
    def getOptions(cls):
        return None

    def validateOptions(self, options):
        for o in options:
            return "unexpected option '%s'" % o
        return True

    def download(self, options):
        raise Exception(
            "invalid LOKI Source plugin: download() not implemented"  # noqa E501
        )

    def update(self, options):
        raise Exception("invalid LOKI Source plugin: update() not implemented")

    ##################################################
    # context manager
    def __enter__(self):
        return self._loki.__enter__()

    def __exit__(self, excType, excVal, traceback):
        return self._loki.__exit__(excType, excVal, traceback)

    ##################################################
    # logging
    def log(self, message=""):
        return self._loki.log(message)

    def logPush(self, message=None):
        return self._loki.logPush(message)

    def logPop(self, message=None):
        return self._loki.logPop(message)

    ##################################################
    # database update
    def prepareTableForUpdate(self, table):
        return self._loki.prepareTableForUpdate(table)

    def prepareTableForQuery(self, table):
        return self._loki.prepareTableForQuery(table)

    def deleteAll(self):
        dbc = self._db.cursor()
        tables = [
            "snp_merge",
            "snp_locus",
            "snp_entrez_role",
            "biopolymer",
            "biopolymer_name",
            "biopolymer_name_name",
            "biopolymer_region",
            "group",
            "group_name",
            "group_group",
            "group_biopolymer",
            "group_member_name",
            "chain",
            "chain_data",
            "gwas",
        ]
        for table in tables:
            dbc.execute(
                "DELETE FROM `db`.`%s` WHERE source_id = %d"
                % (table, self.getSourceID())
            )
