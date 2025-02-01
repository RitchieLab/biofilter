import os
import importlib
from threading import Lock
import logging

import loki_modules.loki_db as loki_db
import loki_modules.loki_source as loki_source
import loki_modules.loaders as loaders
from loki_mixins import (
    UpdaterDownloadMixin,
    UpdaterDatabaseMixin,
    UpdaterLiftOverMixin,
    UpdaterOperationsMixin,
)


class Updater(
    UpdaterDownloadMixin,
    UpdaterDatabaseMixin,
    UpdaterLiftOverMixin,
    UpdaterOperationsMixin,
):

    ##################################################
    # constructor

    def __init__(self, lokidb, is_test=False):
        assert isinstance(lokidb, loki_db.Database)
        self._is_test = is_test
        self._loki = lokidb
        self._db = lokidb._db
        self._sourceLoaders = {}
        self._sourceClasses = dict()
        self._sourceObjects = dict()
        self._sourceOptions = dict()
        self._filehash = dict()
        self._updating = False
        self._tablesUpdated = set()
        self._tablesDeindexed = set()
        self.lock = Lock()
        self.srcSetsToDownload = {}
        self.keepDownload = False
        self.onlyDownload = False
        self.skipDownload = False

    ##################################################
    # logging: pass through to loki_db (Database Class)
    # TODO (Versin 3.0.2) Analise if it is possible to remove this method,
    # check if all calls to log are made through the loki_db class

    def log(self, message="", level=logging.INFO, indent=0):
        return self._loki.log(message=message, level=level, indent=indent)

    def log_exception(self, error):
        return self._loki.log_exception(error)

    ##################################################
    # database update

    def flagTableUpdate(self, table):
        self._tablesUpdated.add(table)

    def prepareTableForUpdate(self, table):
        if self._updating:
            self.flagTableUpdate(table)
            if table not in self._tablesDeindexed:
                self._tablesDeindexed.add(table)
                self._loki.dropDatabaseIndices(None, "db", table)

    def prepareTableForQuery(self, table):
        if self._updating:
            if table in self._tablesDeindexed:
                # print "reindexing %s" % table DEBUG
                self._tablesDeindexed.remove(table)
                self._loki.createDatabaseIndices(None, "db", table)

    def findSourceModules(self):
        if not self._sourceLoaders:
            self._sourceLoaders = {}
            loader_path = loaders.__path__
            if self._is_test:
                loader_path = [
                    os.path.join(loader, "test") for loader in loaders.__path__
                ]
            for path in loader_path:
                for srcModuleName in os.listdir(path):
                    if srcModuleName.startswith("loki_source_"):
                        self._sourceLoaders[srcModuleName[12:-3]] = 1

    def getSourceModules(self):
        self.findSourceModules()
        return self._sourceLoaders.keys()

    def loadSourceModules(self, sources=None):
        self.findSourceModules()
        srcSet = set()
        for srcName in set(sources) if sources else self._sourceLoaders.keys():
            if srcName not in self._sourceClasses:
                if srcName not in self._sourceLoaders:
                    self.log(
                        "WARNING: unknown source '%s'\n" % srcName,
                        level=logging.WARNING,
                        indent=0,
                    )
                    continue
                # if module not available
                srcModule = importlib.import_module(
                    "%s.loki_source_%s" % (loaders.__name__, srcName)
                )
                srcClass = getattr(srcModule, "Source_%s" % srcName)
                if not issubclass(srcClass, loki_source.Source):
                    self.log(
                        "WARNING: invalid module for source '%s'\n" % srcName,
                        level=logging.WARNING,
                        indent=0,
                    )  # noqa: E501
                    continue
                self._sourceClasses[srcName] = srcClass
            # if module class not loaded
            srcSet.add(srcName)
        # foreach source
        return srcSet

    def getSourceModuleVersions(self, sources=None):
        srcSet = self.loadSourceModules(sources)
        return {
            srcName: self._sourceClasses[srcName].getVersionString()
            for srcName in srcSet
        }

    def getSourceModuleOptions(self, sources=None):
        srcSet = self.loadSourceModules(sources)
        return {
            srcName: self._sourceClasses[srcName].getOptions()
            for srcName in srcSet  # noqa: E501
        }

    def attachSourceModules(self, sources=None):
        sources = self.loadSourceModules(sources)
        srcSet = set()
        for srcName in sources:
            if srcName not in self._sourceObjects:
                if srcName not in self._sourceClasses:
                    raise Exception(
                        "loadSourceModules() reported false positive for '%s'"
                        % srcName  # noqa: E501
                    )
                self._sourceObjects[srcName] = self._sourceClasses[srcName](
                    self._loki
                )  # noqa: E501
            # if module not instantiated
            srcSet.add(srcName)
        # foreach source
        return srcSet
