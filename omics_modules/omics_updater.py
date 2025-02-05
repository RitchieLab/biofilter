import os
import importlib
import logging
# from threading import Lock

import omics_modules.omics_db as omics_db
import omics_modules.omics_source as omics_source
import omics_modules.source_systems as source_systems
from omics_modules.omics_logger import OmicsLogger
from omics_modules.omics_mixins import (
    UpdaterDownloadMixin,
    UpdaterWorkflowMixin,
    UpdaterLiftOverMixin,
    UpdaterOperationsMixin,
)  # noqa E501


class Updater(
    UpdaterDownloadMixin,
    UpdaterWorkflowMixin,
    UpdaterLiftOverMixin,
    UpdaterOperationsMixin,
):

    ##################################################
    # constructor

    def __init__(self, database, is_test=False):
        assert isinstance(database, omics_db.Database)
        self._is_test = is_test
        self._database = database
        self._engine = database.engine  # or self.engine = omicsdb.engine
        self.logger = OmicsLogger(log_level="DEBUG")

        # Check it we need:
        self._sourceSystems = {}
        self._sourceClasses = dict()
        self._sourceObjects = dict()
        self._sourceOptions = dict()
        self._filehash = dict()
        # self._updating = False
        self._tablesUpdated = set()
        self._tablesDeindexed = set()
        # self.lock = Lock()
        self.srcSetsToDownload = {}
        self.keepDownload = False
        self.onlyDownload = False
        self.skipDownload = False

    ##################################################
    # database update

    def findSourceModules(self):
        if not self._sourceSystems:
            self._sourceSystems = {}
            source_system_path = source_systems.__path__
            for path in source_system_path:
                for srcModuleName in os.listdir(path):
                    if srcModuleName.startswith("omics_source_"):
                        self._sourceSystems[srcModuleName[13:-3]] = 1

            self.logger.log("[INFO] TESTE")
    
    def getSourceModules(self):
        self.findSourceModules()
        return self._sourceSystems.keys()

    def loadSourceModules(self, sources=None):
        self.findSourceModules()
        srcSet = set()
        for srcName in set(sources) if sources else self._sourceSystems.keys():
            if srcName not in self._sourceClasses:
                if srcName not in self._sourceSystems:
                    self.logger.log(
                        "WARNING: unknown source '%s'\n" % srcName,
                    )
                    continue
                # if module not available
                srcModule = importlib.import_module(
                    "%s.omics_source_%s" % (source_systems.__name__, srcName)
                )
                srcClass = getattr(srcModule, "Source_%s" % srcName)
                if not issubclass(srcClass, omics_source.Source):
                    self.logger.log(
                        "WARNING: invalid module for source '%s'\n" % srcName,
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
                    self._database
                )  # noqa: E501
            # if module not instantiated
            srcSet.add(srcName)
        # foreach source
        return srcSet
