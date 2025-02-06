import omics_modules.omics_db as omics_db
from omics_modules.logger import Logger
from omics_modules.mixins import (
    UpdaterWorkflowMixin,
    UpdaterSourceMixin,
    UpdaterOperationsMixin,
    UpdaterDownloadMixin,
    UpdaterLiftOverMixin,
)  # noqa E501


class Updater(
    UpdaterDownloadMixin,
    UpdaterWorkflowMixin,
    UpdaterSourceMixin,
    UpdaterLiftOverMixin,
    UpdaterOperationsMixin,
):

    def __init__(self, database):
        assert isinstance(database, omics_db.Database)
        self._database = database
        self._engine = database.engine  # or self.engine = omicsdb.engine
        self.logger = Logger()  # (log_level="DEBUG")

        self.keep_download = False
        self.only_download = False
        self.skip_download = False
        self.dir_download = None
        self.source_list = []

        self._sourceSystems = {}

        # Keep the instances of datasources
        self._sourceClasses = dict()

        self._sourceObjects = dict()
        self._sourceOptions = dict()
        self._sourceVersions = dict()
        self._filehash = dict()
        # self.srcSetsToDownload = {}
