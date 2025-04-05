# import os
from biofilter.db.database import Database
from biofilter.core.settings_manager import SettingsManager
from biofilter.utils.logger import Logger
from biofilter.etl.etl_manager import ETLManager


class Biofilter:
    def __init__(self, db_uri: str = None):
        self.logger = Logger(log_level="DEBUG")
        self.db_uri = db_uri
        self.db = None

        if self.db_uri:
            self.connect_db()

    @property
    def settings(self):
        if not self.db:
            msn = "You must connect to a database first."
            self.logger.log(msn, "INFO")
            raise RuntimeError(msn)
        if not hasattr(self, "_settings"):
            msn = "⚙️ Initializing settings..."
            self.logger.log(msn, "INFO")
            # self._settings = SettingsManager(self.biofilter.db.session)
            with self.biofilter.db.get_session() as session:
                self._settings = SettingsManager(session)
        return self._settings

    def connect_db(self, new_uri: str = None):
        if new_uri:
            self.db_uri = new_uri
        self.db = Database(self.db_uri)

    def create_db(self, db_uri: str = None, overwrite=False):
        if db_uri:
            self.db_uri = db_uri
        if not self.db_uri:
            msn = "Database URI must be set before creating the database."
            self.logger.log(msn, "ERROR")
            raise ValueError(msn)
        self.db = Database()
        self.biofilter.db.db_uri = self.db_uri
        self.biofilter.db.create_db(overwrite=overwrite)
        return True

    def update(self, source_system: list = None):
        if not self.db:
            msn = "Database not connected. Use connect_db() first."
            self.logger.log(msn, "ERROR")
            raise RuntimeError(msn)

        self.logger.log("Starting ETL update process...", "INFO")

        manager = ETLManager(self.biofilter.db.get_session())
        manager.start_process(
            source_system=source_system,
            download_path=self.settings.get("download_path"),
            processed_path=self.settings.get("processed_path"),
        )

        self.logger.log("ETL update process finished.", "INFO")

        return True

    def __repr__(self):
        return f"<Biofilter(db_uri={self.db_uri})>"
