# import os
from __future__ import annotations

from typing import Iterable, Optional, Union

import json
from pathlib import Path

# Internal Classes
from biofilter.db.database import Database
from biofilter.db.models import BiofilterMetadata

from biofilter.etl.etl_manager import ETLManager
from biofilter.etl.conflict_manager import ConflictManager

from biofilter.core.settings_manager import SettingsManager
from biofilter.utils.logger import Logger
from biofilter.utils.config import BiofilterConfig
from biofilter.utils.model_explorer import ModelExplorer
from biofilter.utils.migrate import run_migration

from biofilter.report.report_manager import ReportManager
from biofilter.query import Query, SchemaExplorer


class Biofilter:
    def __init__(self, db_uri: str = None, debug_mode: bool = False):
        self.version = "3.2.0"
        
        if debug_mode:
            self.logger = Logger(log_level="DEBUG")
            self.debug_mode = True
        else:
            self.logger = Logger()
            self.debug_mode = False
        self.db = None
        self._metadata = None
        # self._settings = None
        self._report = None     # Lazy-load: Report Manager
        self._query = None      # Lazy-load: Query Manager
        self._schema = None     # Lazy-load: Query Manager
        self._etl = None        # Lazy-load: ETL Manager
        self._conflict = None   # Lazy-load: Conflict Manager

        # Starting Sttings
        try:
            self.config = BiofilterConfig()
            config_path = str(self.config.path)
        except FileNotFoundError:
            # Fallback: empty config, rely on defaults
            self.logger.log(
                "🔧 Configuration file not found. This may lead to unexpected behavior or failures.",
                "WARNING",
            )
            config_path = None
            self.config = None  # or some minimal stub

        # --- Boot banner ---
        self._log_boot_banner(config_path)

        # DB Prioridade:
        # 1. Value from constructor
        # 2. Value from .biofilter.toml
        # 3. Fallback local
        self.db_uri = db_uri or self.config.db_uri or "sqlite:///biofilter.db"
        if self.db_uri:
            self.connect_db()

    def _log_boot_banner(self, config_path: str | None):
        self.logger.log("════════════════════════════════════", "INFO")
        self.logger.log(f"🚀 Initializing Biofilter3R", "INFO")
        self.logger.log(f"   • Version: {self.version}", "INFO")
        self.logger.log(f"   • Debug mode: {self.debug_mode}", "INFO")
        if config_path:
            self.logger.log(f"   • Config: {config_path}", "INFO")
        else:
            self.logger.log("   • Config: <none> (using defaults)", "INFO")
        self.logger.log("════════════════════════════════════", "INFO")


    @property
    def settings(self):
        if not self.db:
            msn = "You must connect to a database first."
            self.logger.log(msn, "INFO")
            raise RuntimeError(msn)
        if not hasattr(self, "_settings"):
            msn = "⚙️  Initializing settings..."
            self.logger.log(msn, "INFO")
            # self._settings = SettingsManager(self.biofilter.db.session)
            with self.db.get_session() as session:
                self._settings = SettingsManager(session)
        return self._settings

    def get_metadata(self):
        if self._metadata is None:
            with self.db.get_session() as session:
                self._metadata = (
                    session.query(BiofilterMetadata)
                    .order_by(BiofilterMetadata.id.desc())
                    .first()
                )
        return self._metadata

    @property
    def metadata(self):
        return self.get_metadata()

    def create_new_project(self, db_uri: str, overwrite=False):
        """Create a new Biofilter project database and connect to it."""
        self.logger.log(f"Creating Biofilter database at {db_uri}", "INFO")
        self._create_db(db_uri=db_uri, overwrite=overwrite)
        # self.connect_db(db_uri)
        self.logger.log(f"Biofilter database ready at {db_uri}", "INFO")

    def _create_db(self, db_uri: str = None, overwrite=False):
        if db_uri:
            self.db_uri = db_uri
        if not self.db_uri:
            msn = "Database URI must be set before creating the database."
            self.logger.log(msn, "ERROR")
            raise ValueError(msn)
        self.db = Database()  # Do not pass db_uri here
        self.db.db_uri = self.db_uri
        return self.db.create_db(overwrite=overwrite)

    def connect_db(self, new_uri: str = None):
        if new_uri:
            self.db_uri = new_uri
        self.db = Database(self.db_uri)

    # -------------------------------------------
    # UPDATE INTERFACES  / ETL MANAGEMENT METHODS
    # -------------------------------------------

    # Create Indexes
    # -------------------------------------------
    def rebuild_indexes(
        self,
        groups: Optional[Union[str, Iterable[str]]] = None,
        drop_only: bool = False,
        drop_first: bool = True,
        set_write_mode: bool = True,
        set_read_mode: bool = True,
    ) -> tuple[bool, str]:
        """
        Rebuild (drop/create) database indexes for selected index groups.

        Parameters
        ----------
        groups:
            Which index groups to process.
            - None: rebuild all groups
            - str: single group name (e.g. "genes", "protein", "variants")
            - Iterable[str]: multiple group names (e.g. ["gene", "protein"])
        drop_only:
            If True, only drops indexes and returns.
        drop_first:
            If True, drops indexes before creating them.
        set_write_mode / set_read_mode:
            Enables DB tuning hooks (SQLite PRAGMAs). No-op on Postgres.

        Examples
        --------
        bf.rebuild_indexes()  # all groups
        bf.rebuild_indexes("genes")
        bf.rebuild_indexes(["genes", "proteins"], drop_first=True)
        bf.rebuild_indexes("variant", drop_only=True)
        """
        if not self.db:
            msg = "Database not connected. Use connect_db() first."
            self.logger.log(msg, "ERROR")
            raise RuntimeError(msg)

        # Normalize groups -> list[str] | None
        if groups is None:
            index_group = None
        elif isinstance(groups, str):
            index_group = [groups]
        else:
            index_group = list(groups)

        self.logger.log("🧱 Starting index rebuild...", "INFO")

        manager = ETLManager(self.debug_mode, self.db.get_session())

        ok, msg = manager.rebuild_indexes(
            index_group=index_group,
            drop_only=drop_only,
            drop_first=drop_first,
            set_write_mode=set_write_mode,
            set_read_mode=set_read_mode,
        )

        level = "INFO" if ok else "WARNING"
        self.logger.log(msg, level)

        return ok, msg

    # Update Data Sources
    # -------------------------------------------
    def update(
        self,
        source_system: list = None,
        data_sources: list = None,
        run_steps: list = None,
        force_steps: list = None,
    ):  # noqa: E501
        """
        Starts the ETL process for the selected systems with step control.

        Parameters:
        - source_system: list of source systems to be processed.
        - run_steps: list of steps to execute ("extract", "transform", "load").
        - force_steps: list of steps to be forced, ignoring previous status.
        """

        if not self.db:
            msg = "Database not connected. Use connect_db() first."
            self.logger.log(msg, "ERROR")
            raise RuntimeError(msg)

        self.logger.log("🚀 Starting ETL update process...", "INFO")

        manager = ETLManager(self.debug_mode, self.db.get_session())

        manager.start_process(
            source_system=source_system,
            data_sources=data_sources,
            download_path=self.settings.get("download_path"),
            processed_path=self.settings.get("processed_path"),
            run_steps=run_steps,
            force_steps=force_steps,
            use_conflict_csv=False,
        )

        self.logger.log("✅ ETL update process finished.", "INFO")
        return True

    def update_conflicts(self, source_system: list = None):  # noqa: E501
        if not self.db:
            msg = "Database not connected. Use connect_db() first."
            self.logger.log(msg, "ERROR")
            raise RuntimeError(msg)

        self.logger.log("Starting ETL conflict resolution process...", "INFO")

        manager = ETLManager(self.db.get_session())

        manager.start_process(
            source_system=source_system,
            download_path=self.settings.get("download_path"),
            processed_path=self.settings.get("processed_path"),
            run_steps=["load"],
            force_steps=["load"],
            use_conflict_csv=True,
        )

        self.logger.log("ETL conflict resolution process finished.", "INFO")

        return True

    def __repr__(self):
        return f"<Biofilter(db_uri={self.db_uri})>"

    def restart_etl(
        self,
        data_source: list[str] = None,
        source_system: list[str] = None,
        delete_files: bool = False,
    ):
        """
        Restart ETL processes for the specified DataSources or SourceSystems.
        Args:
            data_source (list[str], optional): List of DataSources to restart.
            source_system (list[str], opt): List of SourceSystems to restart.
            delete_files (bool, optional): Whether to delete files after
                processing. Defaults to True.
        """
        if not self.db:
            msg = "Database not connected. Use connect_db() first."
            self.logger.log(msg, "ERROR")
            raise RuntimeError(msg)

        self.logger.log("🔄 Resetting the ETL Process", "INFO")

        manager = ETLManager(self.db.get_session())

        return manager.restart_etl_process(
            data_source=data_source,
            source_system=source_system,
            download_path=self.settings.get("download_path"),
            processed_path=self.settings.get("processed_path"),
            delete_files=delete_files,
        )

    def export_conflicts_to_excel(
        self, output_path: str = "curation_conflicts.xlsx"
    ):  # noqa E501
        """
        Exporta os conflitos de curadoria para um arquivo Excel.
        """
        if not self.db:
            msg = "Database not connected. Use connect_db() first."
            self.logger.log(msg, "ERROR")
            raise RuntimeError(msg)

        self.logger.log("🔄 Resetting the ETL Process", "INFO")

        manager = ConflictManager(
            session=self.db.get_session(), logger=self.logger
        )  # noqa E501
        return manager.export_conflicts_to_excel(output_path)

    def import_conflicts_from_excel(
        self, input_path="curation_conflicts_template.xlsx"
    ):
        if not self.db:
            msg = "Database not connected. Use connect_db() first."
            self.logger.log(msg, "ERROR")
            raise RuntimeError(msg)

        manager = ConflictManager(self.db.get_session(), self.logger)
        return manager.import_conflicts_from_excel(input_path)

    def model_explorer(self):
        model_info_path = (
            Path(__file__).parent.parent
            / "biofilter"
            / "db"
            / "models"
            / "models_info.json"
        )
        with open(model_info_path) as f:
            model_info = json.load(f)
        return ModelExplorer(session=self.db.session(), model_info=model_info)

    def migrate(self):
        # run_migration(self.db.session)
        run_migration(self.db.session, self.db.db_uri)


    # ----------------------------------
    # QUERY
    # ----------------------------------
    @property
    def query(self) -> Query:
        """Lazy-load Query interface."""
        if self._query is None:
            self._query = Query(self.db.get_session())
        return self._query

    # ----------------------------------
    # SCHEMA EXPLORER
    # ----------------------------------
    @property
    def schema(self) -> SchemaExplorer:
        """Lazy-load Schema Explorer interface."""
        if self._schema is None:
            self._schema = SchemaExplorer(self.query)
        return self._schema

    # ----------------------------------
    # REPORT
    # ----------------------------------
    @property
    def report(self):
        if self._report is None:
            if not self.db:
                raise RuntimeError("You must connect to a database first.")
            self._report = ReportManager(
                session=self.db.get_session(),
                logger=self.logger,
            )
        return self._report