# biofilter/biofilter.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from biofilter.utils.logger import Logger
from biofilter.utils.config import BiofilterConfig
from biofilter.db.database import Database

from biofilter.api.db import DBModule
from biofilter.api.settings import SettingsModule
from biofilter.api.etl import ETLModule
from biofilter.api.conflicts import ConflictModule
from biofilter.api.query import QueryModule
from biofilter.api.schema import SchemaModule
from biofilter.api.reports import ReportModule


@dataclass
class BiofilterCore:
    db_uri: str
    debug_mode: bool = False
    version: str = "4.0.0"

    def __post_init__(self):
        self.logger = Logger(log_level="DEBUG") if self.debug_mode else Logger()

        # Config
        try:
            self.config = BiofilterConfig()
            self.config_path = str(self.config.path)
        except FileNotFoundError:
            self.config = None
            self.config_path = None
            self.logger.log(
                "🔧 Configuration file not found. Using defaults.",
                "WARNING",
            )

        self.db: Optional[Database] = None

        # Boot banner
        self.logger.log("════════════════════════════════════", "INFO")
        self.logger.log("🚀 Initializing Biofilter", "INFO")
        self.logger.log(f"   • Version: {self.version}", "INFO")
        self.logger.log(f"   • Debug mode: {self.debug_mode}", "INFO")
        self.logger.log(
            f"   • Config: {self.config_path}" if self.config_path else "   • Config: <none>",
            "INFO",
        )
        self.logger.log(f"   • DB URI: {self.db_uri}", "INFO")
        self.logger.log("════════════════════════════════════", "INFO")

    def require_db(self) -> Database:
        if not self.db:
            msg = "Database not connected. Use bf.db.connect() first."
            self.logger.log(msg, "ERROR")
            raise RuntimeError(msg)
        return self.db


class Biofilter:
    """
    Public entry point. Exposes bf.db.*, bf.etl.*, bf.query.*, bf.reports.*, etc.
    """

    def __init__(self, db_uri: str = None, debug_mode: bool = False):
        # DB URI priority:
        # 1) ctor param
        # 2) config db_uri
        # 3) default local
        tmp_logger = Logger(log_level="DEBUG") if debug_mode else Logger()
        try:
            cfg = BiofilterConfig()
            cfg_db_uri = getattr(cfg, "db_uri", None)
        except FileNotFoundError:
            cfg_db_uri = None

        final_uri = db_uri or cfg_db_uri or "sqlite:///./biofilter.db"
        self.core = BiofilterCore(db_uri=final_uri, debug_mode=debug_mode)

        # Modules (composition)
        self.db = DBModule(self.core)
        self.settings = SettingsModule(self.core)
        self.etl = ETLModule(self.core)
        self.conflicts = ConflictModule(self.core)
        self.query = QueryModule(self.core)
        self.schema = SchemaModule(self.core, self.query)
        self.reports = ReportModule(self.core)

    def __repr__(self) -> str:
        return f"<Biofilter(db_uri={self.core.db_uri})>"
