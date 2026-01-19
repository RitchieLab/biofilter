from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from biofilter.modules.db.database import Database
from biofilter.utils.logger import Logger
from biofilter.utils.config import BiofilterConfig

from biofilter.core.components import (
    DBComponent,
    SettingsComponent,
    ETLComponent,
    ConflictsComponent,
    QueryComponent,
    SchemaComponent,
    ReportComponent,
    TransferComponent,
)


@dataclass
class BiofilterCore:
    """
    Shared state container.

    Keeps the single active Database instance (with bootstrapped metadata/mappings)
    so all components can reuse it reliably.
    """
    # db_uri: str
    db_uri: Optional[str]
    debug_mode: bool = False
    version: str = "4.0.0"

    def __post_init__(self):
        self.logger = Logger(log_level="DEBUG") if self.debug_mode else Logger()

        # Config (optional)
        try:
            self.config = BiofilterConfig()
            self.config_path = str(self.config.path)
        except FileNotFoundError:
            self.config = None
            self.config_path = None
            self.logger.log("🔧 Configuration file not found. Using defaults.", "WARNING")

        self.db: Optional[Database] = None

        # Lazy caches
        self._settings_manager = None
        self._query = None
        self._schema = None
        self._report_manager = None

        # Components will be attached by the facade (Biofilter)
        self.db_component = None
        self.settings = None
        self.etl = None
        self.conflicts = None
        self.query_component = None
        self.schema_component = None
        self.reports = None
        self.transfer = None


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
    Public facade.

    Usage:
        bf = Biofilter("sqlite:///./biofilter.db")
        bf.db.connect()
        bf.etl.update(...)
        bf.reports.list()
        bf.reports.run("gene_to_snp", {...})
    """

    def __init__(self, db_uri: str | None = None, debug_mode: bool = False):
        # DB URI priority: ctor > config > default
        cfg_db_uri = None
        try:
            cfg = BiofilterConfig()
            cfg_db_uri = getattr(cfg, "db_uri", None)
        except FileNotFoundError:
            pass

        final_uri = db_uri or cfg_db_uri or None
        self.core = BiofilterCore(db_uri=final_uri, debug_mode=debug_mode)

        # Components
        self.db = DBComponent(self.core)
        if self.core.db_uri:
            self.db.connect()

        self.settings = SettingsComponent(self.core)
        self.etl = ETLComponent(self.core)
        self.conflicts = ConflictsComponent(self.core)

        self.query = QueryComponent(self.core)
        self.schema = SchemaComponent(self.core, self.query)

        self.reports = ReportComponent(self.core)

        self.transfer = TransferComponent(self.core)

        # Optional: expose components on core for internal cross-calls
        self.core.db_component = self.db
        self.core.settings = self.settings
        self.core.etl = self.etl
        self.core.conflicts = self.conflicts
        self.core.query_component = self.query
        self.core.schema_component = self.schema
        self.core.reports = self.reports
        self.core.transfer = self.transfer

    def __repr__(self) -> str:
        return f"<Biofilter(db_uri={self.core.db_uri})>"
