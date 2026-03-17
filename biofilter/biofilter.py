from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

from biofilter.core.components import (
    DBComponent,
    ETLComponent,
    ReportComponent,
    SettingsComponent,
)
from biofilter.modules.db.database import Database
from biofilter.utils.config import BiofilterConfig
from biofilter.utils.logger import Logger
from biofilter.utils.version import __version__


@dataclass
class BiofilterCore:
    """
    Shared state container.

    Keeps the single active Database instance (with bootstrapped metadata/mappings)  # noqa E501
    so all components can reuse it reliably.
    """

    # db_uri: str
    db_uri: Optional[str]
    debug_mode: bool = False
    version: str = __version__

    @staticmethod
    def _safe_db_uri(db_uri: Optional[str]) -> Optional[str]:
        """
        Redact password from URI before logging.
        """
        if not db_uri or "://" not in db_uri:
            return db_uri

        try:
            parts = urlsplit(db_uri)
            if parts.password is None:
                return db_uri

            host = parts.hostname or ""
            if ":" in host:
                host = f"[{host}]"

            userinfo = f"{parts.username}:***@" if parts.username else ""
            port = f":{parts.port}" if parts.port else ""
            netloc = f"{userinfo}{host}{port}"
            return urlunsplit(
                (parts.scheme, netloc, parts.path, parts.query, parts.fragment)
            )
        except Exception:
            return "<db_uri_redacted>"

    def __post_init__(self):
        self.logger = Logger(log_level="DEBUG") if self.debug_mode else Logger()  # noqa E501

        # Config (optional)
        try:
            self.config = BiofilterConfig()
            self.config_path = str(self.config.path)
        except FileNotFoundError:
            self.config = None
            self.config_path = None
            self.logger.log(
                "🔧 Configuration file not found. Using defaults.", "WARNING"
            )

        # db_uri priority: ctor > config > None
        if not self.db_uri and self.config is not None:
            self.db_uri = getattr(self.config, "db_uri", None)

        self.db: Optional[Database] = None

        # Lazy caches
        self._settings_manager = None
        self._report_manager = None

        # Components will be attached by the facade (Biofilter)
        self.db_component = None
        self.settings = None
        self.etl = None
        self.report = None

        # Boot banner
        self.logger.log("════════════════════════════════════", "INFO")
        self.logger.log("🚀 Initializing Biofilter", "INFO")
        self.logger.log(f"   • Version: {self.version}", "INFO")
        self.logger.log(f"   • Debug mode: {self.debug_mode}", "INFO")
        self.logger.log(
            (
                f"   • Config: {self.config_path}"
                if self.config_path
                else "   • Config: <none>"
            ),
            "INFO",
        )
        self.logger.log(f"   • DB URI: {self._safe_db_uri(self.db_uri)}", "INFO")  # noqa E501
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
        bf.report.list()
        bf.report.run("gene_to_snp", {...})
    """

    def __init__(self, db_uri: str | None = None, debug_mode: bool = False):
        self.core = BiofilterCore(db_uri=db_uri, debug_mode=debug_mode)

        # Components
        self.db = DBComponent(self.core)
        if self.core.db_uri:
            self.db.connect()

        self.settings = SettingsComponent(self.core)
        self.etl = ETLComponent(self.core)

        self.report = ReportComponent(self.core)
        # Optional: expose components on core for internal cross-calls
        self.core.db_component = self.db
        self.core.settings = self.settings
        self.core.etl = self.etl
        self.core.report = self.report

    def __repr__(self) -> str:
        return f"<Biofilter(db_uri={self.core.db_uri})>"
