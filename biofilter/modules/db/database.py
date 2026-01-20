from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, Optional

from sqlalchemy import Table, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import sessionmaker

from biofilter.modules.db.base import Base
from biofilter.modules.db.create_db_mixin import CreateDBMixin
from biofilter.utils.db_loader import bootstrap_models
from biofilter.utils.logger import Logger


class Database(CreateDBMixin):
    """
    Central DB access layer for Biofilter3R.

    Responsibilities:
    - Normalize & validate DB URI
    - Create SQLAlchemy Engine + Session factory
    - Bootstrap models (declarative + imperative Core tables) into Base.metadata
    - Provide a unified Table resolver (Core) via db.table("name")
    """

    def __init__(self, db_uri: Optional[str] = None, log_level: str = "DEBUG"):
        self.logger = Logger(log_level=log_level)
        self.db_uri: Optional[str] = db_uri

        self.engine: Optional[Engine] = None
        self.SessionLocal = None
        self.connected: bool = False

        # Cache of resolved SQLAlchemy Core Table objects
        self._tables: Dict[str, Table] = {}

        if self.db_uri:
            self.connect()

    # -------------------------------------------------------------------------
    # URI / Connection
    # -------------------------------------------------------------------------
    def _normalize_uri(self, uri: str) -> str:
        """
        If user passes a filesystem path (no scheme), treat it as sqlite:///path.
        """
        if "://" in uri:
            return uri
        return f"sqlite:///{os.path.abspath(uri)}"

    def connect(self, new_uri: Optional[str] = None, check_exists: bool = True) -> None:
        """
        Connect to database, bootstrap all models for this dialect, and prepare
        a session factory.

        - check_exists=True will attempt a lightweight connectivity check before
          finalizing the connection.
        """
        if new_uri:
            self.db_uri = new_uri

        if not self.db_uri:
            raise ValueError("db_uri must be provided to connect().")
        
        # Close previous engine (if any)
        if self.engine is not None:
            try:
                self.engine.dispose()
            except Exception:
                pass

        # Reset caches
        self._tables.clear()

        # Normalize uri
        self.db_uri = self._normalize_uri(self.db_uri)

        # Optional connectivity check BEFORE bootstrapping
        if check_exists and not self.exists_db():
            msg = f"❌ Database not found at {self.db_uri}"
            self.logger.log(msg, "ERROR")
            raise ValueError(msg)

        start = time.perf_counter()

        # Create engine
        self.engine = create_engine(self.db_uri, future=True)

        # CRITICAL: clear metadata AFTER we know we're switching engines
        # Base.metadata.clear()

        # Re-register everything for this engine/dialect
        bootstrap_models(self.engine)

        self.SessionLocal = sessionmaker(
            bind=self.engine,
            future=True,
            expire_on_commit=False,
        )

        elapsed_ms = (time.perf_counter() - start) * 1000

        # Safe URI info logging
        engine_name = host = db_name = "<unknown>"
        try:
            url = make_url(self.db_uri)
            engine_name = url.drivername
            if url.drivername.startswith("sqlite"):
                host = "local file"
                db_name = url.database
            else:
                host = url.host or "<unknown>"
                db_name = url.database or "<unknown>"
        except Exception:
            pass

        self.logger.log("🔌 Database connection established", "INFO")
        self.logger.log(f"   • Engine: {engine_name}", "INFO")
        self.logger.log(f"   • Host:   {host}", "INFO")
        self.logger.log(f"   • DB:     {db_name}", "INFO")
        self.logger.log(f"   • Time:   {elapsed_ms:.1f} ms", "INFO")
        self.logger.log("════════════════════════════════════", "INFO")

        self.connected = True

    def exists_db(self, new_db=False) -> bool:
        """
        Lightweight check:
        - SQLite: file exists
        - Postgres: SELECT 1 using a temporary engine if needed
        """
        if not self.db_uri:
            self.logger.log("Database URI must be set before connecting.", "ERROR")
            return False

        try:
            url = make_url(self._normalize_uri(self.db_uri))
        except Exception:
            self.logger.log("Invalid database URI.", "ERROR")
            return False

        # SQLite path existence check
        if url.drivername.startswith("sqlite"):
            path = url.database
            return bool(path) and Path(path).exists()

        # PostgreSQL connectivity check
        if url.drivername.startswith("postgresql"):
            temp_engine = None
            try:
                if self.engine is not None:
                    engine = self.engine
                else:
                    temp_engine = create_engine(url, future=True)
                    engine = temp_engine

                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))

                return True
            except Exception as e:
                if not new_db:
                    self.logger.log(f"Could not connect to database: {e}", "ERROR")
                return False
            finally:
                if temp_engine is not None:
                    temp_engine.dispose()

        self.logger.log("Unsupported database type for exists_db check.", "WARNING")
        return False

    # -------------------------------------------------------------------------
    # Sessions / Tables
    # -------------------------------------------------------------------------
    def get_session(self):
        if not self.SessionLocal:
            self.logger.log("⚠️ Database not connected. Call connect() first.", "WARNING")
            return None
        return self.SessionLocal()

    def table(self, name: str) -> Table:
        """
        Return a SQLAlchemy Core Table by name, using Base.metadata as the
        source of truth (populated by bootstrap_models).

        Falls back to reflection if the table isn't registered.
        """
        if not self.engine:
            raise RuntimeError("Database not connected. Call connect() first.")

        if name in self._tables:
            return self._tables[name]

        if name in Base.metadata.tables:
            t = Base.metadata.tables[name]
        else:
            # fallback: reflect from DB into Base.metadata
            t = Table(name, Base.metadata, autoload_with=self.engine)

        self._tables[name] = t
        return t
