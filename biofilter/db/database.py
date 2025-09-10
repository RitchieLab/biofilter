import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from biofilter.utils.logger import Logger
from biofilter.db.create_db_mixin import CreateDBMixin


class Database(CreateDBMixin):
    def __init__(self, db_uri=None):
        self.logger = Logger(log_level="DEBUG")
        self.db_uri = db_uri
        self.engine = None
        self.session = None
        self.connected = False
        # TODO: Extend to other db parameters (e.g. user, password, etc.)

        if self.db_uri:
            self.connect()

    def _normalize_uri(self, uri: str) -> str:
        if "://" in uri:
            return uri
        return f"sqlite:///{os.path.abspath(uri)}"

    def connect(self, new_uri: str = None, check_exists=True):
        """
        Connect to the specified database.

        Args:
            new_uri (str): Optionally provide a new database URI.
            check_exists (bool): If True, raises error if database does not
            exist. Set to False during DB creation.
        """
        """
        Connect to the specified database.
        Can receive SQLite paths as files (e.g. 'biofilter.sqlite')
        or full URIs e.g. 'sqlite:///biofilter.sqlite', 'postgresql://...'
        """
        if new_uri:
            self.db_uri = new_uri

        self.db_uri = self._normalize_uri(self.db_uri)

        if check_exists and not self.exists_db():
            msn = f"Database not found at {self.db_uri}"
            self.logger.log(msn, "ERROR")
            raise ValueError(msn)

        self.engine = create_engine(self.db_uri, future=True)
        self.session = sessionmaker(bind=self.engine, future=True)

        # Be sure that setting is correct.
        # if self.session.bind.dialect.name == "sqlite":
        #     self.session.execute(text("PRAGMA journal_mode = DELETE;"))
        #     self.session.execute(text("PRAGMA foreign_keys = ON;"))
        #     self.session.commit()
        # if self.engine.dialect.name == "sqlite":
        #     with self.Session() as s:
        #         s.execute(text("PRAGMA journal_mode = DELETE;"))
        #         s.execute(text("PRAGMA foreign_keys = ON;"))
        #         s.commit()

        self.connected = True

    def exists_db(self):
        if not self.db_uri:
            msn = "Database URI must be set before connecting."
            self.logger.log(msn, "ERROR")
            return False  # or: raise ValueError(msn)
        if self.db_uri.startswith("sqlite:///"):
            path = self.db_uri.replace("sqlite:///", "")
            return Path(path).exists()
        # TODO: Add support for other DBs (e.g. Postgres)
        return False

    def get_session(self):
        if not self.session:
            msn = "⚠️ Database not connected. Call connect() first."
            self.logger.log(msn, "WARNING")
            return None
        return self.session()


"""
===============================================================================
Developer Note - Database Class
================================================================================

IMPORT: NEED TO BE UPDATE!!

This class manages the core database connection logic and session management
for the Biofilter system. It supports both SQLite and other
SQLAlchemy-compatible backends.

Key Responsibilities:
- Normalize and store the database URI
- Create SQLAlchemy Engine and Session factory
- Check if the database exists (only implemented for SQLite for now)
- Provide sessions via `get_session()`
- Integrate with CreateDBMixin to support schema creation and seeding

Usage:
>>> db = Database("sqlite:///biofilter.sqlite")
>>> with db.get_session() as session:
...     session.query(...)

types:
uri to SQLlite: "sqlite:///biofilter.sqlite"
uri to Postgres: "postgresql://user:pass@localhost/dbname""

Notes:
- Ensure all models are loaded before `create_all()`
- When adding support for Postgres or others, update `exists_db()`
- `connect(check_exists=True)` raises if the DB does not exist

See also: biofilter.db.create_db_mixin.CreateDBMixin

================================================================================
    Author: Andre Garon - Biofilter 3R
    Date: 2025-04
================================================================================
"""
