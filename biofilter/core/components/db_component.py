from __future__ import annotations

from typing import Optional

from biofilter.core.components.base_component import BaseComponent
from biofilter.modules.db.database import Database
from biofilter.utils.migrate import run_migration


class DBComponent(BaseComponent):
    """
    Database lifecycle component.

    Owns the ONLY place where core.db is created/replaced.
    All other components must consume core.db via core.require_db().
    """

    def connect(self, new_uri: Optional[str] = None) -> Database:
        if new_uri:
            self.core.db_uri = new_uri

        self.core.db = Database(self.core.db_uri)

        # Optional: eager session test (helps catch bad URIs early)
        # with self.core.db.get_session() as _:
        #     pass

        # self.core.logger.log(f"✅ Connected to DB: {self.core.db_uri}", "INFO")
        return self.core.db

    def create(self, db_uri: Optional[str] = None, overwrite: bool = False):
        """
        Create a new database file/schema and connect to it.

        Note:
        - Uses Database().create_db(), which is expected to bootstrap core tables/mappings.
        """
        if db_uri:
            self.core.db_uri = db_uri

        # Create DB using your existing Database/CreateDBMixin logic.
        db = Database()  # do not pass uri in ctor, following your current pattern
        db.db_uri = self.core.db_uri
        db.create_db(overwrite=overwrite)

        # Ensure this becomes the active shared instance (core tables/mappings live here)
        self.core.db = db

        self.core.logger.log(f"🏗️ Database created at: {self.core.db_uri}", "INFO")
        return True

    def migrate(self) -> bool:
        """
        Run migrations (Alembic or custom).
        """
        db = self.require_db()
        # You had: run_migration(self.db.session, self.db.db_uri)
        # We'll keep the same, but ensure we use the active shared db.
        run_migration(db.session, db.db_uri)
        self.core.logger.log("✅ Migration completed.", "INFO")
        return True

    def get_session(self):
        """
        Convenience passthrough to the shared Database session context manager.
        """
        return self.require_db().get_session()
