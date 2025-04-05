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
            check_exists (bool): If True, raises error if database does not exist.
                                Set to False during DB creation.
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

    # def create_db(self, overwrite: bool = False) -> bool:
    #     if self.exists_db() and not overwrite:
    #         print(f"🛑 Database already exists at {self.uri}")
    #         return False

    #     self.connect()

    #     import biofilter.db.models.config_models
    #     import biofilter.db.models.etl_models
    #     import biofilter.db.models.loki_models
    #     import biofilter.db.models.omics_models
    #     import biofilter.db.models.relationship_models

    #     self.connect()
    #     Base.metadata.create_all(self.engine)

    #     self.seed_all()

    #     print(f"✅ Database created at {self.uri}")
    #     return True

    # def seed_all(self):
    #     self.seed_initial_data()
    #     self.seed_settings()

    # def seed_settings(self, json_path=None):
    #     import os
    #     import json
    #     from biofilter.db.models.config_models import SystemConfig
    #     from sqlalchemy.exc import IntegrityError

    #     # Resolve path dinamicamente
    #     if json_path is None:
    #         current_dir = os.path.dirname(os.path.abspath(__file__))
    #         json_path = os.path.join(current_dir, "seed", "initial_config.json")

    #     if not self.engine:
    #         raise RuntimeError("Database not connected")

    #     with self.get_session() as session:
    #         with open(json_path, "r") as f:
    #             data = json.load(f)

    #         for setting in data:
    #             session.add(SystemConfig(**setting))

    #         try:
    #             session.commit()
    #             print("✅ Initial data seeded.")
    #         except IntegrityError:
    #             session.rollback()
    #             print("⚠️ Initial data already exists. Skipping.")

    # # def seed_initial_data(self, json_path="db/seed/initial_data.json"):
    # def seed_initial_data(self, json_path=None):
    #     from biofilter.db.models.etl_models import DataSource
    #     from sqlalchemy.exc import IntegrityError

    #     # Resolve path dinamicamente
    #     if json_path is None:
    #         current_dir = os.path.dirname(os.path.abspath(__file__))
    #         json_path = os.path.join(current_dir, "seed", "initial_data.json")

    #     if not self.engine:
    #         raise RuntimeError("Database not connected")

    #     with self.get_session() as session:
    #         with open(json_path, "r") as f:
    #             data = json.load(f)

    #         for ds in data.get("data_sources", []):
    #             session.add(DataSource(**ds))

    #         try:
    #             session.commit()
    #             print("✅ Initial data seeded.")
    #         except IntegrityError:
    #             session.rollback()
    #             print("⚠️ Initial data already exists. Skipping.")


"""
uri to SQLlite: "sqlite:///biofilter.sqlite"
uri to Postgres: "postgresql://user:pass@localhost/dbname""
"""
