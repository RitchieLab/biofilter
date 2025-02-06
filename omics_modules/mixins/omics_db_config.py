import os
import logging
from sqlalchemy import text, create_engine, inspect
from sqlalchemy.orm import sessionmaker
from models import Base


class DBConfigMixin:
    """
    Mixin for managing database initialization, integrity checks, and
    performance settings.
    """

    def initialize_database(self):
        """
        Initializes the database by creating tables if they do not exist.
        """
        if not os.path.exists(self._dbFile):
            self.logger.log("[INFO] Database not found. Creating new...")
            self._create_database()
        else:
            self.logger.log("[INFO] Database found. Checking integrity...")
            # Initialize engine and session
            if not self._check_database_integrity():
                self.logger.log("[ERROR] Database is corrupted or incompatible!", level="ERROR")
                raise Exception("Database corrupted or incompatible with models.")
            # Define autoflush dynamically based on updating mode
            self.autoflush = not self._updating  # True:SelectMode, False:InsertMode
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=self.autoflush,
                bind=self.engine
            )  # noqa: E501

            # Apply performance settings
            self.configure_database(
                updating=self._updating,
                temp_mem=self._temp_mem
            )  # noqa: E501

    def _create_database(self):
        """
        Creates a new database file and its tables.
        """
        self.engine = create_engine(self._dbURL, connect_args={"check_same_thread": False})
        Base.metadata.create_all(self.engine)
        self.logger.log("[INFO] Database created successfully!")

    def _check_database_integrity(self):
        """
        Verifies if all expected tables exist in the database.
        Returns True if integrity is valid, False otherwise.
        """
        self.engine = create_engine(self._dbURL, connect_args={"check_same_thread": False})
        inspector = inspect(self.engine)
        expected_tables = Base.metadata.tables.keys()
        existing_tables = inspector.get_table_names()

        missing_tables = set(expected_tables) - set(existing_tables)
        if missing_tables:
            self.logger.log(f"[ERROR] Missing tables: {missing_tables}", level="ERROR")
            return False

        self.logger.log("[INFO] Database integrity is valid.")
        return True

    def configure_database(self, updating=False, temp_mem=False):
        """
        Configures SQLite PRAGMAs for performance tuning.

        Args:
            updating (bool): If True, optimizes the database for bulk updates.
            temp_mem (bool): If True, stores temporary data in RAM.
        """
        try:
            self.logger.log("[INFO] Configuring database performance settings...")

            with self.engine.connect() as conn:
                conn.execute(text("PRAGMA page_size = 4096"))
                conn.execute(text("PRAGMA cache_size = -65536"))
                conn.execute(text("PRAGMA synchronous = OFF"))

                # NOTE / TODO Rever essa configuração
                # journal_mode = "MEMORY" if updating else "WAL"
                # conn.execute(text(f"PRAGMA journal_mode = {journal_mode}"))

                # if temp_mem:
                #     conn.execute(text("PRAGMA temp_store = MEMORY"))

                # locking_mode = "EXCLUSIVE" if updating else "NORMAL"
                # conn.execute(text(f"PRAGMA locking_mode = {locking_mode}"))

            self.logger.log("[INFO] Database configured successfully!")

        except Exception as e:
            self.logger.log(f"[ERROR] Database configuration failed: {e}")
            raise

    def drop_indexes(self):
        """
        Drops indexes to speed up bulk inserts.
        """
        try:
            self.logger.log("[INFO] Dropping database indexes before bulk updates...")

            with self.engine.connect() as conn:
                conn.execute(text("DROP INDEX IF EXISTS idx_snp_rs_current"))
                conn.execute(text("DROP INDEX IF EXISTS idx_gene_symbol"))
                conn.execute(text("DROP INDEX IF EXISTS idx_protein_uniprot_id"))

            self.logger.log("[INFO] Indexes dropped successfully!")

        except Exception as e:
            self.logger.log(f"[ERROR] Failed to drop indexes: {e}", logging.ERROR)
            raise

    def recreate_indexes(self):
        """
        Recreates indexes after bulk updates to optimize query performance.
        """
        try:
            self.logger.log("[INFO] Recreating database indexes...")

            with self.engine.connect() as conn:
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_snp_rs_current ON snps (rs_current)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_gene_symbol ON genes (symbol)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_protein_uniprot_id ON proteins (uniprot_id)"))

            self.logger.log("[INFO] Indexes recreated successfully!")

        except Exception as e:
            self.logger.log(f"[ERROR] Failed to recreate indexes: {e}", logging.ERROR)
            raise
