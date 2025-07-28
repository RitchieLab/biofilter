from sqlalchemy import text


class DBTuningMixin:
    """
    Mixin to apply database optimizations for bulk insert operations.
    Currently supports SQLite and creates indexes for SQLite/PostgreSQL.
    """

    def db_write_mode(self):
        """
        Apply SQLite-specific PRAGMA settings to optimize for bulk insert.
        Does nothing if not using SQLite.
        """
        if self.session.bind.dialect.name != "sqlite":
            return

        self.logger.log(
            "⚙️ Applying SQLite PRAGMA optimizations for bulk insert", "DEBUG"
        )  # noqa E501

        # self.session.execute(text("PRAGMA journal_mode = WAL;"))
        self.session.execute(text("PRAGMA journal_mode = DELETE;"))
        self.session.execute(text("PRAGMA synchronous = NORMAL;"))
        self.session.execute(text("PRAGMA locking_mode = EXCLUSIVE;"))
        self.session.execute(text("PRAGMA temp_store = MEMORY;"))
        self.session.execute(
            text("PRAGMA cache_size = -100000;")
        )  # ~100MB of memory cache  # noqa E501
        self.session.execute(
            text("PRAGMA foreign_keys = OFF;")
        )  # Temporarily disable FK checks  # noqa E501
        self.session.commit()

        # DEBUG MODE
        # mode = self.session.execute(text("PRAGMA journal_mode")).scalar()
        # self.logger.log(f"🧾 Current journal_mode: {mode}", "DEBUG")

    def db_read_mode(self):
        """
        Reset SQLite PRAGMAs to default values after bulk insert.
        Does nothing if not using SQLite.
        """
        if self.session.bind.dialect.name != "sqlite":
            return

        self.logger.log(
            "🔄 Resetting SQLite PRAGMAs to default settings", "DEBUG"
        )  # noqa E501

        self.session.execute(text("PRAGMA journal_mode = DELETE;"))
        self.session.execute(text("PRAGMA synchronous = FULL;"))
        self.session.execute(text("PRAGMA locking_mode = NORMAL;"))
        self.session.execute(text("PRAGMA foreign_keys = ON;"))
        self.session.commit()

    def create_indexes(self, index_specs: list[tuple[str, list[str]]]):
        """
        Create indexes on the database to speed up queries.

        Parameters:
            index_specs: List of tuples (table_name, [column1, column2, ...])
        """
        engine = self.session.bind.dialect.name
        if engine not in ("sqlite", "postgresql"):
            self.logger.log(
                f"❌ Index creation not supported for engine: {engine}", "WARNING"
            )  # noqa E501
            return

        for table, columns in index_specs:
            index_name = f"idx_{table}_{'_'.join(columns)}"
            col_str = ", ".join(columns)
            sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({col_str});"  # noqa E501
            self.session.execute(text(sql))
            self.logger.log(f"📌 Created index: {index_name}", "DEBUG")

        self.session.commit()

    def drop_indexes(self, index_specs: list[tuple[str, list[str]]]):
        """
        Drop indexes based on the provided table/column specs.

        Parameters:
            index_specs: List of tuples (table_name, [column1, column2, ...])
        """
        engine = self.session.bind.dialect.name
        if engine not in ("sqlite", "postgresql"):
            self.logger.log(
                f"❌ Index removal not supported for engine: {engine}", "WARNING"
            )  # noqa E501
            return

        for table, columns in index_specs:
            index_name = f"idx_{table}_{'_'.join(columns)}"

            if engine == "sqlite":
                sql = f"DROP INDEX IF EXISTS {index_name};"
            elif engine == "postgresql":
                sql = f'DROP INDEX IF EXISTS "{index_name}";'

            self.session.execute(text(sql))

            self.logger.log(f"🗑️ Droped index: {index_name}", "DEBUG")  # noqa E501

        self.session.commit()


"""
# 🧠 How to use in DTP.load()
def load(self):
    INDEX_SPECS = [
        ("gene", ["entity_id"]),
        ("gene", ["hgnc_id", "ensembl_id", "entrez_id"]),
    ]

    self.db_write_mode()
    self.drop_indexes(self.INDEX_SPECS)

    # ... Logic ...

    self.create_indexes(self.INDEX_SPECS)
    self.db_read_mode()
"""
