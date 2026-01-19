# biofilter/db/types.py
from sqlalchemy.types import TypeDecorator, BigInteger, Integer


class PKBigIntOrInt(TypeDecorator):
    """
    Primary-key type that is:
        - SQLite: INTEGER (rowid-compatible, autoincrement)
        - Postgres / others: BIGINT

    Use it for surrogate PKs that you want to be BIGINT in production
    but still work correctly as autoincrement in SQLite.
    """

    impl = BigInteger
    cache_ok = True  # required for SQLAlchemy 1.4+

    def load_dialect_impl(self, dialect):
        # SQLite needs INTEGER PRIMARY KEY for real autoincrement behavior
        if dialect.name == "sqlite":
            return dialect.type_descriptor(Integer())
        # Postgres (and others) can safely use BIGINT
        return dialect.type_descriptor(BigInteger())
