from sqlalchemy.orm import declarative_base

#: Everything 4.3.0 writes. `Base.metadata` is what `create_all` builds,
#: what `export_full_clone` exports, and what `_report_schema_drift`
#: compares a bundle against — so a table declared here is a promise that
#: some plan can fill it.
Base = declarative_base()
