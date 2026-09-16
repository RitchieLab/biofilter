from sqlalchemy.orm import declarative_base

#: Everything 4.3.0 writes. `Base.metadata` is what `create_all` builds,
#: what `export_full_clone` exports, and what `_report_schema_drift`
#: compares a bundle against — so a table declared here is a promise that
#: some plan can fill it.
Base = declarative_base()

#: Tables no plan can fill any more, kept only because
#: `biofilter/modules/report_legacy/` still imports their classes and
#: is frozen until its reports are replaced.
#:
#: They live on their own metadata so `Base.metadata` stops listing
#: tables nothing writes. Declaring them beside the live schema made
#: `db verify --schema` report no drift while three of the tables it
#: compared could never exist — a clean result that meant less than it
#: appeared to, which is the failure this codebase keeps finding.
#:
#: Deleting the classes outright is the end state. It cannot happen yet:
#: two report modules import them at module level, so removing them stops
#: the whole report package from importing.
RetiredBase = declarative_base()
