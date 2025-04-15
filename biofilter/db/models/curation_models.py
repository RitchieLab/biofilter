from sqlalchemy import Column, Integer, String, Enum, Text

# from sqlalchemy.orm import declarative_base

# from sqlalchemy.sql import func
import enum

# Base = declarative_base()
from biofilter.db.base import Base


class ConflictStatus(enum.Enum):
    pending = "pending"
    resolved = "resolved"


class ConflictResolution(enum.Enum):
    keep_both = "keep_both"  # Ira manter os dois registro
    merge = "merge"  # Ira mesclar os dois registro
    delete = "delete"  # Ira deletar o novo registro


# class CurationConflict(Base):
#     __tablename__ = "curation_conflicts"

#     id = Column(Integer, primary_key=True, autoincrement=True)

#     entity_type = Column(String, nullable=False)  # Ex: "gene"
#     identifier_type = Column(String, nullable=False)  # Ex: "entrez_id"
#     identifier_value = Column(String, nullable=False)  # Ex: "12345"

#     item_exist = Column(String, nullable=False)  # Ex: "HGNC:A1BG"
#     item_new = Column(String, nullable=False)  # Ex: "HGNC:A1BG-AS1"

#     status = Column(Enum(ConflictStatus), default=ConflictStatus.pending)
#     resolution = Column(Enum(ConflictResolution), nullable=True)

#     notes = Column(Text, nullable=True)

#     # created_at = Column(DateTime(timezone=True), server_default=func.now())
#     # updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class CurationConflict(Base):
    __tablename__ = "curation_conflicts"

    id = Column(Integer, primary_key=True, autoincrement=True)

    entity_type = Column(String, nullable=False)  # Ex: "gene"
    identifier = Column(String, nullable=False)  # Ex: "HGNC:40594"
    existing_identifier = Column(String, nullable=False)  # Ex: "HGNC:58098"

    status = Column(Enum(ConflictStatus), default=ConflictStatus.pending)
    resolution = Column(Enum(ConflictResolution), nullable=True)

    description = Column(Text, nullable=True)
    # Ex: "Conflicting entrez_id=122526782 and ensembl_id=ENSG00000229280 with HGNC:58098"

    entity_id = Column(Integer, nullable=True)

    notes = Column(Text, nullable=True)


"""
👇 Sample :
id	omic_type	identifier_type	identifier_value	item_1	    item_2	        status	    resolution	        notes               # noqa E501
1	gene	    entrez_id	    12345	            HGNC:A1BG	HGNC:A1BG-AS1	open		                    Same entrez ID      # noqa E501
2	gene	    ensembl_id	    ENSG00000100001	    HGNC:GENE1	HGNC:GENE2	    ignored	    allow duplicates	Curated manually    # noqa E501
"""
