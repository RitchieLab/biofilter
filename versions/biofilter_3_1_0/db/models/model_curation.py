from sqlalchemy import Column, Integer, String, Enum, Text, BigInteger
from sqlalchemy import ForeignKey

# from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship

# from biofilter.db.models.etl_models import DataSource  # ou o caminho correto

# from sqlalchemy.sql import func
import enum

# Base = declarative_base()
from biofilter.db.base import Base


class OmicStatus(Base):
    """
    Represents the annotation status or review flag for an omics entity
    (e.g., Gene).

    Used in curation pipelines to mark reviewed, pending, or rejected entries.
    """

    __tablename__ = "omic_status"

    id = Column(Integer, primary_key=True)
    name = Column(String(30), unique=True, nullable=False)
    description = Column(String(100), nullable=True)


class ConflictStatus(enum.Enum):
    pending = "pending"
    resolved = "resolved"


class ConflictResolution(enum.Enum):
    keep_both = "keep_both"  # Ira manter os dois registro
    merge = "merge"  # Ira mesclar os dois registro
    delete = "delete"  # Ira deletar o novo registro


class CurationConflict(Base):
    __tablename__ = "curation_conflicts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id"), nullable=True
    )  # noqa E501
    data_source = relationship("ETLDataSource")
    entity_type = Column(String, nullable=False)  # Ex: "gene"
    entity_id = Column(BigInteger, nullable=True)
    identifier = Column(String, nullable=False)  # Ex: "HGNC:40594"
    existing_identifier = Column(String, nullable=False)  # Ex: "HGNC:58098"
    # status = Column(Enum(ConflictStatus), default=ConflictStatus.pending)
    # resolution = Column(Enum(ConflictResolution), nullable=True)
    status = Column(Enum(ConflictStatus, name="conflict_status_enum"), default=ConflictStatus.pending)
    resolution = Column(Enum(ConflictResolution, name="conflict_resolution_enum"), nullable=True)

    description = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)


"""
👇 Sample :
id	omic_type	identifier_type	identifier_value	item_1	    item_2	        status	    resolution	        notes               # noqa E501
1	gene	    entrez_id	    12345	            HGNC:A1BG	HGNC:A1BG-AS1	open		                    Same entrez ID      # noqa E501
2	gene	    ensembl_id	    ENSG00000100001	    HGNC:GENE1	HGNC:GENE2	    ignored	    allow duplicates	Curated manually    # noqa E501
"""
