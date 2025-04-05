from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.orm import relationship
from biofilter.db.base import Base
import datetime


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


# GENE TABLES
class Gene(Base):
    __tablename__ = "genes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_id = Column(Integer, nullable=False)  # Entity.id (sem FK)
    hgnc_id = Column(String, nullable=True, unique=True)
    entrez_id = Column(String, nullable=True)
    ensembl_id = Column(String, nullable=True)
    vega_id = Column(String, nullable=True)
    ucsc_id = Column(String, nullable=True)
    symbol = Column(String, nullable=False)
    name = Column(String, nullable=True)
    location = Column(String, nullable=True)
    locus_group = Column(String, nullable=True)
    locus_type = Column(String, nullable=True)
    status = Column(String, nullable=True)
    gene_group_id = Column(Integer, nullable=True)  # FK opcional para GeneGroup
    date_approved = Column(String, nullable=True)
    date_modified = Column(String, nullable=True)
    date_symbol_changed = Column(String, nullable=True)
    date_name_changed = Column(String, nullable=True)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class GeneGroup(Base):
    __tablename__ = "gene_groups"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(String, nullable=True)


class GeneLocus(Base):
    __tablename__ = "gene_locus"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gene_id = Column(Integer, nullable=False)  # Gene.id (sem FK)
    location = Column(String, nullable=False)
    source = Column(String, nullable=True)  # UCSC, Ensembl, etc.
    version = Column(String, nullable=True)
    created_at = Column(DateTime, default=utcnow)
