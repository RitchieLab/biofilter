from sqlalchemy import Column, Text, Integer, String, ForeignKey, Boolean, PrimaryKeyConstraint
from sqlalchemy.orm import relationship
from biofilter.db.base import Base


class ProteinPfam(Base):
    __tablename__ = "protein_pfam"

    id = Column(Integer, primary_key=True)
    pfam_acc = Column(String, unique=True, index=True)   # e.g., PF00067
    pfam_id = Column(String, index=True)                 # e.g., p450
    description = Column(String)
    long_description = Column(Text)
    type = Column(String)                                # Domain, Family, Repeat, etc.
    source_database = Column(String)                     # e.g., Prosite
    clan_acc = Column(String)                            # e.g., CL0123
    clan_name = Column(String)

    data_source_id = Column(Integer, ForeignKey("etl_data_sources.id"), nullable=False)
    data_source = relationship("DataSource")


class ProteinMaster(Base):
    __tablename__ = "protein_master"

    id = Column(Integer, primary_key=True)
    protein_id = Column(String, unique=True, index=True)  # e.g., A0A087X1C5

    function = Column(Text)
    location = Column(String)
    tissue_expression = Column(Text)
    pseudogene_note = Column(Text)

    data_source_id = Column(Integer, ForeignKey("etl_data_sources.id"), nullable=False)
    data_source = relationship("DataSource")


class ProteinEntity(Base):
    __tablename__ = "protein_entity"

    id = Column(Integer, primary_key=True)
    # entity_id = Column(Integer, nullable=False)  # Ref to Entity.id
    entity_id = Column(Integer, nullable=False)
    protein_master_id = Column(Integer, ForeignKey("protein_master.id"), nullable=False)

    is_isoform = Column(Boolean, default=False, nullable=False)
    isoform_accession = Column(String, nullable=True)  # e.g., P12345-2

    data_source_id = Column(Integer, ForeignKey("etl_data_sources.id"), nullable=False)

    protein_master = relationship("ProteinMaster")
    data_source = relationship("DataSource")

    # entity = relationship("Entity")  # Optional: future FK


class ProteinPfamLink(Base):
    __tablename__ = "protein_pfam_link"

    protein_master_id = Column(Integer, ForeignKey("protein_master.id"), nullable=False)
    pfam_id = Column(Integer, ForeignKey("protein_pfam.id"), nullable=False)
    data_source_id = Column(Integer, ForeignKey("etl_data_sources.id"), nullable=False)

    __table_args__ = (PrimaryKeyConstraint("protein_master_id", "pfam_id"),)

    protein_master = relationship("ProteinMaster")
    pfam = relationship("ProteinPfam")
    data_source = relationship("DataSource")
