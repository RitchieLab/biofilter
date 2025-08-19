from biofilter.db.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Boolean,
    PrimaryKeyConstraint,
)


class ProteinPfam(Base):
    """
    Stores Pfam protein family/domain definitions.
    Each Pfam entry is uniquely identified by `pfam_acc` (e.g., PF00067).
    """

    __tablename__ = "protein_pfams"

    id = Column(Integer, primary_key=True)
    pfam_acc = Column(String(10), unique=True, index=True, nullable=False)
    pfam_id = Column(String(20), index=True)
    description = Column(String(255))
    long_description = Column(String(255))
    type = Column(String(10))  # Domain, Family, Repeat, etc.
    source_database = Column(String(15))  # e.g., Prosite
    clan_acc = Column(String(15))
    clan_name = Column(String(15))

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=False,
    )

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,  # noqa E501
    )

    # Relationships (Go Up)
    data_source = relationship("DataSource", passive_deletes=True)

    # Relationships (Go Down)
    protein_links = relationship("ProteinPfamLink", back_populates="pfam")


class ProteinMaster(Base):
    """
    Stores protein records from UniProt or other protein databases.
    Each row represents a unique protein (canonical or not).
    """

    __tablename__ = "protein_masters"

    id = Column(Integer, primary_key=True)
    protein_id = Column(String(20), unique=True, index=True, nullable=False)
    function = Column(String(255))
    location = Column(String(100))
    tissue_expression = Column(String(255))
    pseudogene_note = Column(String(255))

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,  # noqa E501
    )

    # Relationships (Go Up)
    data_source = relationship("DataSource", passive_deletes=True)

    # Relationships (Go Down)
    pfam_links = relationship(
        "ProteinPfamLink", back_populates="protein_master"
    )  # noqa E501

    def __repr__(self):
        return f"<ProteinMaster(protein_id={self.protein_id})>"


class ProteinEntity(Base):
    """
    Links an Entity (e.g., gene symbol) to a ProteinMaster.
    Supports isoform annotations.
    """

    __tablename__ = "protein_entities"

    id = Column(Integer, primary_key=True)
    entity_id = Column(Integer, ForeignKey("entities.id"), nullable=False)
    protein_master_id = Column(
        Integer, ForeignKey("protein_masters.id"), nullable=False
    )  # noqa E501

    is_isoform = Column(Boolean, default=False, nullable=False)
    isoform_accession = Column(String(20), nullable=True)

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=False,
    )

    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    # Relationships (Go Up)
    protein_master = relationship("ProteinMaster")
    data_source = relationship("DataSource", passive_deletes=True)
    entity = relationship("Entity")


class ProteinPfamLink(Base):
    """
    Many-to-many table linking ProteinMaster entries with Pfam domains.
    Composite primary key ensures uniqueness of each link.
    """

    __tablename__ = "protein_pfam_links"

    protein_master_id = Column(
        Integer, ForeignKey("protein_masters.id"), nullable=False
    )  # noqa E501
    pfam_pk_id = Column(
        Integer,
        ForeignKey("protein_pfams.id", ondelete="CASCADE"),
        nullable=False,  # noqa E501
    )  # noqa E501
    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=False,
    )
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    __table_args__ = (PrimaryKeyConstraint("protein_master_id", "pfam_pk_id"),)

    # Relationships (Go Up)
    protein_master = relationship("ProteinMaster", back_populates="pfam_links")
    pfam = relationship(
        "ProteinPfam",
        back_populates="protein_links",
        foreign_keys=[pfam_pk_id],  # noqa E501
    )  # noqa E501
    data_source = relationship("DataSource", passive_deletes=True)
