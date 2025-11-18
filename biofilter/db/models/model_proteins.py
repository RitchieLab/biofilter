from biofilter.db.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import (
    Column,
    BigInteger,
    Integer,
    String,
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
    pfam_acc = Column(String(50), unique=True, index=True, nullable=False)
    pfam_id = Column(String(50), index=True)
    description = Column(String(255))
    long_description = Column(String(255))
    type = Column(String(50))  # Domain, Family, Repeat, etc.
    source_database = Column(String(15))  # e.g., Prosite
    clan_acc = Column(String(50))
    clan_name = Column(String(50))

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=True,
    )
    data_source = relationship("ETLDataSource", passive_deletes=True)

    etl_package_id = Column(
        Integer,
        ForeignKey("etl_packages.id", ondelete="CASCADE"),
        nullable=True,
    )
    etl_package = relationship("ETLPackage", passive_deletes=True)

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
    function = Column(String(255))  # TODO BUG Ajustar 3.2.0
    location = Column(String(255))  # TODO BUG; Ajustar 3.2.0
    tissue_expression = Column(String(255))
    pseudogene_note = Column(String(255))

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=True,
    )
    data_source = relationship("ETLDataSource", passive_deletes=True)

    etl_package_id = Column(
        Integer,
        ForeignKey("etl_packages.id", ondelete="CASCADE"),
        nullable=True,
    )
    etl_package = relationship("ETLPackage", passive_deletes=True)

    # Relationships (Go Down)
    pfam_links = relationship(
        "ProteinPfamLink", back_populates="protein_master"
    )  # noqa E501
    protein_entity = relationship(
        "ProteinEntity", back_populates="protein_master"
    )  # noqa E501

    def __repr__(self):
        return f"<ProteinMaster(protein_id={self.protein_id})>"


class ProteinEntity(Base):
    """
    Links an Entity (e.g., gene symbol) to a ProteinMaster.
    Supports isoform annotations.
    """

    __tablename__ = "protein_entities"

    id = Column(Integer, primary_key=True, autoincrement=True)

    entity_id = Column(
        BigInteger, ForeignKey("entities.id", ondelete="CASCADE"), nullable=False  # noqa E501
    )  # noqa E501
    entity = relationship("Entity", passive_deletes=True)

    protein_id = Column(
        Integer, ForeignKey("protein_masters.id"), nullable=False
    )  # noqa E501
    protein_master = relationship("ProteinMaster", back_populates="protein_entity")   # noqa E501

    is_isoform = Column(Boolean, default=False, nullable=False)
    isoform_accession = Column(String(20), nullable=True)

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=True,
    )
    data_source = relationship("ETLDataSource", passive_deletes=True)

    etl_package_id = Column(
        Integer,
        ForeignKey("etl_packages.id", ondelete="CASCADE"),
        nullable=True,
    )
    etl_package = relationship("ETLPackage", passive_deletes=True)

    # __table_args__ = (
    #     PrimaryKeyConstraint("protein_id", "pfam_pk_id"),
    #     UniqueConstraint("entity_id", "protein_id"),  # em ProteinEntity
    # )


class ProteinPfamLink(Base):
    """
    Many-to-many table linking ProteinMaster entries with Pfam domains.
    Composite primary key ensures uniqueness of each link.
    """

    __tablename__ = "protein_pfam_links"

    protein_id = Column(
        Integer, ForeignKey("protein_masters.id"), nullable=False
    )  # noqa E501
    protein_master = relationship("ProteinMaster", back_populates="pfam_links")

    pfam_pk_id = Column(
        Integer,
        ForeignKey("protein_pfams.id", ondelete="CASCADE"),
        nullable=False,  # noqa E501
    )  # noqa E501
    pfam = relationship(
        "ProteinPfam",
        back_populates="protein_links",
        foreign_keys=[pfam_pk_id],  # noqa E501
    )  # noqa E501

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=True,
    )
    data_source = relationship("ETLDataSource", passive_deletes=True)

    etl_package_id = Column(
        Integer,
        ForeignKey("etl_packages.id", ondelete="CASCADE"),
        nullable=True,
    )
    etl_package = relationship("ETLPackage", passive_deletes=True)

    __table_args__ = (PrimaryKeyConstraint("protein_id", "pfam_pk_id"),)
