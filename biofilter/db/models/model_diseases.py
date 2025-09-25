from biofilter.db.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, ForeignKey, Text


class DiseaseGroup(Base):
    """
    Reference table for disease subsets (tags).
    Example: rare, gard_rare, nord_rare, otar.
    """
    __tablename__ = "disease_groups"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)
    description = Column(String(255), nullable=True)

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

    # Relationships
    memberships = relationship(
        "DiseaseGroupMembership", back_populates="group", cascade="all, delete-orphan"
    )


class DiseaseGroupMembership(Base):
    """
    Linking table between DiseaseMaster and DiseaseGroup.
    One disease can have multiple groups, and each group can apply to many diseases.
    """
    __tablename__ = "disease_group_memberships"

    id = Column(Integer, primary_key=True, autoincrement=True)

    disease_id = Column(Integer, ForeignKey("disease_masters.id", ondelete="CASCADE"))
    disease = relationship("DiseaseMaster", back_populates="group_memberships")

    group_id = Column(Integer, ForeignKey("disease_groups.id", ondelete="CASCADE"))
    group = relationship("DiseaseGroup", back_populates="memberships")

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


class DiseaseMaster(Base):
    """
    Canonical representation of diseases in Biofilter3R.

    Each disease is linked to a unique Biofilter Entity (`entity_id`) and
    identified by a MONDO ID (preferred primary identifier).
    The description provides a human-readable label or definition.
    Provenance is tracked via the originating DataSource and ETLPackage.

    Relationships:
        - entity: Unique entity representation for the disease
        - group_memberships: Links to DiseaseGroup through DiseaseGroupMembership
    """

    __tablename__ = "disease_masters"

    id = Column(Integer, primary_key=True, autoincrement=True)

    disease_id = Column(String(50), nullable=False, index=True, unique=True)
    label = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)

    # Omic status (like in GeneMaster)
    omic_status_id = Column(
        Integer,
        ForeignKey("omic_status.id", ondelete="SET NULL"),
        nullable=True,
    )
    omic_status = relationship("OmicStatus")

    # Links to the central Entity table
    entity_id = Column(
        Integer, ForeignKey("entities.id", ondelete="CASCADE"), nullable=False
    )
    entity = relationship("Entity", passive_deletes=True)

    # Provenance
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

    # Relationships
    group_memberships = relationship(
        "DiseaseGroupMembership", back_populates="disease", cascade="all, delete-orphan"
    )
