from sqlalchemy import BigInteger, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from biofilter.modules.db.base import Base


class GeneGroup(Base):
    """
    Represents a functional or curated group of genes.

    Examples include predefined biological groups (e.g., 'HOX Cluster',
    Mitochondrial Genes'). Each group can be associated with multiple genes
    through the GeneGroupMembership table.

    # To avoit N+1 query:
    from sqlalchemy.orm import selectinload
    group = session.query(GeneGroup).options(
        selectinload(GeneGroup.memberships).selectinload(GeneGroupMembership.gene)
    ).filter_by(name="HOX Cluster").first()
    """

    __tablename__ = "gene_groups"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(String(255), nullable=True)

    # Denormalized for fast deletion/filter
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

    memberships = relationship(
        "GeneGroupMembership",
        back_populates="group",
        cascade="all, delete-orphan",  # para que ser?
    )

    def __repr__(self):
        return f"<GeneGroup(name={self.name})>"


class GeneLocusGroup(Base):
    """
    Represents the HGNC 'Locus Group' classification of a gene.

    Examples: 'protein-coding gene', 'RNA gene', 'pseudogene'.
    Used to group genes based on function or biotype.
    """

    __tablename__ = "gene_locus_groups"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(String(100), nullable=True)

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
    genes = relationship("GeneMaster", back_populates="gene_locus_group")  # noqa E501

    def __repr__(self):
        return f"<GeneLocusGroup(name={self.name})>"


class GeneLocusType(Base):
    """
    Represents the HGNC 'Locus Type' classification.

    Describes the structural or detailed category of the gene (e.g., 'miRNA',
    snRNA', 'pseudogene').
    """

    __tablename__ = "gene_locus_types"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(String(100), nullable=True)

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
    genes = relationship("GeneMaster", back_populates="gene_locus_type")  # noqa E501

    def __repr__(self):
        return f"<GeneLocusType(name={self.name})>"


# =============================================================================
# MAIN GENE MODEL
# =============================================================================


class GeneMaster(Base):
    """
    Main table for gene metadata and identifiers.

    This model links each gene to a unique Entity and stores standard IDs such
    as HGNC, Entrez, and Ensembl. It also holds references to curated locus
    type/group, genomic locations, and source tracking.
    """

    __tablename__ = "gene_masters"

    id = Column(Integer, primary_key=True, autoincrement=True)

    symbol = Column(String(64), nullable=True, index=True)

    hgnc_status = Column(
        String(50), nullable=True
    )  # Ex: "Approved", "Symbol Approved"        # noqa E501

    omic_status_id = Column(
        Integer,
        ForeignKey("omic_status.id", ondelete="SET NULL"),
        nullable=True,  # noqa E501
    )  # noqa E501
    omic_status = relationship("OmicStatus")

    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,  # noqa E501
    )  # noqa E501
    entity = relationship("Entity", passive_deletes=True)

    chromosome = Column(String(5), nullable=True)

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

    # Functional category
    locus_group_id = Column(
        Integer,
        ForeignKey("gene_locus_groups.id", ondelete="SET NULL"),
        nullable=True,  # noqa E501
    )  # noqa E501
    gene_locus_group = relationship("GeneLocusGroup", back_populates="genes")

    locus_type_id = Column(
        Integer,
        ForeignKey("gene_locus_types.id", ondelete="SET NULL"),
        nullable=True,  # noqa E501
    )  # noqa E501
    gene_locus_type = relationship("GeneLocusType", back_populates="genes")

    # Relational M:N with functional groups
    group_memberships = relationship(
        "GeneGroupMembership",
        back_populates="gene",
        cascade="all, delete-orphan",  # noqa E501
    )

    def __repr__(self):
        return f"<Gene(entity_id={self.entity_id}, Symbol ID={self.symbol}, Status={self.hgnc_status})>"  # noqa E501


# =============================================================================
# RELATION M:N BETWEEN GENE AND GROUP
# =============================================================================


class GeneGroupMembership(Base):
    """
    Intermediate table for many-to-many relationship between GeneMaster and
    GeneGroup.

    Ensures a gene can belong to multiple groups and each group can include
    multiple genes. Includes timestamps and source tracking for ETL
    traceability.
    """

    __tablename__ = "gene_group_memberships"

    gene_id = Column(Integer, ForeignKey("gene_masters.id"), primary_key=True)
    gene = relationship("GeneMaster", back_populates="group_memberships")

    group_id = Column(Integer, ForeignKey("gene_groups.id"), primary_key=True)
    group = relationship("GeneGroup", back_populates="memberships")

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

    def __repr__(self):
        return f"<GeneGroupMembership(gene_id={self.gene_id}, group_id={self.group_id})>"  # noqa E501
