from sqlalchemy import (
    Column,
    Integer,
    String,
    UniqueConstraint,
    Text,
)  # ForeignKey                        # noqa E501

# from sqlalchemy.orm import relationship
from biofilter.db.base import Base


# =============================================================================
# VARIANTS DOMAIN MODELS
# =============================================================================

# NOTE: Future improvement: Promove Variants to the Entity level and transfer
#       this model to EntityRelationship

# NOTE: Future improvement: Activate the FKs

# NOTE: The full model to handle all types of variants (SNP, CNV, etc.) was
#       saved in backups


class GenomeAssembly(Base):
    __tablename__ = "genome_assemblies"

    id = Column(Integer, primary_key=True)
    accession = Column(
        String, unique=True, nullable=False
    )  # e.g., NC_000024.10                          # noqa E501
    assembly_name = Column(
        String, nullable=False
    )  # e.g., GRCh38.p14                            # noqa E501
    chromosome = Column(
        String, nullable=True
    )  # e.g., 1–22, X, Y, MT                        # noqa E501

    # Optional relationships
    # variants = relationship("Variant", back_populates="assembly", cascade="all, delete-orphan")           # noqa E501


class Variant(Base):
    __tablename__ = "variants"

    # id = Column(Integer, primary_key=True, autoincrement=True)
    # variant_id = Column(String)  # e.g., rsID (not unique globally)
    variant_id = Column(String, primary_key=True)  # e.g., rsID
    position = Column(Integer, nullable=False)
    assembly_id = Column(Integer, nullable=False)
    chromosome = Column(String)  # values like '1', 'X', 'Y', 'MT'
    ref = Column(Text)
    alt = Column(Text)
    data_source_id = Column(Integer, nullable=False)

    # assembly_id = Column(Integer, ForeignKey("genome_assemblies.id"), nullable=False)                     # noqa E501
    # data_source_id = Column(Integer, ForeignKey("etl_data_sources.id"), nullable=False)                   # noqa E501

    # Optional relationships
    # assembly = relationship("GenomeAssembly", back_populates="variants")
    # data_source = relationship("DataSource", back_populates="variants")
    # gene_links = relationship("VariantGeneRelationship", back_populates="variant", cascade="all, delete-orphan")  # noqa E501


class VariantGeneRelationship(Base):
    __tablename__ = "variant_gene_relationships"

    gene_id = Column(Integer, primary_key=True)
    variant_id = Column(String, primary_key=True)
    data_source_id = Column(Integer, nullable=False)
    # gene_id = Column(Integer, ForeignKey("genes.id"), nullable=False)
    # variant_id = Column(Integer, ForeignKey("variants.rs_id"), nullable=False)                            # noqa E501

    __table_args__ = (
        UniqueConstraint("gene_id", "variant_id", name="uq_gene_variant"),
    )
    # __table_args__ = (
    #     UniqueConstraint("gene_id", "variant_id", "data_source_id", name="uq_gene_variant"),
    # )

    # Optional relationships
    # gene = relationship("Gene", back_populates="variant_links")
    # variant = relationship("Variant", back_populates="gene_links")
