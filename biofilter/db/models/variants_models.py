from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from biofilter.db.base import Base


# =============================================================================
# VARIANTS DOMAIN MODELS
# =============================================================================

class VariantType(Base):
    __tablename__ = "variant_types"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)  # e.g., SNV, InDel, SV, MNV


class AlleleType(Base):
    __tablename__ = "allele_types"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)  # e.g., ref, sub, del, dup, rep, oth


class GenomeAssembly(Base):
    __tablename__ = "genome_assemblies"

    id = Column(Integer, primary_key=True)
    accession = Column(String, unique=True, nullable=False)   # e.g., NC_000024.10
    assembly_name = Column(String, nullable=False)            # e.g., GRCh38.p14
    chromosome = Column(String, nullable=True)                # e.g., 1–22, X, Y, MT


class Variant(Base):
    __tablename__ = "variants"

    id = Column(Integer, primary_key=True)
    # entity_id = Column(Integer, ForeignKey("entities.id"), unique=True, nullable=False)
    entity_id = Column(Integer, nullable=True)
    external_id = Column(String, nullable=True, index=True)  # e.g., rs2267
    variant_type_id = Column(Integer, ForeignKey("variant_types.id"), nullable=False)
    assembly_id = Column(Integer, ForeignKey("genome_assemblies.id"), nullable=False)
    data_source_id = Column(Integer, ForeignKey("etl_data_sources.id"), nullable=False)
    build_id = Column(Integer, nullable=True)  # dbSNP build (e.g., 157)

    # Relationships
    # entity = relationship("Entity", back_populates="variant")
    variant_type = relationship("VariantType")
    assembly = relationship("GenomeAssembly")
    data_source = relationship("DataSource")
    locations = relationship("VariantLocation", back_populates="variant", cascade="all, delete-orphan")
    gene_links = relationship("GeneVariantLink", back_populates="variant", cascade="all, delete-orphan")


class VariantLocation(Base):
    __tablename__ = "variant_locations"

    id = Column(Integer, primary_key=True)
    variant_id = Column(Integer, ForeignKey("variants.id"), nullable=False)
    assembly_id = Column(Integer, ForeignKey("genome_assemblies.id"), nullable=True)
    hgvs = Column(String, nullable=True)                     # e.g., NC_000024.10:g.41223094G>A
    position_base_1 = Column(Integer, nullable=True)         # Original SPDI position (1-based)
    position_start = Column(Integer, nullable=True)          # Start of the variant range
    position_end = Column(Integer, nullable=True)            # End of the variant range
    allele_type_id = Column(Integer, ForeignKey("allele_types.id"), nullable=True)
    allele = Column(String, nullable=True)                   # Inserted sequence or affected allele

    # Relationships
    variant = relationship("Variant", back_populates="locations")
    assembly = relationship("GenomeAssembly")
    allele_type = relationship("AlleleType")


# TODO: Check if keep this model or change to Entity Area
class GeneVariantLink(Base):
    __tablename__ = "gene_variant_links"

    id = Column(Integer, primary_key=True)
    gene_id = Column(Integer, ForeignKey("genes.id"), nullable=False)
    variant_id = Column(Integer, ForeignKey("variants.id"), nullable=False)

    # Optional: source information, or annotation type

    __table_args__ = (
        UniqueConstraint("gene_id", "variant_id", name="uq_gene_variant"),
    )

    gene = relationship("Gene", back_populates="variant_links")
    variant = relationship("Variant", back_populates="gene_links")