from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from datetime import datetime
from biofilter.db.base import Base


# =============================================================================
# VARIANTS DOMAIN MODELS
# =============================================================================


class GenomeAssembly(Base):
    __tablename__ = "genome_assemblies"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)  # ex: "GRCh38"
    description = Column(
        String, nullable=True
    )  # "Genome Reference Consortium Human Build 38"


class Variant(Base):
    __tablename__ = "variants"

    id = Column(Integer, primary_key=True)
    entity_id = Column(Integer, ForeignKey("entities.id"), unique=True, nullable=False)
    rs_id = Column(String, nullable=True, index=True)
    variant_type = Column(String, nullable=False)  # SNP, InDel, SV, MNV, etc.
    # hgvs = Column(String, nullable=True)  # ex: NM_001301717.2:c.123A>G
    source = Column(String, nullable=True)  # dbSNP, ClinVar, Ensembl, etc.
    length = Column(Integer, nullable=True)

    entity = relationship("Entity", back_populates="variant")
    locations = relationship("VariantLocation", back_populates="variant")


class VariantLocation(Base):
    __tablename__ = "variant_locations"

    id = Column(Integer, primary_key=True)
    variant_id = Column(Integer, ForeignKey("variants.id"), nullable=False)
    assembly_id = Column(Integer, ForeignKey("genome_assemblies.id"), nullable=False)
    chromosome = Column(String, nullable=False)
    position = Column(Integer, nullable=False)  # 1-based position
    reference_allele = Column(String, nullable=False)
    alternate_allele = Column(String, nullable=False)

    variant = relationship("Variant", back_populates="locations")
    assembly = relationship("GenomeAssembly")


class VariantAnnotation(Base):
    __tablename__ = "variant_annotations"

    id = Column(Integer, primary_key=True)
    variant_id = Column(Integer, ForeignKey("variants.id"), nullable=False)
    gene_id = Column(Integer, ForeignKey("genes.id"), nullable=True)
    transcript_id = Column(String, nullable=True)
    effect = Column(String, nullable=True)  # missense, synonymous, etc.
    clinical_significance = Column(String, nullable=True)  # ex: pathogenic
    source = Column(String, nullable=True)  # ClinVar, gnomAD, etc.
    phenotype = Column(String, nullable=True)  # doença associada, se houver
    consequence = Column(String, nullable=True)  # ex: splice_acceptor_variant


class VariantHGVS(Base):
    __tablename__ = "variant_hgvs"

    id = Column(Integer, primary_key=True)
    variant_id = Column(Integer, ForeignKey("variants.id"), nullable=False)
    notation = Column(String, nullable=False)  # ex: NC_000017.10:g.41223094G>A
    level = Column(String, nullable=True)  # "g", "c", "p"
    reference = Column(String, nullable=True)  # ex: NM_000237, NP_000228
    version = Column(String, nullable=True)  # ex: 11, 3

    variant = relationship("Variant", backref="hgvs_notations")


class VariantLink(Base):
    __tablename__ = "variant_links"

    id = Column(Integer, primary_key=True)
    variant_id = Column(Integer, ForeignKey("variants.id"))
    related_entity_id = Column(Integer)  # Gene, Protein, Pathway, etc.
    related_entity_type = Column(String)  # gene, pathway, disease...
    relation_type = Column(String)  # ex: impacts, associated_with


# Merged Support
class VariantMergeLog(Base):
    __tablename__ = "variant_merge_log"

    id = Column(Integer, primary_key=True)
    old_rs_id = Column(String, nullable=False, index=True)
    new_rs_id = Column(String, nullable=False, index=True)
    source = Column(String, default="dbSNP")
    date_merged = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<VariantMergeLog {self.old_rs_id} → {self.new_rs_id}>"


# Liftover Support
class LiftedPosition(Base):
    __tablename__ = "lifted_positions"

    id = Column(Integer, primary_key=True)
    chromosome = Column(String, nullable=False)
    position_37 = Column(Integer, nullable=False, index=True)
    position_38 = Column(Integer, nullable=False, index=True)
    reference_allele = Column(String, nullable=True)
    alternate_allele = Column(String, nullable=True)

    def __repr__(self):
        return f"<LiftedPosition chr{self.chromosome} {self.position_37}→{self.position_38}>"
