from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from biofilter.db.base import Base
import datetime


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


# =============================================================================
# FUNCTIONAL CATEGORIZATION
# =============================================================================


class GeneGroup(Base):
    __tablename__ = "gene_groups"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(String, nullable=True)

    genes = relationship(
        "Gene", secondary="gene_group_membership", back_populates="groups"
    )  # noqa E501

    def __repr__(self):
        return f"<GeneGroup(name={self.name})>"


class LocusGroup(Base):
    __tablename__ = "locus_groups"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(String, nullable=True)

    genes = relationship("Gene", back_populates="locus_group")

    def __repr__(self):
        return f"<LocusGroup(name={self.name})>"


class LocusType(Base):
    __tablename__ = "locus_types"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(String, nullable=True)

    genes = relationship("Gene", back_populates="locus_type")

    def __repr__(self):
        return f"<LocusType(name={self.name})>"


# =============================================================================
# CITOGENETIC REGION
# =============================================================================


class GenomicRegion(Base):
    __tablename__ = "genomic_regions"

    id = Column(Integer, primary_key=True)
    label = Column(String, unique=True, nullable=False)  # Ex: "12p13.31"
    chromosome = Column(String, nullable=True)
    start = Column(Integer, nullable=True)
    end = Column(Integer, nullable=True)
    description = Column(String)

    locations = relationship("GeneLocation", back_populates="region")

    def __repr__(self):
        return f"<GenomicRegion(label={self.label})>"


# =============================================================================
# MAIN GENE MODEL
# =============================================================================


class OmicStatus(Base):
    __tablename__ = "omic_status"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(String, nullable=True)


class Gene(Base):
    __tablename__ = "genes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    omic_status_id = Column(
        Integer, ForeignKey("omic_status.id"), nullable=True
    )  # noqa E501
    omic_status = relationship("OmicStatus")

    entity_id = Column(Integer, nullable=False)

    # NOTE / TODO: Improve this field to save space and standardize with other sources
    hgnc_status = Column(String, nullable=True)  # Ex: "Approved", "Symbol Approved"

    hgnc_id = Column(String, unique=True, nullable=True)
    entrez_id = Column(String, nullable=True)
    ensembl_id = Column(String, nullable=True)

    data_source_id = Column(Integer, nullable=True)

    # Functional category
    locus_group_id = Column(
        Integer, ForeignKey("locus_groups.id"), nullable=True
    )  # noqa E501
    locus_type_id = Column(
        Integer, ForeignKey("locus_types.id"), nullable=True
    )  # noqa E501

    locus_group = relationship("LocusGroup", back_populates="genes")
    locus_type = relationship("LocusType", back_populates="genes")

    # Relational M:N with functional groups
    groups = relationship(
        "GeneGroup", secondary="gene_group_membership", back_populates="genes"
    )  # noqa E501
    locations = relationship(
        "GeneLocation", back_populates="gene", cascade="all, delete-orphan"
    )  # noqa E501
    variant_links = relationship("GeneVariantLink", back_populates="gene", cascade="all, delete-orphan")

    # Audit fields
    # created_at = Column(DateTime, default=utcnow)
    # updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

    def __repr__(self):
        return f"<Gene(hgnc_id={self.hgnc_id}, entity_id={self.entity_id})>"


# =============================================================================
# RELATION M:N BETWEEN GENE AND GROUP
# =============================================================================


class GeneGroupMembership(Base):
    __tablename__ = "gene_group_membership"

    gene_id = Column(Integer, ForeignKey("genes.id"), primary_key=True)
    group_id = Column(Integer, ForeignKey("gene_groups.id"), primary_key=True)

    def __repr__(self):
        return f"<GeneGroupMembership(gene_id={self.gene_id}, group_id={self.group_id})>"  # noqa E501


# =============================================================================
# GENOMIC POSITION
# =============================================================================


class GeneLocation(Base):
    __tablename__ = "gene_locations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gene_id = Column(Integer, ForeignKey("genes.id"), nullable=False)

    chromosome = Column(String, nullable=True)
    start = Column(Integer, nullable=True)
    end = Column(Integer, nullable=True)
    strand = Column(String, nullable=True)  # Ex: "+", "-"

    assembly = Column(String, nullable=True, default="GRCh38")
    region_id = Column(
        Integer, ForeignKey("genomic_regions.id"), nullable=True
    )  # noqa E501
    data_source_id = Column(Integer, nullable=True)

    region = relationship("GenomicRegion", back_populates="locations")
    gene = relationship("Gene", back_populates="locations")

    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

    def __repr__(self):
        return f"<GeneLocation(gene_id={self.gene_id}, chr={self.chromosome}, start={self.start})>"  # noqa E501


"""
================================================================================
Developer Note - OMICS Core Models (Change to Genes Models???)
================================================================================
Developer Note:
This model represents the many-to-many relationship between Genes and
GeneGroups.

Motivation:
- A single gene can belong to multiple functional or structural groups
    (e.g., RNA-binding, Kinases).
- Conversely, a GeneGroup can contain many genes.

Implementation:
- This table uses a composite primary key (gene_id, group_id) to ensure
    uniqueness of each link.
- The relationship is configured via `secondary` in both Gene and GeneGroup
    models.
- Direct use of this model is optional — appending to `gene.groups` or
    `group.genes` automatically creates entries.

Constraints:
- Duplicates are not allowed by design; attempting to insert the same
    gene-group link twice will raise an IntegrityError.
- Deleting a gene or group will not cascade delete this entry unless
    configured with ON DELETE CASCADE at DB level (optional).

Recommendations:
- Prefer using ORM relationships (`gene.groups.append(group)`) unless you need
    low-level control.
- Always commit the session after adding relationships to ensure database
    integrity.

Future Improvements:
- A timestamp field (e.g., `added_at`) can be added if tracking relationship
    creation time is needed.
- A `data_source_id` field could also be useful to trace whether the link was
    added via ETL, manual curation, or an external tool.
- Consider changing the name of this model to `Gene` for clarity, and open
    other files.

================================================================================
    Author: Andre Garon - Biofilter 3R
    Date: 2025-04
================================================================================
"""
