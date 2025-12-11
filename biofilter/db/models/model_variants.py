from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    ForeignKey,
    String,
    Float,
    Text,
)
from sqlalchemy.orm import relationship
from biofilter.db.base import Base
from biofilter.db.types import PKBigIntOrInt

"""
Biofilter3R v3.2.0 Note:

Starting in version 3.2.0, variants are no longer modeled as Entities.
Instead, the system adopts a lightweight SNP schema optimized for large
variant datasets. The `VariantSNP` table stores one row per rsID (as the
primary key), containing chromosome, positions, alleles, and provenance
metadata. Merged rsIDs are tracked separately in `VariantSNPMerge`.

This simplified design improves ingestion speed, reduces storage overhead,
and makes the database more suitable for dbSNP- and GWAS-scale variant
workflows. Gene - Variant relationships are now resolved through genomic
locations (`EntityLocation`) rather than Entity-level relationships.

Future releases may reintroduce richer variant Entity modeling when VEP
annotation layers are fully established.
"""


class VariantSNP(Base):

    __tablename__ = "variant_snps"

    # Natural primary key: numeric rsID (e.g., 123456 for rs123456)
    rs_id = Column(BigInteger, primary_key=True)
    # This is not necessary here
    # id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

    # Chromosome encoding:
    #   1..22 = autosomes
    #   23    = X
    #   24    = Y
    #   25    = MT
    chromosome = Column(Integer, nullable=False)

    # This version works only with SNV (no range).
    # Position in each build is optional: if the SNP is missing in a build,
    # the corresponding position is NULL.
    position_37 = Column(BigInteger, nullable=True)
    position_38 = Column(BigInteger, nullable=True)

    # SNV alleles. We allow some extra room for edge cases.
    reference_allele = Column(String(4), nullable=True)
    alternate_allele = Column(String(16), nullable=True)

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


class VariantSNPMerge(Base):

    __tablename__ = "variant_snp_merges"

    # Composite natural primary key
    rs_obsolete_id = Column(BigInteger, primary_key=True)
    rs_canonical_id = Column(BigInteger, primary_key=True)

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


class VariantGWAS(Base):
    """
    Flat representation of GWAS Catalog associations.

    This table hosts the raw + mapped data from the GWAS Catalog,
    joined with the EFO trait mapping file. It allows queries
    on variants, studies, and traits, even before full Entity integration.

    Future: link `variant_id`, `trait_id`, and `study_id` to Entities.
    """

    __tablename__ = "variant_gwas"

    # id = Column(BigInteger, primary_key=True, autoincrement=True)
    id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

    # Publication / study info
    pubmed_id = Column(String(255), index=True, nullable=True)
    # first_author = Column(String(255), nullable=True)
    # publication_date = Column(String(50), nullable=True)  # raw string for now  # noqa E501
    # journal = Column(String(255), nullable=True)
    # study_title = Column(Text, nullable=True)
    # link = Column(String(500), nullable=True)

    # Trait / phenotype mapping
    raw_trait = Column(String(255), nullable=True)  # "DISEASE/TRAIT" field  # noqa E501
    mapped_trait = Column(String(255), nullable=True)  # "EFO term"
    mapped_trait_id = Column(String(255), nullable=True)  # "EFO/MONDO ID"
    parent_trait = Column(String(255), nullable=True)  # Parent term
    parent_trait_id = Column(String(255), nullable=True)  # Parent URI ID

    # Variant info
    chr_id = Column(String(255), nullable=True)
    chr_pos = Column(Integer, nullable=True)
    reported_gene = Column(String(255), nullable=True)
    mapped_gene = Column(String(255), nullable=True)
    snp_id = Column(String(255), index=True, nullable=True)  # dbSNP ID (rsID)
    snp_risk_allele = Column(String(255), nullable=True)  # Qual a origem
    risk_allele_frequency = Column(Float, nullable=True)
    context = Column(String(255), nullable=True)
    intergenic = Column(String(255), nullable=True)

    # Statistics
    p_value = Column(Float, nullable=True)
    pvalue_mlog = Column(Float, nullable=True)
    odds_ratio_beta = Column(String(255), nullable=True)
    ci_text = Column(String(255), nullable=True)  # confidence interval raw

    # Sample sizes
    initial_sample_size = Column(Text, nullable=True)
    replication_sample_size = Column(Text, nullable=True)

    # Platform info
    platform = Column(String(255), nullable=True)
    cnv = Column(String(255), nullable=True)

    # Notes
    notes = Column(Text, nullable=True)

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


# =====================================================================
# V 3.2.0: Disabled Variants as Entities and develop SNP Model to start
# # --- Lookup: status (current, merged, withdrawn, suspect, etc.) ----
# # --- Canonical variant (one row per rsID) --------------------------
# class VariantMaster(Base):
#     """
#     Canonical variant (rsID) representation.

#     - One row per stable dbSNP rsID
#     - Stores canonical assembly, alleles, and quality
#     - Linked to Entity for cross-domain relations
#     """

#     __tablename__ = "variant_masters"

#     id = Column(BigInteger, primary_key=True, autoincrement=True)

#     # dbSNP rsID (stable external id)
#     # variant_id = Column(String(100), unique=True, index=True, nullable=False)  # noqa E501
#     rs_id = Column(String(100), unique=True, index=True, nullable=False)

#     variant_type = Column(String(16), nullable=False, default="SNP")

#     omic_status_id = Column(
#         Integer, ForeignKey("omic_status.id"), nullable=True
#     )  # noqa E501
#     omic_status = relationship("OmicStatus", passive_deletes=True)

#     chromosome = Column(String(10), nullable=True)  # '1'..'22','X','Y','MT'

#     quality = Column(Numeric(3, 1), nullable=True)

#     entity_id = Column(
#         BigInteger, ForeignKey("entities.id", ondelete="CASCADE"), nullable=False  # noqa E501
#     )  # noqa E501 Trocar
#     entity = relationship("Entity", passive_deletes=True)

#     data_source_id = Column(
#         Integer,
#         ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
#         nullable=True,
#     )
#     data_source = relationship("ETLDataSource", passive_deletes=True)

#     etl_package_id = Column(
#         Integer,
#         ForeignKey("etl_packages.id", ondelete="CASCADE"),
#         nullable=True,
#     )
#     etl_package = relationship("ETLPackage", passive_deletes=True)

#     loci = relationship(
#         "VariantLocus",
#         back_populates="variant",
#     )


# # --- Per-assembly locus index (accelerates position/range queries) ----------  # noqa E501
# class VariantLocus(Base):
#     """
#     Per-assembly locus index for a variant.

#     - Stores coordinates (assembly, chr, start, end)
#     - Supports multiple placements across assemblies
#     - Optimized for fast position/range queries
#     """

#     __tablename__ = "variant_loci"

#     id = Column(BigInteger, primary_key=True, autoincrement=True)

#     variant_id = Column(
#         BigInteger,
#         ForeignKey("variant_masters.id", ondelete="CASCADE"),
#         nullable=False,  # noqa E501
#     )

#     variant = relationship(
#         "VariantMaster",
#         back_populates="loci",
#         passive_deletes=True,
#     )

#     rs_id = Column(String(100), nullable=False)

#     entity_id = Column(
#         BigInteger, ForeignKey("entities.id", ondelete="CASCADE"), nullable=False  # noqa E501
#     )  # noqa E501 Trocar
#     entity = relationship("Entity", passive_deletes=True)

#     build = Column(String(10), nullable=False) # Here add a build alias as 37, 38  # noqa E501

#     assembly_id = Column(
#         Integer, ForeignKey("genome_assemblies.id"), nullable=False
#     )  # noqa E501
#     assembly = relationship("GenomeAssembly", passive_deletes=True)

#     chromosome = Column(String(10), nullable=False)  # '1'..'22','X','Y','MT'
#     start_pos = Column(BigInteger, nullable=False)
#     end_pos = Column(BigInteger, nullable=False)

#     reference_allele = Column(Text, nullable=True)
#     alternate_allele = Column(Text, nullable=True)

#     data_source_id = Column(
#         Integer,
#         ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
#         nullable=True,
#     )
#     data_source = relationship("ETLDataSource", passive_deletes=True)

#     etl_package_id = Column(
#         Integer,
#         ForeignKey("etl_packages.id", ondelete="CASCADE"),
#         nullable=True,
#     )
#     etl_package = relationship("ETLPackage", passive_deletes=True)
# V 3.2.0: Disabled Variants as Entities and develop a simple model to start performance  # noqa E501
# ======================================================================================  # noqa E501
