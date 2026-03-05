from sqlalchemy import (
    Float,
    Text,
    Table,
    Column,
    Integer,
    BigInteger,
    String,
    ForeignKey,
    UniqueConstraint,
    PrimaryKeyConstraint,
    MetaData,
)
from sqlalchemy.orm import relationship
from sqlalchemy import Identity

from biofilter.modules.db.base import Base
from biofilter.modules.db.types import PKBigIntOrInt

# # This model has rsID as PK because the group asked to keep only dbSNP as Source
# # and now they need more sources
# class VariantSNP(Base):
#     __tablename__ = "variant_snps"

#     # Natural primary key: numeric rsID (e.g., 123456 for rs123456)
#     rs_id = Column(BigInteger, primary_key=True)
#     # This is not necessary here
#     # id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

#     # Chromosome encoding:
#     #   1..22 = autosomes
#     #   23    = X
#     #   24    = Y
#     #   25    = MT
#     chromosome = Column(Integer, nullable=False)

#     # This version works only with SNV (no range).
#     # Position in each build is optional: if the SNP is missing in a build,
#     # the corresponding position is NULL.
#     position_37 = Column(BigInteger, nullable=True)
#     position_38 = Column(BigInteger, nullable=True)

#     # SNV alleles. We allow some extra room for edge cases.
#     reference_allele = Column(String(4), nullable=True)
#     alternate_allele = Column(String(16), nullable=True)

#     # Provenance
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
# class VariantSNP(Base):
#     __tablename__ = "variant_snps"

#     # Partition key
#     chromosome = Column(Integer, nullable=False)

#     # Identity / autoincrement (works well on Postgres; SQLite will emulate)
#     id = Column(PKBigIntOrInt, autoincrement=True, nullable=False)

#     source_type = Column(String(20), nullable=False)   # e.g. "rs"
#     source_id = Column(BigInteger, nullable=False)     # numeric part (e.g. 123 for rs123)

#     position_37 = Column(BigInteger, nullable=True)
#     position_38 = Column(BigInteger, nullable=True)
#     position_other = Column(BigInteger, nullable=True)

#     reference_allele = Column(String(4), nullable=True)
#     alternate_allele = Column(String(16), nullable=True)

#     # Provenance
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

#     __table_args__ = (
#         # Must include the partition key in PK on Postgres
#         PrimaryKeyConstraint("chromosome", "id", name="pk_variant_snps"),
#         # Uniqueness that matches your lookup semantics and partitioning
#         UniqueConstraint(
#             "chromosome", "source_type", "source_id",
#             name="uq_variant_snps_chr_source"
#         ),
#     )

#     # IMPORT: This model is create table by DB management because partitions



# def map_variant_snp(engine, metadata: MetaData):
#     """
#     Ensure Table('variant_snps') exists in the provided metadata for ALL dialects.

#     This table is accessed via SQLAlchemy Core in both SQLite and PostgreSQL.
#     Safe to call multiple times.
#     """
#     dialect = engine.dialect.name
#     is_sqlite = dialect == "sqlite"

#     id_col = (
#         Column(
#             "id",
#             Integer,
#             primary_key=True,
#             autoincrement=True,
#             nullable=False,
#         )
#         if is_sqlite
#         else Column(
#             "id",
#             BigInteger,
#             nullable=False,
#             server_default=Identity(always=False),
#             # No autoincrement Here
#         )
#     )

#     # 1) Ensure Table is registered in metadata (for both SQLite and Postgres)
#     if "variant_snps" in metadata.tables:
#         variant_snps = metadata.tables["variant_snps"]
#     else:
#         variant_snps = Table(
#             "variant_snps",
#             metadata,

#             # Keep chromosome as a normal column always
#             Column("chromosome", Integer, nullable=False),

#             # SQLite: INTEGER PRIMARY KEY => autoincrement behavior
#             # Postgres: bigint identity handled by raw DDL; here we declare BigInteger and keep nullable=False.
#             # Column(
#             #     "id",
#             #     Integer if is_sqlite else BigInteger,
#             #     primary_key=is_sqlite,      # SQLite: PK is only id
#             #     autoincrement=is_sqlite,    # SQLite: true autoincrement
#             #     nullable=False,
#             # ),
#             id_col,

#             Column("source_type", String(20), nullable=False),
#             Column("source_id", BigInteger, nullable=False),

#             Column("position_37", BigInteger, nullable=True),
#             Column("position_38", BigInteger, nullable=True),
#             Column("position_other", BigInteger, nullable=True),

#             Column("reference_allele", String(4), nullable=True),
#             Column("alternate_allele", String(16), nullable=True),

#             Column("data_source_id", Integer, ForeignKey("etl_data_sources.id", ondelete="CASCADE"), nullable=True),
#             Column("etl_package_id", Integer, ForeignKey("etl_packages.id", ondelete="CASCADE"), nullable=True),

#             # Postgres: declare composite PK in metadata (matches your partitioned parent DDL).
#             # SQLite: cannot have autoincrement on composite PK, so we omit this constraint there.
#             *((
#                 PrimaryKeyConstraint("chromosome", "id", name="pk_variant_snps"),
#             ) if not is_sqlite else ()),

#             UniqueConstraint("chromosome", "source_type", "source_id", name="uq_variant_snps_chr_source"),
#         )

#     return variant_snps


def map_variant_snp(engine, metadata: MetaData):
    dialect = engine.dialect.name
    is_sqlite = dialect == "sqlite"

    if "variant_snps" in metadata.tables:
        return metadata.tables["variant_snps"]

    if is_sqlite:
        # SQLite: SINGLE PK autoincrement
        variant_snps = Table(
            "variant_snps",
            metadata,
            Column("chromosome", Integer, nullable=False),
            Column("id", Integer, primary_key=True, autoincrement=True),  # ✅ must be INTEGER
            Column("source_type", String(20), nullable=False),
            Column("source_id", BigInteger, nullable=False),
            Column("position_37", BigInteger, nullable=True),
            Column("position_38", BigInteger, nullable=True),
            Column("position_other", BigInteger, nullable=True),
            Column("reference_allele", String(4), nullable=True),
            Column("alternate_allele", String(16), nullable=True),
            Column("data_source_id", Integer, nullable=True),
            Column("etl_package_id", Integer, nullable=True),
            # Column("data_source_id", Integer, ForeignKey("etl_data_sources.id", ondelete="CASCADE"), nullable=True),
            # Column("etl_package_id", Integer, ForeignKey("etl_packages.id", ondelete="CASCADE"), nullable=True),
            UniqueConstraint("chromosome", "source_type", "source_id", name="uq_variant_snps_chr_source"),
        )
    else:
        # Postgres: COMPOSITE PK for partitioned parent (matches DDL)
        variant_snps = Table(
            "variant_snps",
            metadata,
            Column("chromosome", Integer, nullable=False),
            Column("id", BigInteger, nullable=False, server_default=Identity(always=False)),  # identity comes from raw DDL
            Column("source_type", String(20), nullable=False),
            Column("source_id", BigInteger, nullable=False),
            Column("position_37", BigInteger, nullable=True),
            Column("position_38", BigInteger, nullable=True),
            Column("position_other", BigInteger, nullable=True),
            Column("reference_allele", String(4), nullable=True),
            Column("alternate_allele", String(16), nullable=True),
            Column("data_source_id", Integer, nullable=True),
            Column("etl_package_id", Integer, nullable=True),
            PrimaryKeyConstraint("chromosome", "id", name="pk_variant_snps"),
            UniqueConstraint("chromosome", "source_type", "source_id", name="uq_variant_snps_chr_source"),
        )

    return variant_snps



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

    snp_links = relationship(
        "VariantGWASSNP",
        back_populates="variant_gwas",
        cascade="all, delete-orphan",
        lazy="selectin",
    )


class VariantGWASSNP(Base):
    """
    Helper table indexing rsIDs for VariantGWAS rows.

    Each row corresponds to one SNP extracted from the original
    GWAS Catalog `SNPS` field. This allows fast lookup of GWAS
    associations by numeric rsID, even when the original record
    lists multiple SNPs (e.g. "rs6934929 x rs7276462").
    """

    __tablename__ = "variant_gwas_snp"

    id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

    variant_gwas_id = Column(
        PKBigIntOrInt,
        ForeignKey("variant_gwas.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    snp_id = Column(BigInteger, nullable=False, index=True)
    snp_label = Column(String(50), nullable=True)
    snp_rank = Column(Integer, nullable=True)

    variant_gwas = relationship(
        "VariantGWAS",
        back_populates="snp_links",
    )


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


# # biofilter/modules/db/models/model_variant_annotations.py
# from __future__ import annotations

# from sqlalchemy import (
#     BigInteger,
#     Boolean,
#     Column,
#     Float,
#     Integer,
#     String,
#     Text,
#     Index,
#     UniqueConstraint,
# )
# from sqlalchemy.orm import declarative_base

# Base = declarative_base()


# # ---------------------------------------------------------------------
# # Shared Contract
# # ---------------------------------------------------------------------
# # All large annotation tables must include the canonical variant key:
# # (assembly, chromosome, position, reference_allele, alternate_allele)
# #
# # IMPORTANT: These tables assume one row per ALT allele (no aggregated ALT strings).
# # No FKs are used by design (scalability + partition constraints).
# # ---------------------------------------------------------------------


# class VariantMolecularEffect(Base):
#     """
#     Layer 1 (Consequence) + Layer 2 (LoF/LOFTEE fields when available).

#     One row per:
#       (assembly, chr, pos, ref, alt, transcript_id, consequence, data_source_id)

#     Notes:
#     - transcript_id and gene_id are stored as strings (no master domains in this phase)
#     - LOFTEE fields are optional; only present when source provides them (e.g., gnomAD VEP annotations)
#     """

#     __tablename__ = "variant_molecular_effects"

#     id = Column(BigInteger, primary_key=True, autoincrement=True)

#     # Canonical variant key
#     assembly = Column(Integer, nullable=False)  # e.g. 38
#     chromosome = Column(Integer, nullable=False)  # keep consistent with your system (int)
#     position = Column(BigInteger, nullable=False)  # build-specific (e.g. GRCh38 position)
#     reference_allele = Column(String(64), nullable=False)
#     alternate_allele = Column(String(256), nullable=False)

#     # Context
#     gene_id = Column(String(64), nullable=False)  # e.g. ENSG... or your entity_id string
#     transcript_id = Column(String(64), nullable=True)  # ENST... (nullable for non-transcript contexts)
#     consequence = Column(String(128), nullable=False)  # SO term(s) but stored as string
#     impact = Column(String(32), nullable=True)  # e.g. HIGH/MODERATE/LOW/MODIFIER

#     # LoF / LOFTEE-ish fields (source-dependent)
#     lof_flag = Column(Boolean, nullable=True)  # True/False if you want a quick boolean; else keep null
#     lof_confidence = Column(String(32), nullable=True)  # HC/LC/Filtered/NA/etc.
#     lof_filter = Column(Text, nullable=True)
#     lof_flags = Column(Text, nullable=True)
#     lof_info = Column(Text, nullable=True)

#     # Provenance
#     data_source_id = Column(Integer, nullable=True)
#     etl_package_id = Column(Integer, nullable=True)

#     __table_args__ = (
#         # Logical uniqueness to reduce duplicates from re-loads (tune as needed)
#         UniqueConstraint(
#             "assembly",
#             "chromosome",
#             "position",
#             "reference_allele",
#             "alternate_allele",
#             "transcript_id",
#             "consequence",
#             "data_source_id",
#             name="uq_vme_key_ctx_ds",
#         ),
#         # Query acceleration
#         Index(
#             "ix_vme_variant_key",
#             "assembly",
#             "chromosome",
#             "position",
#             "reference_allele",
#             "alternate_allele",
#         ),
#         Index("ix_vme_gene", "gene_id"),
#         Index("ix_vme_transcript", "transcript_id"),
#         Index("ix_vme_consequence", "consequence"),
#         Index("ix_vme_ds", "data_source_id"),
#     )


# class VariantEffectPrediction(Base):
#     """
#     Layer 3 (Pathogenicity / Predictors).

#     Stores both:
#     - Variant-level predictors (transcript_id = NULL): SpliceAI, Pangolin, CADD (if ingested here)
#     - Effect-level predictors (transcript_id set): AlphaMissense, etc.

#     One row per:
#       (assembly, chr, pos, ref, alt, transcript_id, predictor_name, predictor_version, data_source_id)
#     """

#     __tablename__ = "variant_effect_predictions"

#     id = Column(BigInteger, primary_key=True, autoincrement=True)

#     # Canonical variant key
#     assembly = Column(Integer, nullable=False)
#     chromosome = Column(Integer, nullable=False)
#     position = Column(BigInteger, nullable=False)
#     reference_allele = Column(String(64), nullable=False)
#     alternate_allele = Column(String(256), nullable=False)

#     # Optional transcript context (NULL for variant-level predictors)
#     transcript_id = Column(String(64), nullable=True)

#     # Predictor identity
#     predictor_name = Column(String(64), nullable=False)  # e.g. alphamissense, spliceai, pangolin, clinvar, cadd
#     predictor_version = Column(String(64), nullable=True)  # release/version string

#     # Values
#     score = Column(Float, nullable=True)  # continuous score; may be null for pure categorical labels
#     classification = Column(String(64), nullable=True)  # benign/pathogenic/ambiguous/ClinVar labels/etc.
#     details = Column(Text, nullable=True)  # optional raw payload / extra info as text or JSON string

#     # Provenance
#     source_system_id = Column(Integer, nullable=True)
#     data_source_id = Column(Integer, nullable=True)
#     etl_package_id = Column(Integer, nullable=True)

#     __table_args__ = (
#         UniqueConstraint(
#             "assembly",
#             "chromosome",
#             "position",
#             "reference_allele",
#             "alternate_allele",
#             "transcript_id",
#             "predictor_name",
#             "predictor_version",
#             "data_source_id",
#             name="uq_vep_key_pred_ds",
#         ),
#         Index(
#             "ix_vep_variant_key",
#             "assembly",
#             "chromosome",
#             "position",
#             "reference_allele",
#             "alternate_allele",
#         ),
#         Index("ix_vep_predictor", "predictor_name"),
#         Index("ix_vep_transcript", "transcript_id"),
#         Index("ix_vep_ds", "data_source_id"),
#     )


# class VariantRegulatoryElement(Base):
#     """
#     Layer 5 (Structural regulatory overlap).

#     Represents: variant overlaps a regulatory element in a given context.

#     One row per:
#       (assembly, chr, pos, ref, alt, regulatory_element_id, bio_context, data_source_id)
#     """

#     __tablename__ = "variant_regulatory_elements"

#     id = Column(BigInteger, primary_key=True, autoincrement=True)

#     # Canonical variant key (kept allele-specific for uniform joins)
#     assembly = Column(Integer, nullable=False)
#     chromosome = Column(Integer, nullable=False)
#     position = Column(BigInteger, nullable=False)
#     reference_allele = Column(String(64), nullable=False)
#     alternate_allele = Column(String(256), nullable=False)

#     # Regulatory element identity
#     regulatory_element_id = Column(String(128), nullable=False)  # source-specific element ID
#     element_type = Column(String(64), nullable=True)  # enhancer/promoter/TFBS/etc.

#     # Tissue/cell context stored as string in this phase
#     bio_context = Column(String(128), nullable=True)

#     # Optional additional fields
#     score = Column(Float, nullable=True)  # optional overlap/element confidence score
#     details = Column(Text, nullable=True)

#     # Provenance
#     source_system_id = Column(Integer, nullable=True)
#     data_source_id = Column(Integer, nullable=True)
#     etl_package_id = Column(Integer, nullable=True)

#     __table_args__ = (
#         UniqueConstraint(
#             "assembly",
#             "chromosome",
#             "position",
#             "reference_allele",
#             "alternate_allele",
#             "regulatory_element_id",
#             "bio_context",
#             "data_source_id",
#             name="uq_vre_key_elem_ctx_ds",
#         ),
#         Index(
#             "ix_vre_variant_key",
#             "assembly",
#             "chromosome",
#             "position",
#             "reference_allele",
#             "alternate_allele",
#         ),
#         Index("ix_vre_element", "regulatory_element_id"),
#         Index("ix_vre_context", "bio_context"),
#         Index("ix_vre_ds", "data_source_id"),
#     )


# class VariantGeneRegulatoryEvidence(Base):
#     """
#     Layer 5 (Functional regulatory evidence: eQTL/sQTL/isoQTL).

#     One row per:
#       (assembly, chr, pos, ref, alt, gene_id, bio_context, qtl_type, data_source_id)
#     """

#     __tablename__ = "variant_gene_regulatory_evidence"

#     id = Column(BigInteger, primary_key=True, autoincrement=True)

#     # Canonical variant key
#     assembly = Column(Integer, nullable=False)
#     chromosome = Column(Integer, nullable=False)
#     position = Column(BigInteger, nullable=False)
#     reference_allele = Column(String(64), nullable=False)
#     alternate_allele = Column(String(256), nullable=False)

#     # Target gene (string in this phase)
#     gene_id = Column(String(64), nullable=False)

#     # Context + type
#     bio_context = Column(String(128), nullable=True)  # tissue / cell type
#     qtl_type = Column(String(32), nullable=True)  # eQTL, sQTL, isoQTL, etc.

#     # Summary stats
#     beta = Column(Float, nullable=True)
#     se = Column(Float, nullable=True)
#     p_value = Column(Float, nullable=True)
#     n = Column(Integer, nullable=True)

#     # Optional fields
#     effect_allele = Column(String(16), nullable=True)  # if source provides explicit effect allele
#     details = Column(Text, nullable=True)

#     # Provenance
#     source_system_id = Column(Integer, nullable=True)
#     data_source_id = Column(Integer, nullable=True)
#     etl_package_id = Column(Integer, nullable=True)

#     __table_args__ = (
#         UniqueConstraint(
#             "assembly",
#             "chromosome",
#             "position",
#             "reference_allele",
#             "alternate_allele",
#             "gene_id",
#             "bio_context",
#             "qtl_type",
#             "data_source_id",
#             name="uq_vg_re_key_gene_ctx_type_ds",
#         ),
#         Index(
#             "ix_vg_re_variant_key",
#             "assembly",
#             "chromosome",
#             "position",
#             "reference_allele",
#             "alternate_allele",
#         ),
#         Index("ix_vg_re_gene", "gene_id"),
#         Index("ix_vg_re_context", "bio_context"),
#         Index("ix_vg_re_ds", "data_source_id"),
#         Index("ix_vg_re_p", "p_value"),
#     )