from sqlalchemy import (
    Column,
    Integer,
    Numeric,
    # String,
    ForeignKey,
    String,
    Float,
    Text,
    #     UniqueConstraint,
    #     Index,
    #     CheckConstraint,  # noqa E501
)
from sqlalchemy.orm import relationship
from biofilter.db.base import Base


# --- Lookup: status (current, merged, withdrawn, suspect, etc.) -------------
# --- Canonical variant (one row per rsID) -----------------------------------
class VariantMaster(Base):
    """
    Canonical variant (rsID) representation.

    - One row per stable dbSNP rsID
    - Stores canonical assembly, alleles, and quality
    - Linked to Entity for cross-domain relations
    """

    __tablename__ = "variant_masters"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # dbSNP rsID (stable external id)
    variant_id = Column(String(100), unique=True, index=True, nullable=False)

    variant_type = Column(String(16), nullable=False, default="SNP")

    omic_status_id = Column(
        Integer, ForeignKey("omic_status.id"), nullable=True
    )  # noqa E501
    omic_status = relationship("OmicStatus", passive_deletes=True)

    # assembly_id = Column(
    #     Integer, ForeignKey("genome_assemblies.id"), nullable=False
    # )  # noqa E501
    # assembly = relationship("GenomeAssembly", passive_deletes=True)

    chromosome = Column(String(10), nullable=True)  # '1'..'22','X','Y','MT'
    # start_pos = Column(Integer, nullable=True)
    # end_pos = Column(Integer, nullable=True)  # SNP: end_pos == start_pos

    # reference_allele = Column(String(100), nullable=True)
    # alternate_allele = Column(String(100), nullable=True)

    quality = Column(Numeric(3, 1), nullable=True)

    entity_id = Column(
        Integer, ForeignKey("entities.id", ondelete="CASCADE"), nullable=False
    )  # noqa E501
    entity = relationship("Entity", passive_deletes=True)

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
    #     CheckConstraint(
    #        "start_pos <= end_pos", name="ck_variant_span_valid"
    #     ),
    #     Index(
    #         "ix_var_asm_chr_start", "assembly_id", "chromosome", "start_pos"
    #     ),  # noqa E501
    # )

    loci = relationship(
        "VariantLocus",
        back_populates="variant",
        # cascade="all, delete-orphan",
        # passive_deletes=True,
    )


# --- Per-assembly locus index (accelerates position/range queries) ----------
class VariantLocus(Base):
    """
    Per-assembly locus index for a variant.

    - Stores coordinates (assembly, chr, start, end)
    - Supports multiple placements across assemblies
    - Optimized for fast position/range queries
    """

    __tablename__ = "variant_loci"

    id = Column(Integer, primary_key=True, autoincrement=True)

    variant_id = Column(
        Integer,
        ForeignKey("variant_masters.id", ondelete="CASCADE"),
        nullable=False,  # noqa E501
    )
    variant = relationship(
        "VariantMaster",
        back_populates="loci",
        passive_deletes=True,
    )

    assembly_id = Column(
        Integer, ForeignKey("genome_assemblies.id"), nullable=False
    )  # noqa E501
    assembly = relationship("GenomeAssembly", passive_deletes=True)

    chromosome = Column(String(10), nullable=False)
    start_pos = Column(Integer, nullable=False)
    end_pos = Column(Integer, nullable=False)  # SNP: end_pos == start_pos

    # reference_allele = Column(String(100), nullable=True)
    # alternate_allele = Column(String(100), nullable=True)
    reference_allele = Column(Text, nullable=True)
    alternate_allele = Column(Text, nullable=True)

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
    #     # Natural unique key for a locus mapping within an assembly
    #     UniqueConstraint(
    #         "variant_id",
    #         "assembly_id",
    #         "chromosome",
    #         "start_pos",
    #         "end_pos",
    #         name="uq_variant_locus_natural_key",
    #     ),
    #     # Range-friendly indexes for queries by region
    #     Index(
    #         "ix_vloc_asm_chr_start", "assembly_id", "chromosome", "start_pos"
    #     ),  # noqa E501
    #     Index("ix_vloc_asm_chr_end", "assembly_id", "chromosome", "end_pos"),
    # )


class VariantGWAS(Base):
    """
    Flat representation of GWAS Catalog associations.

    This table hosts the raw + mapped data from the GWAS Catalog,
    joined with the EFO trait mapping file. It allows queries
    on variants, studies, and traits, even before full Entity integration.

    Future: link `variant_id`, `trait_id`, and `study_id` to Entities.
    """

    __tablename__ = "variant_gwas"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Publication / study info
    pubmed_id = Column(String(50), index=True, nullable=True)
    # first_author = Column(String(255), nullable=True)
    # publication_date = Column(String(50), nullable=True)  # raw string for now
    # journal = Column(String(255), nullable=True)
    # study_title = Column(Text, nullable=True)
    # link = Column(String(500), nullable=True)

    # Trait / phenotype mapping
    raw_trait = Column(String(255), nullable=True)        # "DISEASE/TRAIT" field
    mapped_trait = Column(String(255), nullable=True)     # "EFO term"
    mapped_trait_id = Column(String(100), nullable=True)  # "EFO/MONDO ID"
    parent_trait = Column(String(255), nullable=True)     # Parent term
    parent_trait_id = Column(String(100), nullable=True)  # Parent URI ID

    # Variant info
    chr_id = Column(String(10), nullable=True)
    chr_pos = Column(Integer, nullable=True)
    reported_gene = Column(String(255), nullable=True)
    mapped_gene = Column(String(255), nullable=True)
    snp_id = Column(String(50), index=True, nullable=True)  # dbSNP ID (rsID)
    snp_risk_allele = Column(String(50), nullable=True)  # Qual a origem
    risk_allele_frequency = Column(Float, nullable=True)
    context = Column(String(100), nullable=True)
    intergenic = Column(String(10), nullable=True)

    # Statistics
    p_value = Column(Float, nullable=True)
    pvalue_mlog = Column(Float, nullable=True)
    odds_ratio_beta = Column(String(50), nullable=True)
    ci_text = Column(String(100), nullable=True)  # confidence interval raw

    # Sample sizes
    initial_sample_size = Column(Text, nullable=True)
    replication_sample_size = Column(Text, nullable=True)

    # Platform info
    platform = Column(String(255), nullable=True)
    cnv = Column(String(10), nullable=True)

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


# --- Liftover cache/audit (for derived mappings or missing placements) ------
# class VariantLiftedPosition(Base):
#     """
#     Cached mapping between assemblies (original -> lifted), with provenance.
#     Only needed when mapping is computed (UCSC/CrossMap) or to record
#     failures.
#     """

#     __tablename__ = "variant_lifted_positions"

#     id = Column(Integer, primary_key=True, autoincrement=True)
#     variant_id = Column(
#         Integer, ForeignKey("variant_masters.id", ondelete="CASCADE"),
#         nullable=False
#     )

#     # Original = Variant's canonical assembly/locus at insertion time
#     original_assembly_id = Column(
#         Integer, ForeignKey("genome_assemblies.id"), nullable=False
#     )
#     original_pos_start = Column(Integer, nullable=False)
#     original_pos_end = Column(Integer, nullable=False)

#     # Target
#     lifted_assembly_id = Column(
#         Integer, ForeignKey("genome_assemblies.id"), nullable=False
#     )
#     lifted_pos_start = Column(Integer, nullable=True)
#     lifted_pos_end = Column(Integer, nullable=True)

#     # Optional: materialize target alleles when validated (e.g., FASTA check)
#     lifted_ref = Column(String(100), nullable=True)
#     lifted_alt = Column(String(100), nullable=True)
#     alleles_checked = Column(Integer, nullable=False, default=0)  # 0/1

#     liftover_status = Column(
#         String(16), nullable=False, default="success"
#     )  # success/failed/ambiguous
#     method = Column(String(20), nullable=True)  # 'dbsnp','ucsc_liftover','crossmap'  # noqa E501
#     chain_name = Column(String(64), nullable=True)  # 'hg38ToHg19.over.chain.gz'  # noqa E501
#     tool_version = Column(String(20), nullable=True)
#     error_code = Column(String(32), nullable=True)
#     error_msg = Column(String(255), nullable=True)

#     data_source_id = Column(
#         Integer, ForeignKey("etl_data_sources.id", ondelete="CASCADE"), nullable=False  # noqa E501
#     )

#     created_at = Column(DateTime, server_default=func.now(), nullable=False)
#     updated_at = Column(
#         DateTime, server_default=func.now(), onupdate=func.now(),
#         nullable=True
#     )

#     __table_args__ = (
#         UniqueConstraint(
#             "variant_id",
#             "original_assembly_id",
#             "lifted_assembly_id",
#             name="uq_vlp_triplet",
#         ),
#         Index("ix_vlp_var_orig", "variant_id", "original_assembly_id"),
#         CheckConstraint(
#             "original_assembly_id <> lifted_assembly_id", name="ck_vlp_diff_asm"  # noqa E501
#         ),
#     )

#     variant = relationship("VariantMaster", back_populates="liftovers")


# # --- rsID merges (old -> new) -------------------------------------------
# class VariantMergeLog(Base):
#     """
#     Tracks rsID merges as reported by dbSNP (old_rs_id -> new_rs_id).
#     """

#     __tablename__ = "variant_merge_log"

#     id = Column(Integer, primary_key=True, autoincrement=True)
#     old_rs_id = Column(String(32), index=True, nullable=False)
#     new_rs_id = Column(String(32), index=True, nullable=False)
#     source = Column(String(16), default="dbSNP")
#     date_merged = Column(DateTime, server_default=func.now())


# --- Links Variant <-> Gene (enquanto não usamos EntityRelation) ------------


# class VariantGeneRelationship(Base):
#     """
#     Many-to-many between variants and genes (body, regulatory, predicted, etc.)  # noqa E501
#     Keep it lean; add fields only when necessary.
#     """

#     __tablename__ = "variant_gene_relationships"

#     gene_id = Column(Integer, ForeignKey("gene_masters.id"), primary_key=True)  # noqa E501
#     variant_id = Column(Integer, ForeignKey("variants.id"), primary_key=True)

#     # Provenance
#     data_source_id = Column(
#         Integer, ForeignKey("etl_data_sources.id", ondelete="CASCADE"), nullable=False  # noqa E501
#     )

#     created_at = Column(DateTime, server_default=func.now(), nullable=False)
#     updated_at = Column(
#         DateTime, server_default=func.now(), onupdate=func.now(), nullable=False  # noqa E501
#     )

#     __table_args__ = (
#         UniqueConstraint("gene_id", "variant_id", name="uq_gene_variant"),
#     )

#     variant = relationship("Variant", back_populates="gene_links")
#     # gene = relationship("GeneMaster", back_populates="variant_gene_links")  # defina no GeneMaster  # noqa E501


# --- (Opcional) Annotations (effects/ClinVar/phenotypes) ---------------------
# Adicione quando for integrar efeitos e clinvar
# class VariantAnnotation(Base):
#     """
#     Optional: functional/clinical annotations per variant.
#     For transcript-level details, add a separate table keyed by transcript_id.    # noqa E501
#     """

#     __tablename__ = "variant_annotations"

#     id = Column(Integer, primary_key=True, autoincrement=True)
#     variant_id = Column(
#         Integer, ForeignKey("variants.id", ondelete="CASCADE"), nullable=False  # noqa E501
#     )

#     gene_id = Column(Integer, ForeignKey("gene_masters.id"), nullable=True)
#     transcript_id = Column(String(64), nullable=True)

#     effect = Column(String(64), nullable=True)  # missense, synonymous, ...
#     consequence = Column(String(64), nullable=True)  # e.g., splice_acceptor_variant  # noqa E501
#     clinical_significance = Column(
#         String(64), nullable=True
#     )  # pathogenic, likely_benign...
#     phenotype = Column(String(128), nullable=True)  # disease/trait label
#     source = Column(String(32), nullable=True)  # ClinVar, gnomAD, Ensembl-VEP...  # noqa E501

#     created_at = Column(DateTime, server_default=func.now(), nullable=False)
#     updated_at = Column(
#         DateTime, server_default=func.now(), onupdate=func.now(), nullable=False  # noqa E501
#     )
#     Index("ix_vann_variant", "variant_id")
