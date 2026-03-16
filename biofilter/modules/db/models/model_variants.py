# biofilter/modules/db/models/model_variants.py
from sqlalchemy import (
    Float,
    Text,
    Table,
    Column,
    Boolean,
    Integer,
    BigInteger,
    String,
    ForeignKey,
    UniqueConstraint,
    PrimaryKeyConstraint,
)
from sqlalchemy.orm import relationship, mapped_column, Mapped
from sqlalchemy import Identity

from biofilter.modules.db.base import Base
from biofilter.modules.db.types import PKBigIntOrInt


class VariantConsequenceGroup(Base):
    __tablename__ = "variant_consequence_groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)  # noqa E501
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)

    consequences: Mapped[list["VariantConsequence"]] = relationship(
        "VariantConsequence",
        back_populates="group",
    )


class VariantConsequenceCategory(Base):
    __tablename__ = "variant_consequence_categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)  # noqa E501
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)

    consequences: Mapped[list["VariantConsequence"]] = relationship(
        "VariantConsequence",
        back_populates="category",
    )


class VariantConsequence(Base):
    __tablename__ = "variant_consequences"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)  # noqa E501
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)

    severity_rank: Mapped[int] = mapped_column(Integer, nullable=False)

    consequence_group_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("variant_consequence_groups.id"),
        nullable=True,
    )
    consequence_category_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("variant_consequence_categories.id"),
        nullable=True,
    )

    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)  # noqa E501

    group: Mapped["VariantConsequenceGroup | None"] = relationship(
        "VariantConsequenceGroup",
        back_populates="consequences",
    )
    category: Mapped["VariantConsequenceCategory | None"] = relationship(
        "VariantConsequenceCategory",
        back_populates="consequences",
    )


class VariantImpact(Base):
    __tablename__ = "variant_impacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)  # noqa E501
    name: Mapped[str] = mapped_column(String(16), unique=True, nullable=False)
    severity_rank: Mapped[int] = mapped_column(Integer, nullable=False)


class VariantBiotype(Base):
    __tablename__ = "variant_biotypes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)  # noqa E501
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)


def map_variant_masters(engine, metadata):
    """
    VariantMasters (BF4 4.1.0):
    - One row per ALT allele (chr, start, end, ref, alt) in GRCh38
    - Partitioned by chromosome on Postgres
    - Logical identity: (chromosome, variant_id)
    """
    dialect = engine.dialect.name
    is_sqlite = dialect == "sqlite"

    if "variant_masters" in metadata.tables:
        return metadata.tables["variant_masters"]

    common_cols = [
        Column("chromosome", Integer, nullable=False),
        Column("position_start", BigInteger, nullable=False),
        Column("position_end", BigInteger, nullable=False),
        Column("reference_allele", String(64), nullable=False),
        Column("alternate_allele", String(256), nullable=False),
        # External IDs (optional)
        Column("rsid", String(32), nullable=True),  # e.g. "rs123"
        # Variant classification
        Column("variant_type", String(20), nullable=True),  # mixed/multi/etc/
        Column(
            "allele_type", String(20), nullable=True
        ),  # optional: SNV/MNV/INS/DEL/etc.
        # Frequency summaries (variant-level)
        Column("ac", BigInteger, nullable=True),
        Column("an", BigInteger, nullable=True),
        Column("af", Float, nullable=True),
        Column("grpmax", String(32), nullable=True),
        Column("grpmax_af", Float, nullable=True),
        # Predictors / scores (summary fields at variant-allele level)
        Column("cadd_raw_score", Float, nullable=True),
        Column("cadd_phred", Float, nullable=True),
        Column("revel_max", Float, nullable=True),
        Column("spliceai_ds_max", Float, nullable=True),
        Column("pangolin_largest_ds", Float, nullable=True),
        # SIFT/PolyPhen are transcript/protein level. Here is a Max Score
        Column("sift_max", Float, nullable=True),
        Column("polyphen_max", Float, nullable=True),
        # Provenance (no FK)
        Column("data_source_id", Integer, nullable=True),
        Column("etl_package_id", Integer, nullable=True),
    ]

    if is_sqlite:
        variant_masters = Table(
            "variant_masters",
            metadata,
            Column("variant_id", Integer, primary_key=True, autoincrement=True),  # noqa E501
            *common_cols,
            UniqueConstraint(
                "chromosome",
                "position_start",
                "position_end",
                "reference_allele",
                "alternate_allele",
                name="uq_variant_masters_natkey",
            ),
        )
    else:
        variant_masters = Table(
            "variant_masters",
            metadata,
            Column(
                "variant_id",
                BigInteger,
                nullable=False,
                server_default=Identity(always=False),
            ),
            *common_cols,
            PrimaryKeyConstraint("chromosome", "variant_id", name="pk_variant_masters"),  # noqa E501
            UniqueConstraint(
                "chromosome",
                "position_start",
                "position_end",
                "reference_allele",
                "alternate_allele",
                name="uq_variant_masters_natkey",
            ),
        )

    return variant_masters


def map_variant_molecular_effects(engine, metadata):
    """
    VariantMolecularEffects:
    - One row per (variant allele × transcript × atomic consequence) in GRCh38
    - Partitioned by chromosome on Postgres
    - Logical join key with VariantMasters: (chromosome, variant_id)
    - No physical FK constraints (ETL-enforced integrity)
    """
    dialect = engine.dialect.name
    is_sqlite = dialect == "sqlite"

    if "variant_molecular_effects" in metadata.tables:
        return metadata.tables["variant_molecular_effects"]

    common_cols = [
        Column("chromosome", Integer, nullable=False),
        # Useful operational key
        Column("variant_key", String(256), nullable=False),
        # Raw / stable identity
        Column("gene_id", String(32), nullable=True),
        Column("gene_symbol", String(64), nullable=True),
        Column("transcript_id", String(32), nullable=False),
        Column("feature_type", String(32), nullable=True),
        # Raw VEP consequence preserved
        Column("consequence_raw", String(255), nullable=True),
        # Dimension-backed fields (no physical FK)
        Column("consequence_id", Integer, nullable=False),
        Column("impact_id", Integer, nullable=True),
        Column("biotype_id", Integer, nullable=True),
        # Derived severity / helper fields
        Column("consequence_rank", Integer, nullable=True),
        Column("impact_rank", Integer, nullable=True),
        Column("most_severe_consequence_per_annotation_id", Integer, nullable=True),  # noqa E501
        Column("most_severe_consequence_per_variant_id", Integer, nullable=True),  # noqa E501
        Column("is_most_severe_for_annotation", Boolean, nullable=True),
        Column("is_most_severe_for_variant", Boolean, nullable=True),
        # Useful VEP context
        Column("variant_class", String(16), nullable=True),
        Column("canonical", Boolean, nullable=True),
        Column("mane_select", Boolean, nullable=True),
        Column("mane_plus_clinical", Boolean, nullable=True),
        # HGVS / protein context
        Column("hgvsc", String(128), nullable=True),
        Column("hgvsp", String(128), nullable=True),
        Column("cdna_position", String(32), nullable=True),
        Column("cds_position", String(32), nullable=True),
        Column("protein_position", String(32), nullable=True),
        Column("amino_acids", String(32), nullable=True),
        Column("codons", String(64), nullable=True),
        Column("ensp", String(32), nullable=True),
        # Layer 2 — LoF / LOFTEE
        Column("lof_flag", Boolean, nullable=True),
        Column("lof_confidence", String(8), nullable=True),  # HC/LC/Filtered/NA  # noqa E501
        Column("lof_filter", String(128), nullable=True),
        Column("lof_flags", String(256), nullable=True),
        Column("lof_info", Text, nullable=True),
        # Provenance
        Column("data_source_id", Integer, nullable=True),
        Column("etl_package_id", Integer, nullable=True),
    ]

    if is_sqlite:
        variant_molecular_effects = Table(
            "variant_molecular_effects",
            metadata,
            Column("variant_id", Integer, nullable=False),
            *common_cols,
            PrimaryKeyConstraint(
                "chromosome",
                "variant_id",
                "transcript_id",
                "consequence_id",
                name="pk_variant_molecular_effects",
            ),
        )
    else:
        variant_molecular_effects = Table(
            "variant_molecular_effects",
            metadata,
            Column("variant_id", BigInteger, nullable=False),
            *common_cols,
            PrimaryKeyConstraint(
                "chromosome",
                "variant_id",
                "transcript_id",
                "consequence_id",
                name="pk_variant_molecular_effects",
            ),
        )

    return variant_molecular_effects


def map_variant_effect_predictions(engine, metadata):
    """
    VariantEffectPredictions (BF4 4.1.0):
    - One row per predictor per variant (optionally per transcript)
    - Partitioned by chromosome on Postgres
    - Join key with VariantMasters: (chromosome, variant_id)
    - No physical FK constraints
    """
    dialect = engine.dialect.name
    is_sqlite = dialect == "sqlite"

    if "variant_effect_predictions" in metadata.tables:
        return metadata.tables["variant_effect_predictions"]

    common_cols = [
        Column("chromosome", Integer, nullable=False),
        Column("predictor_key", String(128), nullable=False),
        Column("transcript_id", String(32), nullable=True),
        Column("predictor_name", String(64), nullable=False),
        Column("predictor_version", String(32), nullable=True),
        Column("score", Float, nullable=True),
        Column("classification", String(64), nullable=True),
        Column("details", Text, nullable=True),
        Column("data_source_id", Integer, nullable=True),
        Column("etl_package_id", Integer, nullable=True),
    ]

    if is_sqlite:
        variant_effect_predictions = Table(
            "variant_effect_predictions",
            metadata,
            Column("variant_id", Integer, nullable=False),
            *common_cols,
            PrimaryKeyConstraint(
                "chromosome",
                "variant_id",
                "predictor_key",
                name="pk_variant_effect_predictions",
            ),
        )
    else:
        variant_effect_predictions = Table(
            "variant_effect_predictions",
            metadata,
            Column("variant_id", BigInteger, nullable=False),
            *common_cols,
            PrimaryKeyConstraint(
                "chromosome",
                "variant_id",
                "predictor_key",
                name="pk_variant_effect_predictions",
            ),
        )

    return variant_effect_predictions


def map_variant_regulatory_elements(engine, metadata):
    """
    VariantRegulatoryElements (BF4 4.1.0):
    - One row per (variant × regulatory element × bio_context)
    - Partitioned by chromosome on Postgres
    - Join key with VariantMasters: (chromosome, variant_id)
    """
    dialect = engine.dialect.name
    is_sqlite = dialect == "sqlite"

    if "variant_regulatory_elements" in metadata.tables:
        return metadata.tables["variant_regulatory_elements"]

    common_cols = [
        Column("chromosome", Integer, nullable=False),
        Column("reg_element_key", String(192), nullable=False),
        Column("regulatory_element_id", String(64), nullable=False),
        Column("element_type", String(32), nullable=True),
        Column("bio_context", String(128), nullable=True),
        Column("score", Float, nullable=True),
        Column("details", Text, nullable=True),
        Column("data_source_id", Integer, nullable=True),
        Column("etl_package_id", Integer, nullable=True),
    ]

    if is_sqlite:
        variant_regulatory_elements = Table(
            "variant_regulatory_elements",
            metadata,
            Column("variant_id", Integer, nullable=False),
            *common_cols,
            PrimaryKeyConstraint(
                "chromosome",
                "variant_id",
                "reg_element_key",
                name="pk_variant_regulatory_elements",
            ),
        )
    else:
        variant_regulatory_elements = Table(
            "variant_regulatory_elements",
            metadata,
            Column("variant_id", BigInteger, nullable=False),
            *common_cols,
            PrimaryKeyConstraint(
                "chromosome",
                "variant_id",
                "reg_element_key",
                name="pk_variant_regulatory_elements",
            ),
        )

    return variant_regulatory_elements


def map_variant_gene_regulatory_evidence(engine, metadata):
    """
    VariantGeneRegulatoryEvidence (BF4 4.1.0):
    - One row per (variant × gene × qtl_type × bio_context)
    - Partitioned by chromosome on Postgres
    - Join key with VariantMasters: (chromosome, variant_id)
    """
    dialect = engine.dialect.name
    is_sqlite = dialect == "sqlite"

    if "variant_gene_regulatory_evidence" in metadata.tables:
        return metadata.tables["variant_gene_regulatory_evidence"]

    common_cols = [
        Column("chromosome", Integer, nullable=False),
        Column("evidence_key", String(256), nullable=False),
        Column("gene_id", String(32), nullable=False),
        Column("bio_context", String(128), nullable=True),
        Column("qtl_type", String(16), nullable=False),
        Column("beta", Float, nullable=True),
        Column("se", Float, nullable=True),
        Column("p_value", Float, nullable=True),
        Column("n", Integer, nullable=True),
        Column("effect_allele", String(64), nullable=True),
        Column("details", Text, nullable=True),
        Column("data_source_id", Integer, nullable=True),
        Column("etl_package_id", Integer, nullable=True),
    ]

    if is_sqlite:
        variant_gene_regulatory_evidence = Table(
            "variant_gene_regulatory_evidence",
            metadata,
            Column("variant_id", Integer, nullable=False),
            *common_cols,
            PrimaryKeyConstraint(
                "chromosome",
                "variant_id",
                "evidence_key",
                name="pk_variant_gene_regulatory_evidence",
            ),
        )
    else:
        variant_gene_regulatory_evidence = Table(
            "variant_gene_regulatory_evidence",
            metadata,
            Column("variant_id", BigInteger, nullable=False),
            *common_cols,
            PrimaryKeyConstraint(
                "chromosome",
                "variant_id",
                "evidence_key",
                name="pk_variant_gene_regulatory_evidence",
            ),
        )

    return variant_gene_regulatory_evidence


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
