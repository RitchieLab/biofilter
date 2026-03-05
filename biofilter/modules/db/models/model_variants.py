# biofilter/modules/db/models/model_variants.py
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


def map_variant_master(engine, metadata):
    """
    VariantMaster (BF4 4.1.0):
    - One row per ALT allele (chr, start, end, ref, alt) in GRCh38
    - Partitioned by chromosome on Postgres
    - Logical identity: (chromosome, variant_id)
    """
    dialect = engine.dialect.name
    is_sqlite = dialect == "sqlite"

    if "variant_master" in metadata.tables:
        return metadata.tables["variant_master"]

    if is_sqlite:
        # SQLite: keep it simple. Single integer PK for fast inserts.
        variant_masters = Table(
            "variant_masters",
            metadata,
            Column("chromosome", Integer, nullable=False),
            Column("variant_id", Integer, primary_key=True, autoincrement=True),

            Column("position_start", BigInteger, nullable=False),
            Column("position_end", BigInteger, nullable=False),

            Column("reference_allele", String(64), nullable=False),
            Column("alternate_allele", String(256), nullable=False),

            # Optional external id (rsID etc.)
            Column("source_type", String(20), nullable=True),   # e.g. "rs"
            Column("source_id", BigInteger, nullable=True),     # numeric part (e.g. 123 for rs123)

            # Variant shape
            Column("variant_type", String(20), nullable=True),  # SNP/INDEL/SV...

            # Frequency summaries (optional)
            Column("af_global", Float, nullable=True),
            Column("grpmax_af", Float, nullable=True),

            # TODO: Adicionar os demais campos!

            # Provenance (no FK for now)
            Column("data_source_id", Integer, nullable=True),
            Column("etl_package_id", Integer, nullable=True),

            UniqueConstraint(
                "chromosome",
                "position_start",
                "position_end",
                "reference_allele",
                "alternate_allele",
                name="uq_variant_master_natkey",
            ),
            UniqueConstraint(
                "chromosome",
                "source_type",
                "source_id",
                name="uq_variant_master_chr_source",
            ),
        )
    else:
        # Postgres: composite PK (chromosome, variant_id) to match partitioned parent DDL
        variant_masters = Table(
            "variant_masters",
            metadata,
            Column("chromosome", Integer, nullable=False),
            Column("variant_id", BigInteger, nullable=False, server_default=Identity(always=False)),

            Column("position_start", BigInteger, nullable=False),
            Column("position_end", BigInteger, nullable=False),

            Column("reference_allele", String(64), nullable=False),
            Column("alternate_allele", String(256), nullable=False),

            Column("source_type", String(20), nullable=True),
            Column("source_id", BigInteger, nullable=True),

            Column("variant_type", String(20), nullable=True),

            Column("af_global", Float, nullable=True),
            Column("grpmax_af", Float, nullable=True),

            Column("data_source_id", Integer, nullable=True),
            Column("etl_package_id", Integer, nullable=True),

            PrimaryKeyConstraint("chromosome", "variant_id", name="pk_variant_master"),
            UniqueConstraint(
                "chromosome",
                "position_start",
                "position_end",
                "reference_allele",
                "alternate_allele",
                name="uq_variant_master_natkey",
            ),
            UniqueConstraint(
                "chromosome",
                "source_type",
                "source_id",
                name="uq_variant_master_chr_source",
            ),
        )

    return variant_masters


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
