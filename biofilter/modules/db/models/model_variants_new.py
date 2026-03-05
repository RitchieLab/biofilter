# biofilter/modules/db/models/model_variants.py

from sqlalchemy import (
    Table,
    Column,
    Integer,
    BigInteger,
    String,
    Float,
    UniqueConstraint,
    PrimaryKeyConstraint,
)
from sqlalchemy import Identity


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
        variant_master = Table(
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
        variant_master = Table(
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

    return variant_master