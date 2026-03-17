from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.sql import func

from biofilter.modules.db.base import Base


class SystemConfig(Base):
    """
    Stores global configuration parameters for controlling Biofilter system
    behavior.

    Each record represents a configurable key-value pair that can influence the
    execution of ETL processes, query behavior, or UI features.

    Attributes:
        id (int): Primary key.
        key (str): Unique name for the configuration setting.
        value (str): Value assigned to the setting (always stored as string).
        type (str): Type of the setting (e.g., 'string', 'boolean', 'integer').
        description (str): Optional human-readable explanation of the setting.
        editable (bool): Indicates if this config can be modified by external
            clients.
        created_at (datetime): Timestamp of record creation.
        updated_at (datetime): Timestamp of last update.
    """

    __tablename__ = "system_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(50), unique=True, nullable=False)
    value = Column(String(50), nullable=False)
    type = Column(String(50), nullable=False, default="string")
    description = Column(String(255), nullable=True)
    editable = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,  # noqa E501
    )


class BiofilterMetadata(Base):
    """
    Metadata table for tracking schema and ETL versioning of the Biofilter
    instance.

    This model is used to track internal release versions and contextual
    information regarding the state of the database, which is helpful for
    migration, reproducibility, and system introspection.

    Attributes:
        id (int): Primary key.
        schema_version (str): Version of the database schema (e.g., '3.0.1').
        etl_version (str): Version of the latest ETL code that populated db.
        description (str): Optional metadata notes or changelog context.
        created_at (datetime): Timestamp of metadata creation.
        updated_at (datetime): Timestamp of last update.
    """

    __tablename__ = "biofilter_metadata"

    id = Column(Integer, primary_key=True)
    schema_version = Column(String(50), nullable=False)
    # New field: Alembic revision hash (or comma-separated heads)
    schema_revision = Column(String(64), nullable=True)
    etl_version = Column(String(50), nullable=True)
    build_hash = Column(String(50), nullable=True)  # TODO: Use to Reports?!?
    description = Column(String(255), nullable=True)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,  # noqa E501
    )


class GenomeAssembly(Base):
    """
    Stores reference genome assembly information used for variant mapping and
    annotation.

    Each row typically represents a specific chromosome from a particular
    genome build. This table is essential for resolving genomic positions
    during SNP ingestion and liftover support.

    Attributes:
        id (int): Primary key.
        accession (str): Unique accession ID (e.g., 'NC_000001.11').
        assembly_name (str): Human-readable name of the assembly
            (e.g., 'GRCh38.p14').
        chromosome (str): Chromosome identifier (e.g., '1', 'X', 'Y', 'MT').
        created_at (datetime): Timestamp of record creation.
        updated_at (datetime): Timestamp of last update.
    """

    __tablename__ = "genome_assemblies"

    id = Column(Integer, primary_key=True)
    accession = Column(
        String(50), unique=True, nullable=False
    )  # e.g., NC_000024.10                          # noqa E501
    assembly_name = Column(
        String(50), nullable=False
    )  # e.g., GRCh38.p14                            # noqa E501
    chromosome = Column(
        String(50), nullable=True
    )  # e.g., 1–22, X, Y, MT                        # noqa E501
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,  # noqa E501
    )
