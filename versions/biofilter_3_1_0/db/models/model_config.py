from biofilter.db.base import Base
from sqlalchemy.sql import func
from sqlalchemy import Column, Integer, String, Boolean, DateTime


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


# class GenomeAssembly(Base):
#     __tablename__ = "genome_assemblies"

#     id = Column(Integer, primary_key=True, autoincrement=True)
#     name = Column(String, unique=True, nullable=False) # ex: GRCh37, GRCh38
#     ucsc_name = Column(String, unique=True, nullable=True) # hg19, hg38
#     description = Column(String, nullable=True)
#     is_default = Column(Boolean, default=False)


"""
================================================================================
Developer Note - SystemConfig Model
================================================================================

The `SystemConfig` model is responsible for storing dynamic, global
configuration parameters that influence system behavior without requiring code
changes.

Key Design Considerations:

1. **Key/Value Storage**:
    - Each configuration is stored as a simple key-value pair.
    - Values are stored as strings but interpreted according to the `type`
        field (e.g., string, integer, boolean, float, etc.).

2. **Editable Flag**:
    - Controls whether the config can be updated via UI or external clients.
    - Used to protect critical internal settings from unintended changes.

3. **Description Field**:
    - Optional field for providing human-readable conString(255) to each
        setting.
    - Encouraged for documentation and frontend presentation.

4. **Audit Timestamps**:
    - `created_at` and `updated_at` are stored in UTC using Python
        timezone-aware datetimes.
    - Useful for tracking configuration changes over time.

5. **Uniqueness Constraint**:
    - The `key` field must be unique to avoid collisions and ambiguity.

Limitations & Future Enhancements:

- Current implementation stores all values as strings.
    Future versions may include value validation and type conversion during
        access.

- There is no built-in versioning or change history.
    Consider integrating change logs or audit tables if advanced tracking is
        needed.

- No encryption is applied to values. For sensitive settings like tokens or
    credentials, implement additional security measures in the application
    layer.

================================================================================
    Author: Andre Garon - Biofilter 3R
    Date: 2025-04
================================================================================
"""
