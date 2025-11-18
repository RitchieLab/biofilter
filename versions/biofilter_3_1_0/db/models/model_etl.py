from biofilter.db.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    Enum,
    JSON,
)


def get_etl_status_enum(name: str):
    return Enum(
        "pending",
        "running",
        "completed",
        "failed",
        "not-applicable",
        "up-to-date",
        name=name,
        create_constraint=True,
        validate_strings=True,
    )  # noqa E501


class ETLSourceSystem(Base):
    """Represents a data source provider, such as NCBI, UniProt, etc."""

    __tablename__ = "etl_source_systems"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)  # length definido
    description = Column(String(1024), nullable=True)
    homepage = Column(String(512), nullable=True)
    active = Column(Boolean, default=True, nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    # updated_at = Column(
    #     DateTime,
    #     server_default=func.now(),
    #     onupdate=func.now(),
    #     nullable=False,  # noqa E501
    # )

    # Relationship
    data_sources = relationship(
        "ETLDataSource", back_populates="source_system"
    )  # noqa E501


class ETLDataSource(Base):
    """
    Represents a data source used in the ETL process.

    Tracks metadata source origin, format, version, and ETL status. Enables
    linking data entities (e.g., genes, proteins, pathways) to their source.
    """

    __tablename__ = "etl_data_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)

    name = Column(
        String(255), unique=True, nullable=False
    )  # e.g., dbSNP, Ensembl  # noqa E501
    dtp_version = Column(
        String(50), nullable=True
    )  # version of the DTP script  # noqa E501
    schema_version = Column(
        String(50), nullable=True
    )  # compatible DB schema version  # noqa E501

    source_system_id = Column(
        Integer,
        ForeignKey("etl_source_systems.id", ondelete="CASCADE"),
        nullable=False,
    )

    data_type = Column(String(50), nullable=False)  # e.g., SNP, Gene, Protein
    source_url = Column(
        String(512), nullable=True
    )  # download URL or API endpoint  # noqa E501
    format = Column(String(20), nullable=False)  # CSV, JSON, API, etc.

    grch_version = Column(String(20), nullable=True)  # e.g., GRCh38
    ucschg_version = Column(String(20), nullable=True)  # e.g., hg19

    dtp_script = Column(
        String(255), nullable=False
    )  # path to the DTP ETL script  # noqa E501

    active = Column(Boolean, nullable=False, default=True)

    created_at = Column(
        DateTime, server_default=func.now(), nullable=False
    )  # noqa E501
    # updated_at = Column(
    #     DateTime,
    #     server_default=func.now(),
    #     onupdate=func.now(),
    #     nullable=False,  # noqa E501
    # )

    # Relationships (Go Up)
    source_system = relationship(
        "ETLSourceSystem", back_populates="data_sources"
    )  # noqa E501

    # Relationships (Go Down)
    etl_packages = relationship(
        "ETLPackage",
        back_populates="data_source",
        cascade="all, delete-orphan",  # noqa E501
    )  # noqa E501


class ETLPackage(Base):
    __tablename__ = "etl_packages"

    id = Column(Integer, primary_key=True, autoincrement=True)

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=False,  # noqa E501
    )  # noqa E501
    data_source = relationship("ETLDataSource", back_populates="etl_packages")

    status = Column(
        get_etl_status_enum("etl_extract_status_enum"),
        nullable=False,
        default="pending",
    )

    operation_type = Column(
        String(50), nullable=True, default="insert"
    )  # insert, update, rollback
    version_tag = Column(String(50), nullable=True)  # Optional snapshot tag
    note = Column(String(255), nullable=True)
    active = Column(Boolean, default=True)

    extract_start = Column(DateTime, nullable=True)
    extract_end = Column(DateTime, nullable=True)
    extract_rows = Column(Integer, nullable=True)
    extract_hash = Column(String(128), nullable=True)  # Raw File
    extract_status = Column(
        get_etl_status_enum("etl_extract_status_enum"),
        nullable=True,
        default="pending",
    )
    transform_start = Column(DateTime, nullable=True)
    transform_end = Column(DateTime, nullable=True)
    transform_rows = Column(Integer, nullable=True)
    transform_hash = Column(String(128), nullable=True)  # Raw File
    transform_status = Column(
        get_etl_status_enum("etl_extract_status_enum"),
        nullable=True,
        default="pending",
    )
    load_start = Column(DateTime, nullable=True)
    load_end = Column(DateTime, nullable=True)
    load_rows = Column(Integer, nullable=True)
    load_hash = Column(String(128), nullable=True)  # Raw File
    load_status = Column(
        get_etl_status_enum("etl_extract_status_enum"),
        nullable=True,
        default="pending",
    )

    stats = Column(JSON, nullable=True)
    # ex: {"records_added": 1831, "warnings": 2}

    created_at = Column(DateTime, server_default=func.now(), nullable=False)


# class ETLProcess(Base):
#     """
#     Tracks the ETL execution lifecycle for a given DataSource,
#     including timestamps and status per ETL stage (Extract, Transform, Load).

#     Supports status tracking via enums and optional content hashing for
#     raw and processed data to ensure reproducibility and integrity.
#     """

#     __tablename__ = "etl_process"

#     id = Column(Integer, primary_key=True, autoincrement=True)

#     data_source_id = Column(
#         Integer,
#         ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
#         nullable=False,
#     )

#     global_status = Column(
#         get_etl_status_enum("etl_extract_status_enum"),
#         nullable=False,
#         default="running",
#     )

#     extract_start = Column(DateTime, nullable=True)
#     extract_end = Column(DateTime, nullable=True)
#     extract_status = Column(
#         get_etl_status_enum("extract_status_enum"),
#         nullable=True,
#         default="running",
#     )

#     transform_start = Column(DateTime, nullable=True)
#     transform_end = Column(DateTime, nullable=True)
#     transform_status = Column(
#         get_etl_status_enum("transform_status_enum"),
#         nullable=True,
#         default="running",
#     )

#     load_start = Column(DateTime, nullable=True)
#     load_end = Column(DateTime, nullable=True)
#     load_status = Column(
#         get_etl_status_enum("load_status_enum"),
#         nullable=True,
#         default="running",
#     )

#     raw_data_hash = Column(String(128), nullable=True)
#     process_data_hash = Column(String(128), nullable=True)

#     # Relationship to DataSource
#     data_source = relationship("DataSource", back_populates="etl_processes")


"""
================================================================================
Developer Note - ETL Core Models
================================================================================

...NEED UPDATE

================================================================================
    Author: Andre Garon - Biofilter 3R
    Date: 2025-04
================================================================================
"""
