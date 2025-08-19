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
)


class SourceSystem(Base):
    """Represents a data source provider, such as NCBI, UniProt, etc."""

    __tablename__ = "etl_source_systems"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)  # length definido
    description = Column(String(1024), nullable=True)
    homepage = Column(String(512), nullable=True)
    active = Column(Boolean, default=True, nullable=False)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,  # noqa E501
    )

    # Relationship
    data_sources = relationship("DataSource", back_populates="source_system")


class DataSource(Base):
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

    last_update = Column(DateTime, nullable=True)  # last successful ingestion
    last_status = Column(
        String(20), nullable=False, default="pending"
    )  # status: "success", "failed", "running", "pending"

    active = Column(Boolean, nullable=False, default=True)

    created_at = Column(
        DateTime, server_default=func.now(), nullable=False
    )  # noqa E501
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,  # noqa E501
    )

    # Relationships (Go Up)
    source_system = relationship("SourceSystem", back_populates="data_sources")

    # Relationships (Go Down)
    etl_processes = relationship(
        "ETLProcess",
        back_populates="data_source",
        cascade="all, delete-orphan",  # noqa E501
    )  # noqa E501


def get_etl_status_enum(name: str):
    return Enum(
        "pending",
        "running",
        "completed",
        "failed",
        "not_applicable",
        name=name,
    )  # noqa E501


class ETLProcess(Base):
    """
    Tracks the ETL execution lifecycle for a given DataSource,
    including timestamps and status per ETL stage (Extract, Transform, Load).

    Supports status tracking via enums and optional content hashing for
    raw and processed data to ensure reproducibility and integrity.
    """

    __tablename__ = "etl_process"

    id = Column(Integer, primary_key=True, autoincrement=True)

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=False,
    )

    global_status = Column(
        get_etl_status_enum("global_status_enum"),
        nullable=False,
        default="running",
    )

    extract_start = Column(DateTime, nullable=True)
    extract_end = Column(DateTime, nullable=True)
    extract_status = Column(
        get_etl_status_enum("extract_status_enum"),
        nullable=True,
        default="running",
    )

    transform_start = Column(DateTime, nullable=True)
    transform_end = Column(DateTime, nullable=True)
    transform_status = Column(
        get_etl_status_enum("transform_status_enum"),
        nullable=True,
        default="running",
    )

    load_start = Column(DateTime, nullable=True)
    load_end = Column(DateTime, nullable=True)
    load_status = Column(
        get_etl_status_enum("load_status_enum"),
        nullable=True,
        default="running",
    )

    raw_data_hash = Column(String(128), nullable=True)
    process_data_hash = Column(String(128), nullable=True)

    # Relationship to DataSource
    data_source = relationship("DataSource", back_populates="etl_processes")


# TODO: Review this model / All log is hosting in file nowadays
class ETLLog(Base):
    __tablename__ = "etl_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # etl_process_id = Column(
    #     Integer, ForeignKey("etl_process.id", ondelete="CASCADE"), nullable=False   # noqa: E501
    # )
    etl_process_id = Column(Integer, nullable=False)
    phase = Column(String, nullable=False)  # "extract", "transform", "load"
    action = Column(
        String, nullable=False
    )  # "insert", "update", "skip", "error"  # noqa: E501
    message = Column(String(255), nullable=True)
    # records = Column(Integer, nullable=True)
    # entity_type = Column(String, nullable=True)
    # entity_id = Column(Integer, nullable=True)
    timestamp = Column(DateTime, server_default=func.now(), nullable=False)

    # Relacionamento com ETLProcess
    # etl_process = relationship("ETLProcess", back_populates="etl_logs")


"""
================================================================================
Developer Note - ETL Core Models
================================================================================

This module defines the core database structures for tracking the ETL lifecycle
of each data source integrated into the Biofilter system.

Design Principles:

- Relationships (`relationship()` and `ForeignKey`) are intentionally omitted
    to maximize data ingestion throughput and reduce overhead in massive
    workflows. IDs are stored as plain integers, prioritizing performance and
    simplicity.

- The model separates concerns:
    - `SourceSystem`: Organizational origin of datasets (e.g. NCBI, Ensembl).
    - `DataSource`: Specific datasets, formats, and genome versions.
    - `ETLProcess`: Tracks the state and timing of each ETL run.
    - `ETLLog`: Stores detailed, phase-specific logs for debugging and
        auditing.

- Each ETL process is broken into three phases:
    - `extract`, `transform`, and `load`, each with start/end timestamps,
        individual statuses, and optional file references.

Future Directions:

================================================================================
    Author: Andre Garon - Biofilter 3R
    Date: 2025-04
================================================================================
"""
