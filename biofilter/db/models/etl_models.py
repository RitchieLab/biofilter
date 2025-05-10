from biofilter.db.base import Base
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    Text,
    Enum,
)

from sqlalchemy.orm import relationship
import datetime


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


class SourceSystem(Base):
    __tablename__ = "etl_source_systems"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(
        String, unique=True, nullable=False
    )  # Ex: NCBI, Ensembl, UniProt  # noqa: E501
    description = Column(String, nullable=True)
    homepage = Column(String, nullable=True)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

    # Relationship
    data_sources = relationship("DataSource", back_populates="source_system")


class DataSource(Base):
    __tablename__ = "etl_data_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)  # Ex: dbSNP, Ensembl
    source_system_id = Column(
        Integer,
        ForeignKey("etl_source_systems.id", ondelete="CASCADE"),
        nullable=False,  # noqa: E501
    )
    data_type = Column(String, nullable=False)  # Ex: SNP, Gene, Protein
    source_url = Column(String, nullable=True)  # URL do data source
    format = Column(String, nullable=False)  # CSV, JSON, API, SQL Dump
    grch_version = Column(String, nullable=True)  # Ex: GRCh38, GRCh37
    ucschg_version = Column(String, nullable=True)  # Ex: hg19, hg38
    # dtp_version = Column(String, nullable=False)
    dtp_script = Column(String, nullable=False)
    last_update = Column(
        DateTime, nullable=True
    )  # Last successful update date and time
    last_status = Column(
        String, nullable=False, default="pending"
    )  # "success", "failed", "running"
    active = Column(Boolean, default=True)  # Data Source is active or not
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

    # Relationships
    source_system = relationship(
        "SourceSystem", back_populates="data_sources"
    )  # noqa: E501
    # etl_processes = relationship(
    #     "ETLProcess", back_populates="data_source", cascade="all, delete-orphan"  # noqa: E501
    # )
    variants = relationship("Variant", back_populates="data_source")
    pathways = relationship("Pathway", back_populates="data_source")


# ETL DOMAINS
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
    message = Column(Text, nullable=True)
    # records = Column(Integer, nullable=True)
    # entity_type = Column(String, nullable=True)
    # entity_id = Column(Integer, nullable=True)
    timestamp = Column(DateTime, default=utcnow)

    # Relacionamento com ETLProcess
    # etl_process = relationship("ETLProcess", back_populates="etl_logs")


def get_etl_status_enum(name: str):
    return Enum("pending", "running", "completed", "failed", name=name)


class ETLProcess(Base):
    __tablename__ = "etl_process"

    id = Column(Integer, primary_key=True, autoincrement=True)
    data_source_id = Column(Integer, nullable=False)

    global_status = Column(
        get_etl_status_enum("global_status_enum"),
        default="running",
        nullable=False,  # noqa: E501
    )

    extract_start = Column(DateTime, nullable=True)
    extract_end = Column(DateTime, nullable=True)
    extract_status = Column(
        get_etl_status_enum("extract_status_enum"),
        default="running",
        nullable=True,  # noqa: E501
    )

    transform_start = Column(DateTime, nullable=True)
    transform_end = Column(DateTime, nullable=True)
    transform_status = Column(
        get_etl_status_enum("transform_status_enum"),
        default="running",
        nullable=True,  # noqa: E501
    )

    load_start = Column(DateTime, nullable=True)
    load_end = Column(DateTime, nullable=True)
    load_status = Column(
        get_etl_status_enum("load_status_enum"),
        default="running",
        nullable=True,  # noqa: E501
    )

    dtp_script = Column(String, nullable=False)

    raw_data_hash = Column(String, nullable=True)
    process_data_hash = Column(String, nullable=True)

    # Relationships
    # data_source = relationship("DataSource", back_populates="etl_processes")
    # etl_logs = relationship(
    #     "ETLLog", back_populates="etl_process", cascade="all, delete-orphan"
    # )


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

- Consider enabling `relationship()` once ingestion stabilizes.
- Migrate status fields to SQLAlchemy Enums to enforce valid states.
- Store file hashes and commit references in `dtp_script` for traceability.
- Add finer logs with `entity_type`, `entity_id`, and record counts.

================================================================================
    Author: Andre Garon - Biofilter 3R
    Date: 2025-04
================================================================================
"""
