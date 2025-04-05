from biofilter.db.base import Base
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    Text,
)
from sqlalchemy.orm import relationship
import datetime


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


class SourceSystem(Base):
    __tablename__ = "source_systems"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)  # Ex: NCBI, Ensembl, UniProt
    description = Column(String, nullable=True)
    homepage = Column(String, nullable=True)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

    # Relationship
    data_sources = relationship("DataSource", back_populates="source_system")


class DataSource(Base):
    __tablename__ = "data_sources"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True, nullable=False)  # Ex: dbSNP, Ensembl
    source_system_id = Column(
        Integer, ForeignKey("source_systems.id", ondelete="CASCADE"), nullable=False
    )
    data_type = Column(String, nullable=False)  # Ex: SNP, Gene, Protein
    source_url = Column(String, nullable=True)  # URL do data source
    format = Column(String, nullable=False)  # CSV, JSON, API, SQL Dump
    grch_version = Column(String, nullable=True)  # Ex: GRCh38, GRCh37
    ucschg_version = Column(String, nullable=True)  # Ex: hg19, hg38
    dtp_version = Column(String, nullable=False)  # Data Transformation Process Version
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
    source_system = relationship("SourceSystem", back_populates="data_sources")
    etl_processes = relationship(
        "ETLProcess", back_populates="data_source", cascade="all, delete-orphan"
    )


# ETL DOMAINS
class ETLLog(Base):
    __tablename__ = "etl_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    etl_process_id = Column(Integer, ForeignKey("etl_process.id", ondelete="CASCADE"), nullable=False)

    phase = Column(String, nullable=False)  # "extract", "transform", "load"
    action = Column(String, nullable=False)  # "insert", "update", "skip", "error"
    message = Column(Text, nullable=True)
    records = Column(Integer, nullable=True)
    entity_type = Column(String, nullable=True)  # "Gene", "Protein", etc.
    entity_id = Column(Integer, nullable=True)  # ID da Entity relacionada, se houver

    timestamp = Column(DateTime, default=utcnow)

    # Relacionamento com ETLProcess
    etl_process = relationship("ETLProcess", back_populates="etl_logs")


class ETLProcess(Base):
    __tablename__ = "etl_process"

    id = Column(Integer, primary_key=True, autoincrement=True)
    data_source_id = Column(Integer, ForeignKey("data_sources.id", ondelete="CASCADE"), nullable=False)

    start_time = Column(DateTime, nullable=False, default=utcnow)
    end_time = Column(DateTime, nullable=True)
    status = Column(String, nullable=False, default="running")  # overall: running, completed, failed

    # Phased control
    extract_start = Column(DateTime, nullable=True)
    extract_end = Column(DateTime, nullable=True)
    extract_status = Column(String, nullable=True)  # started, success, failed

    transform_start = Column(DateTime, nullable=True)
    transform_end = Column(DateTime, nullable=True)
    transform_status = Column(String, nullable=True)

    load_start = Column(DateTime, nullable=True)
    load_end = Column(DateTime, nullable=True)
    load_status = Column(String, nullable=True)

    error_message = Column(String, nullable=True)
    dtp_script = Column(String, nullable=False)

    # Tracking if this is reprocessing or not
    file_stamp = Column(String, nullable=True)

    # Relationships
    data_source = relationship("DataSource", back_populates="etl_processes")
    etl_logs = relationship("ETLLog", back_populates="etl_process", cascade="all, delete-orphan")
