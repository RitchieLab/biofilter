# model_kdc.py
"""
KDC (Knowledge Data Catalog) SQLAlchemy models.

Full (Option C):
- Asset identity
- Asset versions (location/layout/status)
- Schema snapshot (machine-friendly)
- Schema fields (human/API-friendly data dictionary)
- Lineage (reproducibility)
- Scan runs (audit for rebuild operations)

Style: classic SQLAlchemy ORM (Column(...) + relationship(...))
to match the existing Biofilter codebase.
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    JSON,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from biofilter.modules.db.base import Base

# # JSON type: JSONB on Postgres, JSON elsewhere
# try:
#     from sqlalchemy.dialects.postgresql import JSONB as _JSON
# except Exception:  # pragma: no cover
#     from sqlalchemy import JSON as _JSON  # type: ignore
from sqlalchemy.dialects.postgresql import JSONB

# ---------------------------------------------------------------------
# Enums (string-enforced)
# ---------------------------------------------------------------------


def get_kdc_asset_status_enum(name: str):
    return Enum(
        "ACTIVE",
        "DEPRECATED",
        "FAILED",
        "STAGED",
        name=name,
        create_constraint=True,
        validate_strings=True,
    )


def get_kdc_field_status_enum(name: str):
    return Enum(
        "ACTIVE",
        "DEPRECATED",
        "EXPERIMENTAL",
        name=name,
        create_constraint=True,
        validate_strings=True,
    )


def get_kdc_scan_status_enum(name: str):
    return Enum(
        "SUCCESS",
        "FAILED",
        name=name,
        create_constraint=True,
        validate_strings=True,
    )


# ---------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------


class KDCAsset(Base):
    """
    Logical asset identity (independent of release/assembly).

    Example:
      source_system=NCBI, data_source=dbSNP, asset=variants_snps
    """

    __tablename__ = "kdc_assets"

    id = Column(Integer, primary_key=True, autoincrement=True)

    source_system = Column(String(100), nullable=False)
    data_source = Column(String(100), nullable=False)
    asset = Column(String(150), nullable=False)

    description = Column(Text, nullable=True)
    # tags = Column(_JSON, nullable=True)  # optional: {"domain":"variants","tier":"core"}
    # tags = Column(JSON, nullable=True)
    tags = Column(JSON().with_variant(JSONB, "postgresql"), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Relationships
    versions = relationship(
        "KDCAssetVersion",
        back_populates="asset_ref",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint(
            "source_system", "data_source", "asset", name="uq_kdc_asset_identity"
        ),
        Index("ix_kdc_assets_source_system", "source_system"),
        Index("ix_kdc_assets_data_source", "data_source"),
        Index("ix_kdc_assets_asset", "asset"),
    )


class KDCAssetVersion(Base):
    """
    A consumable asset-version (what users open via DuckDB/Polars).

    Uniqueness:
      (asset_id, release, assembly, parameters_hash)

    Notes:
    - parameters_hash may be NULL for unknown lineage (MVP/manual manifests).
    - base_path + path_pattern + partitioning describe the physical layout in KDS.
    """

    __tablename__ = "kdc_asset_versions"

    id = Column(Integer, primary_key=True, autoincrement=True)

    asset_id = Column(
        Integer,
        ForeignKey("kdc_assets.id", ondelete="CASCADE"),
        nullable=False,
    )

    # Version axes
    release = Column(String(120), nullable=False)
    assembly = Column(String(40), nullable=False, default="NA")

    status = Column(
        get_kdc_asset_status_enum("kdc_asset_status_enum"),
        nullable=False,
        default="ACTIVE",
    )

    # Location/layout (KDS)
    base_path = Column(Text, nullable=False)
    path_pattern = Column(Text, nullable=True)  # e.g., "chromosome=*/part-*.parquet"
    # partitioning = Column(_JSON, nullable=True)  # e.g., ["chromosome","bucket"]
    partitioning = Column(JSON().with_variant(JSONB, "postgresql"), nullable=True)

    # Metrics (optional; scanner can populate)
    row_count = Column(BigInteger, nullable=True)
    file_count = Column(Integer, nullable=True)

    # Manifest provenance (optional)
    manifest_path = Column(Text, nullable=True)
    manifest_hash = Column(String(64), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Relationships
    asset_ref = relationship("KDCAsset", back_populates="versions")

    schema = relationship(
        "KDCSchema",
        back_populates="asset_version_ref",
        uselist=False,
        cascade="all, delete-orphan",
    )

    lineage = relationship(
        "KDCLineage",
        back_populates="asset_version_ref",
        uselist=False,
        cascade="all, delete-orphan",
    )

    fields = relationship(
        "KDCSchemaField",
        back_populates="asset_version_ref",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint(
            "asset_id",
            "release",
            "assembly",
            "manifest_hash",
            name="uq_kdc_asset_version_identity_by_manifest",
        ),
        # Alternative uniqueness (recommended long-term) if you prefer parameter-based:
        # UniqueConstraint("asset_id","release","assembly","parameters_hash", name="uq_kdc_asset_version_identity"),
        Index("ix_kdc_asset_versions_asset_release", "asset_id", "release"),
        Index("ix_kdc_asset_versions_status", "status"),
        Index("ix_kdc_asset_versions_assembly", "assembly"),
        CheckConstraint("length(assembly) > 0", name="ck_kdc_asset_versions_assembly"),
    )


class KDCSchema(Base):
    """
    Schema snapshot for an asset-version (machine-friendly).

    - schema_json: list/dict of columns + types (as captured from Parquet/Arrow)
    - schema_hash: stable hash for diff/change detection
    - primary_key/link_keys: contract surface
    """

    __tablename__ = "kdc_schemas"

    id = Column(Integer, primary_key=True, autoincrement=True)

    asset_version_id = Column(
        Integer,
        ForeignKey("kdc_asset_versions.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )

    # schema_json = Column(_JSON, nullable=False)
    # schema_hash = Column(String(64), nullable=False)

    # primary_key = Column(_JSON, nullable=True)  # e.g. ["assembly","chromosome","pos","ref","alt"]
    # link_keys = Column(_JSON, nullable=True)  # e.g. ["hgnc_id","entity_id"]

    schema_json = Column(JSON().with_variant(JSONB, "postgresql"), nullable=True)
    schema_hash = Column(JSON().with_variant(JSONB, "postgresql"), nullable=True)
    primary_key = Column(JSON().with_variant(JSONB, "postgresql"), nullable=True)
    link_keys = Column(JSON().with_variant(JSONB, "postgresql"), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    asset_version_ref = relationship("KDCAssetVersion", back_populates="schema")

    __table_args__ = (
        Index("ix_kdc_schemas_schema_hash", "schema_hash"),
        Index("ix_kdc_schemas_asset_version_id", "asset_version_id"),
    )


class KDCSchemaField(Base):
    """
    Field-level data dictionary for an asset-version (human/API-friendly).

    Scanner can auto-populate:
      - field_name, data_type, nullable
      - is_primary_key / is_link_key (derived from KDCSchema primary_key/link_keys)

    Humans can enrich:
      - description, semantics, db_column mapping, units, enum_values, status
    """

    __tablename__ = "kdc_schema_fields"

    id = Column(Integer, primary_key=True, autoincrement=True)

    asset_version_id = Column(
        Integer,
        ForeignKey("kdc_asset_versions.id", ondelete="CASCADE"),
        nullable=False,
    )

    field_name = Column(String(200), nullable=False)
    data_type = Column(String(120), nullable=False)
    nullable = Column(Boolean, nullable=True)

    description = Column(Text, nullable=True)

    # Optional semantics/provenance
    source_field = Column(String(200), nullable=True)
    semantics = Column(String(200), nullable=True)  # e.g., "variant.position"
    units = Column(String(80), nullable=True)
    # enum_values = Column(_JSON, nullable=True)
    enum_values = Column(JSON().with_variant(JSONB, "postgresql"), nullable=True)

    # Contract flags
    is_primary_key = Column(Boolean, nullable=False, default=False)
    is_link_key = Column(Boolean, nullable=False, default=False)

    # Optional guidance for linking to curated DB
    links_to_entity = Column(String(100), nullable=True)  # e.g., "Gene", "Entity"
    db_column = Column(String(200), nullable=True)  # e.g., "gene.hgnc_id"

    status = Column(
        get_kdc_field_status_enum("kdc_field_status_enum"),
        nullable=False,
        default="ACTIVE",
    )

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    asset_version_ref = relationship("KDCAssetVersion", back_populates="fields")

    __table_args__ = (
        UniqueConstraint(
            "asset_version_id",
            "field_name",
            name="uq_kdc_schema_fields_asset_version_field",
        ),
        Index("ix_kdc_schema_fields_field_name", "field_name"),
        Index("ix_kdc_schema_fields_semantics", "semantics"),
        Index("ix_kdc_schema_fields_is_link_key", "is_link_key"),
        Index("ix_kdc_schema_fields_status", "status"),
    )


class KDCLineage(Base):
    """
    Reproducibility metadata for an asset-version.

    This captures what the manifest (or later, DTP) provides:
      - dtp_name/version
      - parameters_json/hash
      - inputs_json (checksums/uris)
    """

    __tablename__ = "kdc_lineages"

    id = Column(Integer, primary_key=True, autoincrement=True)

    asset_version_id = Column(
        Integer,
        ForeignKey("kdc_asset_versions.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )

    dtp_name = Column(String(200), nullable=True)
    dtp_version = Column(String(60), nullable=True)

    # parameters_json = Column(_JSON, nullable=True)
    parameters_json = Column(JSON().with_variant(JSONB, "postgresql"), nullable=True)
    parameters_hash = Column(String(64), nullable=True)

    # inputs_json = Column(_JSON, nullable=True)
    inputs_json = Column(JSON().with_variant(JSONB, "postgresql"), nullable=True)
    # Example: [{"name":"dbsnp.json.bz2","checksum_md5":"...","uri":"..."}]

    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    asset_version_ref = relationship("KDCAssetVersion", back_populates="lineage")

    __table_args__ = (
        Index("ix_kdc_lineages_dtp_name", "dtp_name"),
        Index("ix_kdc_lineages_parameters_hash", "parameters_hash"),
    )


class KDCScanRun(Base):
    """
    Audit table for catalog rebuild runs.

    Useful for ops:
      - when rebuild ran
      - root path scanned
      - summary counts + warnings
    """

    __tablename__ = "kdc_scan_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)

    started_at = Column(DateTime, server_default=func.now(), nullable=False)
    finished_at = Column(DateTime, nullable=True)

    kds_root = Column(Text, nullable=False)

    status = Column(
        get_kdc_scan_status_enum("kdc_scan_status_enum"),
        nullable=False,
        default="SUCCESS",
    )

    # summary_json = Column(_JSON, nullable=True)
    summary_json = Column(JSON().with_variant(JSONB, "postgresql"), nullable=True)

    # Example:
    # {"assets": 12, "versions": 25, "warnings": ["missing manifest: ..."]}

    log_path = Column(Text, nullable=True)

    __table_args__ = (
        Index("ix_kdc_scan_runs_started_at", "started_at"),
        Index("ix_kdc_scan_runs_status", "status"),
    )
