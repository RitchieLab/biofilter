from sqlalchemy import Column, Integer, String, Boolean

# from sqlalchemy.orm import relationship
from biofilter.db.base import Base
import datetime


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


# =============================================================================
# ENTITY CORE MODELS
# =============================================================================
class EntityGroup(Base):
    __tablename__ = "entity_groups"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(String)


# class EntityCategory(Base):
#     __tablename__ = "entity_categories"

#     id = Column(Integer, primary_key=True)
#     name = Column(String, unique=True, nullable=False)
#     description = Column(String)


class Entity(Base):
    __tablename__ = "entities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    group_id = Column(Integer, nullable=True)
    has_conflict = Column(Boolean, nullable=True, default=None)

    # category_id = Column(Integer, nullable=True)
    # description = Column(String)
    # active = Column(Boolean, default=True)
    # created_at = Column(DateTime, default=utcnow)
    # updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

    # names = relationship(
    # "EntityName",
    # back_populates="entity",
    # cascade="all,
    # delete-orphan"
    # )


class EntityName(Base):
    __tablename__ = "entity_names"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_id = Column(Integer, nullable=False)
    # entity_id = Column(Integer, ForeignKey("entities.id"))
    datasource_id = Column(Integer, nullable=False)
    name = Column(String, nullable=False)
    # None value = False by default (Save db space)
    is_primary = Column(Boolean, nullable=True, default=None)
    # created_at = Column(DateTime, default=utcnow)

    # entity = relationship("Entity", back_populates="names")


class RelationshipType(Base):
    __tablename__ = "relationship_types"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # code ex: "is_a", "part_of", "regulates"
    code = Column(String, unique=True, nullable=False)
    # desc ex: "Subclass of", "Part of structure"
    description = Column(String, nullable=True)


class EntityRelationship(Base):
    __tablename__ = "entityrelationships"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_1_id = Column(Integer, nullable=False)
    # entity_1_type = Column(String, nullable=False)
    # entity_1_group_id = Column(Integer, nullable=False)
    entity_2_id = Column(Integer, nullable=False)
    # entity_2_type = Column(String, nullable=False)
    # entity_2_group_id = Column(Integer, nullable=False)
    relationship_type_id = Column(Integer, nullable=False)
    # role = Column(String, nullable=True)
    # created_at = Column(DateTime, default=utcnow)


"""
================================================================================
Developer Note - Entity Core Models
================================================================================

This module defines the foundational models for the Biofilter's core entities.
To optimize performance and disk space usage for massive omics data ingestion,
some important design choices were made during this initial version:

1. **No ForeignKey or relationship() constraints**:
    - All FK fields are stored as plain integers.
    - This improves ingestion and query performance significantly on large
        datasets.
    - However, it disables automatic cascade operations, integrity checks, and
        ORM join features.

2. **Minimized Metadata Columns**:
    - Fields like `created_at`, `updated_at`, and `active` were intentionally
        commented out.
    - These may be reintroduced in the future for auditability and delta
        control.

3. **Commented Categories and Descriptions**:
    - For now, auxiliary descriptions and classifications (e.g.,
        `EntityCategory`) are not in use.
    - This simplifies the data model but limits semantic enrichment.

4. **No Delta Tracking for Updates**:
    - Due to the omission of timestamp fields, this version does not support
        delta tracking.
    - Future versions may reintroduce this functionality when needed for
        synchronization, versioning or historical audits.

This lean design was chosen to prioritize **fast loading**,
**low memory usage**, and **maximum throughput**.
Once the system proves stable under production-scale loads, more advanced
features (relationships, timestamps, category systems)
can be re-enabled incrementally with proper migration strategies.

================================================================================
    Author: Andre Garon - Biofilter 3R
    Date: 2025-04
================================================================================
"""
