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
)


class EntityGroup(Base):
    """
    Represents a category or grouping of entities
    (e.g., Gene, Protein, Disease).

    Each group defines a conceptual type that governs how entities are treated
    during ingestion, querying, and relationship mapping. Useful for enforcing
    semantic boundaries in multi-omics integration.

    Example groups: "Gene", "Protein", "Chemical", "Phenotype"
    """

    __tablename__ = "entity_groups"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), unique=True, nullable=False)
    description = Column(String(255))

    # Relationships as FK
    entities = relationship("Entity", back_populates="entity_group")


class Entity(Base):
    """
    Represents a unique biological or biomedical concept such as a gene,
    protein, chemical, or disease.

    Each entity is linked to a group (e.g., Gene) and may have multiple aliases
    or names across different data sources. Entities support tracking of
    conflicts, deactivation, and timestamps for change control.

    Relationships:
    - Linked to `EntityGroup` via group_id
    - Has multiple `EntityName` records (aliases/synonyms)
    - Can participate in multiple `EntityRelationship` records
    - Exposes a direct link to its primary name for efficient querying
    """

    __tablename__ = "entities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    group_id = Column(
        Integer,
        ForeignKey("entity_groups.id", ondelete="SET NULL"),
        nullable=True,  # noqa E501
    )  # noqa E501
    has_conflict = Column(
        Boolean, nullable=True, default=None
    )  # ⚠️ Inform conflict status
    is_deactive = Column(
        Boolean, nullable=True, default=None
    )  # 🚫 Inform entity deactives

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=False,
    )

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,  # noqa E501
    )

    # Relationships (Go Up)
    entity_group = relationship("EntityGroup", back_populates="entities")
    data_source = relationship("DataSource", passive_deletes=True)

    # Relationships (Go Down)
    entity_names = relationship("EntityName", back_populates="entity")
    relationships_as_1 = relationship(
        "EntityRelationship",
        foreign_keys="[EntityRelationship.entity_1_id]",
        back_populates="entity_1",
    )
    relationships_as_2 = relationship(
        "EntityRelationship",
        foreign_keys="[EntityRelationship.entity_2_id]",
        back_populates="entity_2",
    )

    # -- Permit load primary name in a single query
    primary_name = relationship(
        "EntityName",
        primaryjoin="and_(Entity.id==EntityName.entity_id, EntityName.is_primary==True)",  # noqa E501
        viewonly=True,
        uselist=False,
    )


class EntityName(Base):
    """
    Stores a synonym or alias for a given entity, typically sourced from
    external databases or literature.

    Allows mapping multiple naming conventions (e.g., symbols, synonyms,
    IDs) to a unified `Entity`. One name can be flagged as primary to serve
    as the canonical name during queries and display.

    Relationships:
    - Linked to `Entity` via entity_id
    - Linked to `DataSource` to track origin of the name
    """

    __tablename__ = "entity_names"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_id = Column(Integer, ForeignKey("entities.id", ondelete="CASCADE"))

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=False,
    )

    name = Column(String(255), nullable=False)
    is_primary = Column(Boolean, nullable=True, default=None)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,  # noqa E501
    )

    # Relationships as FK
    entity = relationship("Entity", back_populates="entity_names")
    data_source = relationship("DataSource", passive_deletes=True)


class EntityRelationshipType(Base):
    """
    Defines the semantic nature of a relationship between two entities.

    Used to annotate directed edges in the entity graph, supporting
    interpretations like hierarchy ("is_a"), containment ("part_of"),
    interaction ("binds_to"), regulation ("regulates"), etc.

    This table enables reusability and standardization of relationship
    semantics.
    """

    __tablename__ = "entity_relationship_types"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(
        String(25), unique=True, nullable=False
    )  # ex: "is_a", "part_of"  # noqa E501
    description = Column(String(255), nullable=True)

    # Relationships
    relationships = relationship(
        "EntityRelationship", back_populates="relationship_type"
    )


class EntityRelationship(Base):
    """
    Defines a directed relationship between two entities.

    Each relationship connects a `source` entity (`entity_1`) to a `target`
    entity (`entity_2`), with a defined relationship type and an associated
    data source.

    Supports tracing of ontologies, interactions, regulatory networks,
    and curated mappings from knowledge bases.

    Relationships:
    - `entity_1` and `entity_2` are both instances of `Entity`
    - `relationship_type` defines the semantic type
    - `data_source` indicates the origin of this relationship
    """

    __tablename__ = "entity_relationships"

    id = Column(Integer, primary_key=True, autoincrement=True)

    entity_1_id = Column(
        Integer,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
    )
    entity_2_id = Column(
        Integer,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
    )
    relationship_type_id = Column(
        Integer,
        ForeignKey("entity_relationship_types.id", ondelete="CASCADE"),
        nullable=False,
    )
    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=False,
    )

    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    # Relationships
    entity_1 = relationship(
        "Entity",
        foreign_keys=[entity_1_id],
        back_populates="relationships_as_1",  # noqa E501
    )  # noqa E501
    entity_2 = relationship(
        "Entity",
        foreign_keys=[entity_2_id],
        back_populates="relationships_as_2",  # noqa E501
    )  # noqa E501
    relationship_type = relationship(
        "EntityRelationshipType", back_populates="relationships"
    )  # noqa E501
    data_source = relationship("DataSource", passive_deletes=True)  # noqa E501


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


About Entities:

    System interpretation:
    Situation	                    has_conflict	is_deactive	Expected action
    Normal entity	                None or False	None	    Use normally
    Pending conflict	            True	        None	    Mark, but can use with caution          # noqa E501
    Resolved conflict with delete	True	        True	    Ignore in queries and ingestions        # noqa E501
    Resolved conflict with merge	True	        True	    Transfer aliases and ignore this entity # noqa E501
    Obsolete entity (manual)	    None	        True	    Deactivated by curation

    # Example of safe filter
    session.query(Entity).filter(Entity.is_deactive.is_(None))


================================================================================
    Author: Andre Garon - Biofilter 3R
    Date: 2025-04
================================================================================
"""
