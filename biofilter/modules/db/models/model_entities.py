from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Enum,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from biofilter.modules.db.base import Base
from biofilter.modules.db.types import PKBigIntOrInt


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

    # id = Column(BigInteger, primary_key=True, autoincrement=True)
    id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

    group_id = Column(
        Integer,
        ForeignKey("entity_groups.id", ondelete="SET NULL"),
        nullable=True,  # noqa E501
    )  # noqa E501
    entity_group = relationship("EntityGroup", back_populates="entities")

    has_conflict = Column(
        Boolean, nullable=True, default=None
    )  # ⚠️ Inform conflict status
    is_active = Column(
        Boolean, nullable=True, default=None
    )  # 🚫 Inform entity deactives

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=True,
    )
    data_source = relationship("ETLDataSource", passive_deletes=True)

    etl_package_id = Column(
        Integer,
        ForeignKey("etl_packages.id", ondelete="CASCADE"),
        nullable=True,
    )
    etl_package = relationship("ETLPackage", passive_deletes=True)

    # Relationships (Go Down)
    entity_alias = relationship("EntityAlias", back_populates="entity")

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
        "EntityAlias",
        primaryjoin="and_(Entity.id==EntityAlias.entity_id, EntityAlias.is_primary==True)",  # noqa E501
        viewonly=True,
        uselist=False,
    )


class EntityAlias(Base):
    """
    Canonical registry of display names, synonyms, and external codes for an
    Entity.

    Key ideas:
    - `alias_value`: the literal string (name, symbol, or external code).
    - `alias_type`: semantic category for the alias (preferred/synonym/code).
    - `xref_source`: origin/system of the alias
    (e.g., HGNC, ENSEMBL, OMIM, ICD10, MONDO, MESH, UMLS, NONE).
    - `is_primary`: soft flag kept for backward compatibility; only meaningful
    when alias_type='preferred'.

    Typical patterns:
    - Genes: (HGNC symbol) -> alias_type='preferred', xref_source='HGNC'
        (Ensembl ID)   -> alias_type='code',      xref_source='ENSEMBL'
        (Synonym)      -> alias_type='synonym',   xref_source='HGNC' or 'LITERATURE'. # noqa E501
    - Diseases: (MONDO label)    -> 'preferred' + xref_source='MONDO'
        (ICD10 code)     -> 'code' + xref_source='ICD10'
        (Common synonym) -> 'synonym' + xref_source='MESH' or 'NONE'

    Constraints:
    - At most one 'preferred' alias per entity_id (enforced by app logic or a
    filtered unique index where supported).
    - Prevent duplicates of the *same* alias within the *same* system for an
    entity.
    """

    __tablename__ = "entity_aliases"

    # id = Column(BigInteger, primary_key=True, autoincrement=True)
    id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

    # Core foreign keys
    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    entity = relationship("Entity", back_populates="entity_alias")

    group_id = Column(
        Integer,
        ForeignKey("entity_groups.id", ondelete="SET NULL"),
        nullable=True,  # noqa E501
    )  # noqa E501
    entity_group = relationship("EntityGroup", passive_deletes=True)

    # Alias value + semantics
    alias_value = Column(String(1000), nullable=False)

    # Allowed: 'preferred' | 'synonym' | 'code'
    alias_type = Column(String(16), nullable=False, default="synonym")

    # e.g., HGNC, ENSEMBL, OMIM, ICD10, MONDO, MESH, UMLS, NONE
    xref_source = Column(String(32), nullable=True)

    # Compatibility flag; only meaningful when alias_type='preferred'
    is_primary = Column(Boolean, nullable=True, default=None)

    is_active = Column(Boolean, nullable=True, default=None)

    # Optional normalization helpers (future-proof)
    # e.g., lowercased/stripped for stable matching
    alias_norm = Column(String(1000), nullable=True)
    # e.g., 'en', 'pt'; useful for disease common names
    locale = Column(String(8), nullable=True)

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=True,
    )
    data_source = relationship("ETLDataSource", passive_deletes=True)

    etl_package_id = Column(
        Integer,
        ForeignKey("etl_packages.id", ondelete="CASCADE"),
        nullable=True,
    )
    etl_package = relationship("ETLPackage", passive_deletes=True)

    __table_args__ = (
        # 1) Soft domain for alias_type to stay SQLite-friendly (no native ENUM):  # noqa E501
        # CheckConstraint(
        #     "alias_type IN ('preferred','synonym','code')",
        #     name="ck_entity_names_alias_type",
        # ),
        # 2) Avoid duplicates for the same (entity, alias_type, xref_source, alias_value)  # noqa E501
        UniqueConstraint(
            "entity_id",
            "alias_type",
            "xref_source",
            "alias_value",
            name="uq_entity_names_semantic_unique",
        ),
        # # 3) Index to accelerate lookup by (xref_source, alias_value) -> resolve code->entity fast  # noqa E501
        # Index("ix_entity_names_xref_lookup", "xref_source", "alias_value"),
        # # 4) Index for normalized search (case-insensitive dedup/matching)
        # Index("ix_entity_names_alias_norm", "alias_norm"),
    )


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

    # id = Column(BigInteger, primary_key=True, autoincrement=True)
    id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

    entity_1_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
    )
    entity_1 = relationship(
        "Entity",
        foreign_keys=[entity_1_id],
        back_populates="relationships_as_1",  # noqa E501
    )  # noqa E501

    entity_1_group_id = Column(
        Integer,
        ForeignKey("entity_groups.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    entity_1_group = relationship(
        "EntityGroup", foreign_keys=[entity_1_group_id], passive_deletes=True
    )

    entity_2_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
    )
    entity_2 = relationship(
        "Entity",
        foreign_keys=[entity_2_id],
        back_populates="relationships_as_2",  # noqa E501
    )  # noqa E501

    entity_2_group_id = Column(
        Integer,
        ForeignKey("entity_groups.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    entity_2_group = relationship(
        "EntityGroup", foreign_keys=[entity_2_group_id], passive_deletes=True
    )

    relationship_type_id = Column(
        Integer,
        ForeignKey("entity_relationship_types.id", ondelete="CASCADE"),
        nullable=False,
    )
    relationship_type = relationship(
        "EntityRelationshipType", back_populates="relationships"
    )  # noqa E501

    # Denormalized for fast deletion/filter
    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=True,
    )
    data_source = relationship("ETLDataSource", passive_deletes=True)

    etl_package_id = Column(
        Integer,
        ForeignKey("etl_packages.id", ondelete="CASCADE"),
        nullable=True,
    )
    etl_package = relationship("ETLPackage", passive_deletes=True)


class EntityLocation(Base):
    """
    Generic genomic location for non-variant entities:
    genes, transcripts, regulatory regions, etc.
    """

    __tablename__ = "entity_locations"

    # id = Column(BigInteger, primary_key=True, autoincrement=True)
    id = Column(PKBigIntOrInt, primary_key=True, autoincrement=True)

    # Central entity
    entity_id = Column(
        BigInteger,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    entity = relationship("Entity", passive_deletes=True)

    # Optional: what kind of entity this location describes
    # (Gene, Protein, Regulatory Region, CNV Region, etc.)
    entity_group_id = Column(
        Integer,
        ForeignKey("entity_groups.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    entity_group = relationship(
        "EntityGroup",
        foreign_keys=[entity_group_id],
        passive_deletes=True,
    )

    # Assembly-aware
    assembly_id = Column(
        Integer,
        ForeignKey("genome_assemblies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    assembly = relationship("GenomeAssembly", passive_deletes=True)

    # If you really need build denormalized, keep it;
    # otherwise I would remove it and rely on GenomeAssembly.
    build = Column(Integer, nullable=False, index=True)

    # Chromosome encoding to match SNP:
    # 1..22 = autosomes, 23 = X, 24 = Y, 25 = MT
    chromosome = Column(Integer, nullable=False, index=True)

    start_pos = Column(BigInteger, nullable=False)
    end_pos = Column(BigInteger, nullable=False)

    strand = Column(Enum("+", "-", name="strand_enum"), nullable=True)

    # Human-friendly label (e.g., cytogenetic band)
    region_label = Column(String(50), nullable=True)  # ex: '12p13.31'

    # Provenance
    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=True,
    )
    data_source = relationship("ETLDataSource", passive_deletes=True)

    etl_package_id = Column(
        Integer,
        ForeignKey("etl_packages.id", ondelete="CASCADE"),
        nullable=True,
    )
    etl_package = relationship("ETLPackage", passive_deletes=True)

    __table_args__ = (
        UniqueConstraint(
            "entity_id",
            "assembly_id",
            name="uq_entity_locations_entity_assembly",
        ),
    )
