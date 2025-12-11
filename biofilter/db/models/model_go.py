from biofilter.db.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import (
    Column,
    String,
    BigInteger,
    Integer,
    ForeignKey,
    DateTime,
)  # noqa E501


class GOMaster(Base):
    """
    Stores individual Gene Ontology (GO) terms and their mapping to Biofilter
    entities.

    Each GO term (e.g., GO:0006915) is represented by a unique `go_id` and
    linked to a corresponding Entity through `entity_id`. The `namespace`
    field defines the ontology category (MF = Molecular Function,
    BP = Biological Process, CC = Cellular Component).

    Relationships:
        - parents: Parent terms (incoming edges in the GO DAG)
        - children: Child terms (outgoing edges in the GO DAG)
        - data_source: Source of ingestion (e.g., Gene Ontology Consortium)
    """

    __tablename__ = "go_masters"

    id = Column(Integer, primary_key=True, autoincrement=True)
    go_id = Column(String(100), unique=True, nullable=False)

    name = Column(String(255), nullable=False)
    namespace = Column(String(50), nullable=False)  # MF, BP, CC

    entity_id = Column(
        BigInteger, ForeignKey("entities.id", ondelete="CASCADE"), nullable=False
    )  # noqa E501
    entity = relationship("Entity", passive_deletes=True)

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
    parents = relationship(
        "GORelation",
        back_populates="child_term",
        foreign_keys="GORelation.child_id",  # noqa E501
    )
    children = relationship(
        "GORelation",
        back_populates="parent_term",
        foreign_keys="GORelation.parent_id",  # noqa E501
    )

    def __repr__(self):
        return f"<GOMaster(go_id={self.go_id})>"


class GORelation(Base):
    """
    Represents parent-child relationships between GO terms in a directed
    acyclic graph (DAG).

    Each record links two GO terms (`parent_id`, `child_id`) using a specified
    relation type, such as 'is_a', 'part_of', or 'regulates'. This structure
    captures the hierarchical and semantic relationships within the Gene
    Ontology system.

    Relationships:
        - parent_term: The parent GO term in the relation
        - child_term: The child GO term in the relation
    """

    __tablename__ = "go_relations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id = Column(
        Integer,
        ForeignKey("go_masters.id", ondelete="CASCADE"),
        nullable=False,  # noqa E501
    )  # noqa E501
    parent_term = relationship(
        "GOMaster", foreign_keys=[parent_id], back_populates="children"
    )  # noqa E501

    child_id = Column(
        Integer,
        ForeignKey("go_masters.id", ondelete="CASCADE"),
        nullable=False,  # noqa E501
    )  # noqa E501
    child_term = relationship(
        "GOMaster", foreign_keys=[child_id], back_populates="parents"
    )  # noqa E501

    relation_type = Column(
        String(50), nullable=False
    )  # e.g., 'is_a', 'part_of'                   # noqa E501

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=False,
    )
    data_source = relationship("ETLDataSource", passive_deletes=True)

    etl_package_id = Column(
        Integer,
        ForeignKey("etl_packages.id", ondelete="CASCADE"),
        nullable=True,
    )
    etl_package = relationship("ETLPackage", passive_deletes=True)
