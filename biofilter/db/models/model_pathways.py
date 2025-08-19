from biofilter.db.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime


class PathwayMaster(Base):
    """
    Stores individual biological pathways and their associated metadata.

    Each pathway is linked to a unique Biofilter Entity (`entity_id`) and
    identified by a standard `pathway_id` (e.g, R-HSA-109581 or KEGG:map00010).
    The pathway description provides a human-readable name or title. A ref
    to the originating DataSource is stored for provenance tracking.

    Relationships:
        - entity: Unique entity representation for the pathway
        - data_source: Provenance of the pathway (e.g., Reactome, KEGG)
    """

    __tablename__ = "pathway_masters"

    id = Column(Integer, primary_key=True, autoincrement=True)

    entity_id = Column(
        Integer,
        ForeignKey("entities.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )

    pathway_id = Column(String(100), nullable=False, index=True, unique=True)
    description = Column(String(255), nullable=True)

    data_source_id = Column(
        Integer,
        ForeignKey("etl_data_sources.id", ondelete="CASCADE"),
        nullable=False,  # noqa E501
    )

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,  # noqa E501
    )

    # Relationships (Go Up)
    entity = relationship("Entity")
    data_source = relationship("DataSource", passive_deletes=True)
