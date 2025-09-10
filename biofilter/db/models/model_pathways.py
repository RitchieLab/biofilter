from biofilter.db.base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, String, ForeignKey


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

    pathway_id = Column(String(100), nullable=False, index=True, unique=True)
    description = Column(String(255), nullable=True)

    entity_id = Column(
        Integer, ForeignKey("entities.id", ondelete="CASCADE"), nullable=False
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
