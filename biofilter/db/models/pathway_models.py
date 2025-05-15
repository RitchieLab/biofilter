from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from biofilter.db.base import Base

# import datetime


# def utcnow():
#     return datetime.datetime.now(datetime.timezone.utc)

# NOTE: Future improvement: Add Species and/or Organism in the Biofilter


class Pathway(Base):
    __tablename__ = "pathways"

    id = Column(Integer, primary_key=True)
    entity_id = Column(
        Integer, ForeignKey("entities.id"), nullable=False, unique=True
    )  # noqa E501
    pathway_id = Column(String, nullable=False, index=True, unique=True)  # noqa E501
    description = Column(String, nullable=True)
    data_source_id = Column(
        Integer, ForeignKey("etl_data_sources.id"), nullable=False
    )  # noqa E501

    entity = relationship("Entity", backref="pathway", uselist=False)
    data_source = relationship("DataSource")
