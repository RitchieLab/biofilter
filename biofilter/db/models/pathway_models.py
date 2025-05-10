from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from biofilter.db.base import Base
import datetime


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


class Pathway(Base):
    __tablename__ = "pathways"

    id = Column(Integer, primary_key=True)
    entity_id = Column(Integer, ForeignKey("entities.id"), nullable=False, unique=True)

    reactome_id = Column(String, nullable=False, index=True, unique=True)  # ex: R-HSA-123456
    short_name = Column(String, nullable=False)     # Nome curto
    full_name = Column(String)                      # Nome descritivo longo (opcional)
    species = Column(String, default="Homo sapiens")
    data_source_id = Column(Integer, ForeignKey("etl_data_sources.id"), nullable=False)

    entity = relationship("Entity", backref="pathway", uselist=False)
    data_source = relationship("DataSource")
