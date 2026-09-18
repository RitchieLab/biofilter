# from sqlalchemy.sql import func

from sqlalchemy import Column, Integer, String

from biofilter.modules.db.base import Base


class OmicStatus(Base):
    """
    Represents the annotation status or review flag for an omics entity
    (e.g., Gene).

    Used in curation pipelines to mark reviewed, pending, or rejected entries.
    """

    __tablename__ = "omic_status"

    id = Column(Integer, primary_key=True)
    name = Column(String(30), unique=True, nullable=False)
    description = Column(String(100), nullable=True)
