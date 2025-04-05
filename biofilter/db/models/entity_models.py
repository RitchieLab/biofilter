from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from biofilter.db.base import Base
import datetime


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


class EntityGroup(Base):
    __tablename__ = "entity_groups"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(String)


class EntityCategory(Base):
    __tablename__ = "entity_categories"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(String)


class Entity(Base):
    __tablename__ = "entities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_type = Column(String, nullable=False)
    group_id = Column(Integer, nullable=True)     # Sem FK
    category_id = Column(Integer, nullable=True)  # Sem FK
    description = Column(String)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=utcnow)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)

    names = relationship("EntityName", back_populates="entity", cascade="all, delete-orphan")


class EntityName(Base):
    __tablename__ = "entity_names"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_id = Column(Integer, nullable=False)  # Sem FK
    source = Column(String, nullable=False)      # entrez, ensembl, etc.
    name = Column(String, nullable=False)
    is_primary = Column(Boolean, default=False)
    created_at = Column(DateTime, default=utcnow)

    entity = relationship("Entity", back_populates="names")


class Relationship(Base):
    __tablename__ = "relationships"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_1_id = Column(Integer, nullable=False)
    entity_1_type = Column(String, nullable=False)
    entity_2_id = Column(Integer, nullable=False)
    entity_2_type = Column(String, nullable=False)
    relationship_type = Column(String, nullable=False)  # is_a, regulates, etc
    role = Column(String, nullable=True)
    created_at = Column(DateTime, default=utcnow)


"""
entity = Entity(
    entity_type="gene",
    group="genomics",
    category="protein-coding",
    description="Gene A1BG"
)

entity.names = [
    EntityName(source="symbol", name="A1BG", is_primary=True),
    EntityName(source="ensembl", name="ENSG00000121410"),
    EntityName(source="entrez", name="1"),
]
"""