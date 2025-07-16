# biofilter/models/gene_ontology_models.py

from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship
from biofilter.db.base import Base


class GOMaster(Base):
    __tablename__ = "go_master"

    id = Column(Integer, primary_key=True)
    go_id = Column(String, unique=True, nullable=False)
    entity_id = Column(Integer, nullable=False)
    name = Column(String, nullable=False)
    namespace = Column(String, nullable=False)  # MF, BP, CC

    # Optional: relations to other GO terms (DAG)
    parents = relationship(
        "GORelation", back_populates="child_term", foreign_keys="GORelation.child_id"
    )
    children = relationship(
        "GORelation", back_populates="parent_term", foreign_keys="GORelation.parent_id"
    )


class GORelation(Base):
    __tablename__ = "go_relations"

    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey("go_master.id"))
    child_id = Column(Integer, ForeignKey("go_master.id"))
    relation_type = Column(String)  # e.g., 'is_a', 'part_of'

    parent_term = relationship(
        "GOMaster", foreign_keys=[parent_id], back_populates="children"
    )
    child_term = relationship(
        "GOMaster", foreign_keys=[child_id], back_populates="parents"
    )


# Trocado por EntityRelationship
# class GeneGoAnnotation(Base):
#     __tablename__ = "gene_go_annotation"

#     id = Column(Integer, primary_key=True)
#     gene_id = Column(Integer, ForeignKey("gene.id"))
#     go_term_id = Column(Integer, ForeignKey("gene_ontology_term.id"))
#     evidence_code = Column(String)
#     source = Column(String)

#     gene = relationship("Gene", back_populates="go_annotations")
#     go_term = relationship("GeneOntologyTerm")
"""
🎯 Valores típicos para relation_type
Você pode criar uma enum ou uma tabela para os tipos de relação GO, como:

has_function → Gene/proteína realiza essa função

participates_in → Participa de um processo biológico

localized_to → Componente celular

Evidências (evidence_code) e fonte (source) podem ser adicionadas como colunas extras em EntityRelation ou mantidas em uma extensão auxiliar (EntityRelationMetadata), se quiser granularidade.
"""
