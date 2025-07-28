import logging
from typing import Union, List, Dict, Optional, Any
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select, and_, or_, not_, func
from sqlalchemy.exc import SQLAlchemyError

# Import all models
from biofilter.db.models import (
    # System Models
    SystemConfig,
    BiofilterMetadata,
    # Entity Models
    Entity,
    EntityName,
    EntityGroup,
    EntityRelationshipType,
    EntityRelationship,
    # Curation Models
    CurationConflict,
    # ETL Models
    SourceSystem,
    DataSource,
    ETLLog,
    ETLProcess,
    # Gene Models
    Gene,
    GeneGroup,
    LocusGroup,
    LocusType,
    GenomicRegion,
    OmicStatus,
    GeneGroupMembership,
    GeneLocation,
    # Variant Models
    GenomeAssembly,
    Variant,
    VariantGeneRelationship,
    # Protein Models
    ProteinMaster,
    ProteinPfam,
    ProteinPfamLink,
    ProteinEntity,
    # Pathway Models
    Pathway,
    # GO Models
    GOMaster,
    GORelation,
)


# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Query:
    """
    Generic query interface for Biofilter3R.
    Provides helpers for model access, query execution, and inspection.
    """

    def __init__(self, session: Session):
        self.session = session
        self.select = select
        self.and_ = and_
        self.or_ = or_
        self.not_ = not_
        self.func = func

        self.models: Dict[str, Any] = {
            # System
            "SystemConfig": SystemConfig,
            "BiofilterMetadata": BiofilterMetadata,
            # Entity
            "Entity": Entity,
            "EntityName": EntityName,
            "EntityGroup": EntityGroup,
            "EntityRelationshipType": EntityRelationshipType,
            "EntityRelationship": EntityRelationship,
            # Curation
            "CurationConflict": CurationConflict,
            # ETL
            "SourceSystem": SourceSystem,
            "DataSource": DataSource,
            "ETLLog": ETLLog,
            "ETLProcess": ETLProcess,
            # Gene
            "Gene": Gene,
            "GeneGroup": GeneGroup,
            "LocusGroup": LocusGroup,
            "LocusType": LocusType,
            "GenomicRegion": GenomicRegion,
            "OmicStatus": OmicStatus,
            "GeneGroupMembership": GeneGroupMembership,
            "GeneLocation": GeneLocation,
            # Variant
            "GenomeAssembly": GenomeAssembly,
            "Variant": Variant,
            "VariantGeneRelationship": VariantGeneRelationship,
            # Protein
            "ProteinMaster": ProteinMaster,
            "ProteinPfam": ProteinPfam,
            "ProteinPfamLink": ProteinPfamLink,
            "ProteinEntity": ProteinEntity,
            # Pathway
            "Pathway": Pathway,
            # GO
            "GOMaster": GOMaster,
            "GORelation": GORelation,
        }

        # Optional autocomplete access
        for name, model in self.models.items():
            setattr(self, name, model)

    def get_model(self, model_name: str) -> Optional[Any]:
        """Return a model class by name."""
        return self.models.get(model_name)

    def _to_dict(self, obj) -> Dict[str, Any]:
        return {
            k: v for k, v in vars(obj).items() if not k.startswith("_sa_instance_state")  # noqa E501
        }

    def run_query(
        self, stmt, return_df: bool = False
    ) -> Union[List[Any], pd.DataFrame]:
        """Execute a SQLAlchemy statement and return results."""
        try:
            result = self.session.execute(stmt).scalars().all()
            if return_df:
                df = pd.DataFrame([self._to_dict(r) for r in result])
                return df
            return result
        except SQLAlchemyError as e:
            logger.warning(f"Query failed: {e}")
            return []

    def raw_sql(self, sql: str) -> List[Any]:
        """Run a raw SQL string and return results."""
        try:
            return self.session.execute(sql).fetchall()
        except SQLAlchemyError as e:
            logger.warning(f"Raw SQL failed: {e}")
            return []

    def list_models(self) -> List[str]:
        """List all registered models."""
        return list(self.models.keys())

    def describe_model(self, model_name: str) -> Optional[Dict[str, List[str]]]:  # noqa E501
        """Return column and relationship info for a given model."""
        model = self.get_model(model_name)
        if not model:
            logger.warning(f"Model '{model_name}' not found.")
            return None
        return {
            "columns": list(model.__table__.columns.keys()),
            "relationships": list(model.__mapper__.relationships.keys()),
        }

    def get_model_metadata(self, model_name: str) -> Optional[Dict[str, Any]]:
        """Return detailed metadata about a model's columns and relationships."""  # noqa E501
        model = self.get_model(model_name)
        if not model:
            return None
        return {
            "columns": {
                col.name: {
                    "type": str(col.type),
                    "nullable": col.nullable,
                    "primary_key": col.primary_key,
                }
                for col in model.__table__.columns
            },
            "relationships": list(model.__mapper__.relationships.keys()),
        }

    def query_model(self, model_name: str, **filters) -> List[Any]:
        """Run a query on a model using keyword filters (e.g., hgnc_id='1234')."""  # noqa E501
        model = self.get_model(model_name)
        if not model:
            raise ValueError(f"Model '{model_name}' not found.")
        stmt = select(model).filter_by(**filters)
        return self.run_query(stmt)
