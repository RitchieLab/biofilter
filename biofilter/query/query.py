import logging
import pandas as pd
from typing import Union, List, Dict, Optional, Any

import pandas as pd
from sqlalchemy.orm import Session, aliased
from sqlalchemy import select, and_, or_, not_, func, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.inspection import inspect as sqla_inspect

from biofilter.db.models import (
    # System Models
    SystemConfig,
    BiofilterMetadata,
    GenomeAssembly,
    # Entity Models
    EntityGroup,
    Entity,
    EntityAlias,
    EntityRelationshipType,
    EntityRelationship,
    EntityLocation,
    # Curation Models
    ConflictStatus,
    ConflictResolution,
    CurationConflict,
    OmicStatus,
    # ETL Models
    ETLDataSource,
    ETLSourceSystem,
    ETLPackage,
    # Gene Models
    GeneMaster,
    GeneGroup,
    GeneLocusGroup,
    GeneLocusType,
    GeneGroupMembership,
    # Variant Models
    VariantSNP,
    VariantSNPMerge,
    VariantGWAS,
    VariantGWASSNP,
    # Protein Models
    ProteinMaster,
    ProteinPfam,
    ProteinPfamLink,
    ProteinEntity,
    # Pathway Models
    PathwayMaster,
    # GO Models
    GOMaster,
    GORelation,
    # Diseases
    DiseaseGroup,
    DiseaseGroupMembership,
    DiseaseMaster,
    # Chemical
    ChemicalMaster,
)

from biofilter.utils.logger import Logger


class Query:
    """
    Generic query interface for Biofilter3R.
    Provides helpers for model access, query execution, and inspection.
    """

    def __init__(self, session: Session):
        self.session = session
        self.logger = Logger()

        # Methods from SQLAchemy
        self.select = select
        self.and_ = and_
        self.or_ = or_
        self.not_ = not_
        self.func = func
        self.aliased = aliased

        # Registry of models available to reports and ad-hoc queries
        self.models: Dict[str, Any] = {
            # System
            "SystemConfig": SystemConfig,
            "BiofilterMetadata": BiofilterMetadata,
            "GenomeAssembly": GenomeAssembly,
            # Entity
            "Entity": Entity,
            "EntityAlias": EntityAlias,
            "EntityGroup": EntityGroup,
            "EntityRelationshipType": EntityRelationshipType,
            "EntityRelationship": EntityRelationship,
            "EntityLocation": EntityLocation,
            # Curation
            "CurationConflict": CurationConflict,
            "ConflictStatus": ConflictStatus,
            "ConflictResolution": ConflictResolution,
            "OmicStatus": OmicStatus,
            # ETL
            "ETLSourceSystem": ETLSourceSystem,
            "ETLDataSource": ETLDataSource,
            "ETLPackage": ETLPackage,
            # Gene
            "GeneMaster": GeneMaster,
            "GeneGroup": GeneGroup,
            "GeneLocusGroup": GeneLocusGroup,
            "GeneLocusType": GeneLocusType,
            "GeneGroupMembership": GeneGroupMembership,
            # Variant
            "VariantSNP": VariantSNP,
            "VariantSNPMerge": VariantSNPMerge,
            "VariantGWAS": VariantGWAS,
            "VariantGWASSNP": VariantGWASSNP,
            # Protein
            "ProteinMaster": ProteinMaster,
            "ProteinPfam": ProteinPfam,
            "ProteinPfamLink": ProteinPfamLink,
            "ProteinEntity": ProteinEntity,
            # Pathway
            "PathwayMaster": PathwayMaster,
            # GO
            "GOMaster": GOMaster,
            "GORelation": GORelation,
            # Disease
            "DiseaseGroup": DiseaseGroup,
            "DiseaseGroupMembership": DiseaseGroupMembership,
            "DiseaseMaster": DiseaseMaster,
            # Chemical
            "ChemicalMaster": ChemicalMaster,
        }

        # Optional autocomplete-style access: query.GeneMaster, query.Entity, etc.
        for name, model in self.models.items():
            setattr(self, name, model)

    # def __call__(self, stmt, return_df=True):
    #     return self.run_query(stmt, return_df)

    def _to_dict(self, obj: Any) -> Dict[str, Any]:
        """Convert SQLAlchemy Row / ORM object / scalar into a flat dict."""

        # --- Case 1: SQLAlchemy Row (2.0) ---
        if hasattr(obj, "_mapping"):
            mapping = obj._mapping

            # If we have a single column / model in the Row
            if len(mapping) == 1:
                value = list(mapping.values())[0]

                # Single ORM instance → expand to columns
                if hasattr(value, "__table__"):
                    mapper = sqla_inspect(value.__class__)
                    return {col.key: getattr(value, col.key) for col in mapper.columns}

                # Single scalar → wrap as 'value'
                return {"value": value}

            # Multiple entries in the Row: flatten each
            flat: Dict[str, Any] = {}
            for key, value in mapping.items():
                if hasattr(value, "__table__"):  # ORM instance
                    mapper = sqla_inspect(value.__class__)
                    for col in mapper.columns:
                        flat[f"{key}_{col.key}"] = getattr(value, col.key)
                else:
                    flat[key] = value
            return flat

        # --- Case 2: plain tuple ---
        if isinstance(obj, tuple):
            return {f"col_{i}": v for i, v in enumerate(obj)}

        # --- Case 3: direct ORM object (from scalars().all(), for example) ---
        if hasattr(obj, "__table__"):
            mapper = sqla_inspect(obj.__class__)
            return {col.key: getattr(obj, col.key) for col in mapper.columns}

        # --- Case 4: simple scalar ---
        if hasattr(obj, "__dict__"):
            # generic Python object – best effort
            return {
                k: v
                for k, v in vars(obj).items()
                if not k.startswith("_sa_instance_state")
            }

        return {"value": obj}

    def _get_model(self, model_name: str) -> Optional[Any]:
        """Return a model class by name (or None if not found)."""
        return self.models.get(model_name)

    def get(self, model_name: str, **filters) -> pd.DataFrame:
        """
        Run a simple equality-based query using keyword filters and return a DataFrame.

        Example:
            bf.query.get("GeneMaster", symbol="A1BG")

        Notes:
        - This helper intentionally supports only equality filters.
        - For joins, IN queries, ranges, and more advanced logic, use `run_model(select(...))`.
        """
        try:
            model = model_name
            if isinstance(model_name, str):
                model = self._get_model(model_name)

            if model is None:
                raise ValueError(f"Model '{model_name}' not found.")

            stmt = select(model).filter_by(**filters)
            return self.run_model(stmt)

        except (ValueError, SQLAlchemyError) as e:
            self.logger.log(str(e), "ERROR")
            return pd.DataFrame()

    def run_model(self, stmt) -> pd.DataFrame:
        """
        Execute a SQLAlchemy statement (ORM/Core) and return a pandas DataFrame.

        Intended for interactive exploration in notebooks: always returns a DataFrame
        and logs errors instead of raising, so exploration is resilient.
        """
        if hasattr(stmt, "__table__"):   # user passed a model class
            stmt = select(stmt)
            # ex: bf.query.model(bf.query.GeneMaster).head()

        try:
            result = self.session.execute(stmt).all()
            data = [self._to_dict(r) for r in result]
            return pd.DataFrame(data)
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.log(str(e), "ERROR")
            return pd.DataFrame()

    def run_sql(self, sql: str) -> pd.DataFrame:
        """
        Execute raw SQL and return a pandas DataFrame.

        Intended for advanced/debug usage. Prefer SQLAlchemy statements whenever possible.
        """
        try:
            result = self.session.execute(text(sql)).all()
            data = [self._to_dict(r) for r in result]
            return pd.DataFrame(data)
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.log(str(e), "ERROR")
            return pd.DataFrame()


