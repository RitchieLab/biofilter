from __future__ import annotations

import pandas as pd
from typing import Optional, List, Dict, Any

from biofilter.query import Query  


class SchemaExplorer:
    def __init__(self, query: Query):
        self.query = query
        self.models = query.models

        # High-level grouping for schema inspection
        self.groups: Dict[str, str] = {
            # System
            "SystemConfig": "System",
            "BiofilterMetadata": "System",
            "GenomeAssembly": "System",
            # Entity
            "Entity": "Entity",
            "EntityAlias": "Entity",
            "EntityGroup": "Entity",
            "EntityRelationshipType": "Entity",
            "EntityRelationship": "Entity",
            "EntityLocation": "Entity",
            # Curation
            "CurationConflict": "Curation",
            "ConflictStatus": "Curation",
            "ConflictResolution": "Curation",
            "OmicStatus": "Curation",
            # ETL
            "ETLSourceSystem": "ETL",
            "ETLDataSource": "ETL",
            "ETLPackage": "ETL",
            # Gene
            "GeneMaster": "Gene",
            "GeneGroup": "Gene",
            "GeneLocusGroup": "Gene",
            "GeneLocusType": "Gene",
            "GeneGroupMembership": "Gene",
            # Variant
            "VariantSNP": "Variant",
            "VariantSNPMerge": "Variant",
            "VariantGWAS": "Variant",
            "VariantGWASSNP": "Variant",
            # Protein
            "ProteinMaster": "Protein",
            "ProteinPfam": "Protein",
            "ProteinPfamLink": "Protein",
            "ProteinEntity": "Protein",
            # Pathway
            "PathwayMaster": "Pathway",
            # GO
            "GOMaster": "GO",
            "GORelation": "GO",
            # Disease
            "DiseaseGroup": "Disease",
            "DiseaseGroupMembership": "Disease",
            "DiseaseMaster": "Disease",
            # Chemical
            "ChemicalMaster": "Chemical",
        }

    def __call__(self, keyword: str | None = None):
        """
        Calling bf.schema behaves like bf.schema.models().

        Examples:
            bf.schema()               → list all models
            bf.schema("Gene")         → filter models by keyword
        """
        return self._models(keyword)

    def _models(self, keyword: str | None = None):
        """
        Return a list of model names.
        If keyword is provided, filter by substring (case-insensitive)
        in model name or group.
        """
        names = list(self.models.keys())

        if keyword:
            kw = keyword.lower()
            names = [
                name for name in names
                if kw in name.lower() or kw in self.groups.get(name, "").lower()
            ]

        return names

    def tables(self):
        """List all SQL tables."""
        return [
            (name, model.__tablename__)
            for name, model in self.models.items()
            if hasattr(model, "__table__")
        ]

    def search(self, keyword, return_df=True):
        """Search for models by keyword in name, table, columns, or relationships."""
        return self._build_df(keyword)

    def describe(self, model_name):
        """Pretty-print information about a single model."""
        model = self.models.get(model_name)
        if not model or not hasattr(model, "__table__"):
            print(f"Model '{model_name}' not found or not a SQLAlchemy table.")
            return

        table = model.__table__
        print(f"\n📘 Model: {model_name}")
        print(f"🔹 Table: {table.name}")
        print(f"🔸 Group: {self.groups.get(model_name)}")
        print("\nColumns:")
        for col in table.columns:
            key = " (PK)" if col.primary_key else ""
            print(f"  • {col.name}: {col.type}{key}")

        rels = model.__mapper__.relationships
        if rels:
            print("\nRelationships:")
            for r in rels:
                print(f"  → {r.key} ({r.direction.name})")
        print()

    def overview(self):
        """Return a DataFrame summarizing all Biofilter3R tables."""
        return self._build_df(keyword=None)

    def _build_df(self, keyword=None):
        """Internal generator for the rich schema DataFrame."""
        rows = []
        for name, model in self.models.items():
            if not hasattr(model, "__table__"):
                continue

            table = model.__table__
            row = {
                "group": self.groups.get(name),
                "model": name,
                "table": table.name,
                "columns": [col.name for col in table.columns],
                "primary_keys": [col.name for col in table.columns if col.primary_key],
                "relationships": [rel.key for rel in model.__mapper__.relationships],
            }

            if keyword:
                kw = keyword.lower()
                if not (
                    kw in name.lower()
                    or kw in table.name.lower()
                    or any(kw in c.lower() for c in row["columns"])
                    or any(kw in r.lower() for r in row["relationships"])
                ):
                    continue

            rows.append(row)

        df = pd.DataFrame(rows)
        df["columns"] = df["columns"].apply(lambda xs: ", ".join(xs))
        df["relationships"] = df["relationships"].apply(lambda xs: ", ".join(xs))
        df["primary_keys"] = df["primary_keys"].apply(lambda xs: ", ".join(xs))
        return df
    
        # BUG: Quando filtra a palavra Gene tras outros modelos
