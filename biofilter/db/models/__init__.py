from .config_models import SystemConfig

from .omics_models import Gene, GeneGroup
from .etl_models import DataSource, SourceSystem, ETLProcess, ETLLog
from .entity_models import (
    EntityGroup,
    Entity,
    EntityName,
    RelationshipType,
    EntityRelationship,
)
from .curation_models import (
    ConflictStatus,
    ConflictResolution,
    CurationConflict,
)
from .variants_models import (
    VariantType,
    AlleleType,
    GenomeAssembly,
    Variant,
    # VariantLocation,
    GeneVariantLink,
)

__all__ = [
    # # CONFIGURATION MODELS
    "SystemConfig",
    # # OMICS MODELS
    "GeneGroup",
    "Gene",
    # # ETL MODELS
    "SourceSystem",
    "DataSource",
    "ETLProcess",
    "ETLLog",
    # # ENTITY MODELS
    "EntityGroup",
    "Entity",
    "EntityName",
    "RelationshipType",
    "EntityRelationship",
    # CURATION MODELS
    "ConflictStatus",
    "ConflictResolution",
    "CurationConflict",
    # VARIANTS MODELS
    "VariantType",
    "AlleleType",
    "GenomeAssembly",
    "Variant",
    # "VariantLocation",
    "GeneVariantLink",
]
