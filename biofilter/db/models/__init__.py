from .config_models import SystemConfig

from .genes_models import Gene, GeneGroup
from .etl_models import DataSource, SourceSystem, ETLProcess, ETLLog
from .entity_models import (
    EntityGroup,
    Entity,
    EntityName,
    EntityRelationshipType,
    EntityRelationship,
)
from .curation_models import (
    ConflictStatus,
    ConflictResolution,
    CurationConflict,
)
from .variants_models import (
    # VariantType,
    # AlleleType,
    GenomeAssembly,
    Variant,
    # VariantLocation,
    VariantGeneRelationship,
)
from .pathway_models import Pathway
from .protein_models import (
    ProteinPfam,
    ProteinMaster,
    ProteinEntity,
    ProteinPfamLink,
)
from .go_models import GOMaster, GORelation

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
    "EntityRelationshipType",
    "EntityRelationship",
    # CURATION MODELS
    "ConflictStatus",
    "ConflictResolution",
    "CurationConflict",
    # VARIANTS MODELS
    # "VariantType",
    # "AlleleType",
    "GenomeAssembly",
    "Variant",
    # "VariantLocation",
    "VariantGeneRelationship",
    "Pathway",
    # PROTEIN MODELS
    "ProteinPfam",
    "ProteinMaster",
    "ProteinEntity",
    "ProteinPfamLink",
    # GENE ONTOLOGY MODELS
    "GOMaster",
    "GORelation",
]
