from .model_config import SystemConfig, BiofilterMetadata, GenomeAssembly

from .model_genes import (
    GeneMaster,
    GeneGroup,
    GeneLocusGroup,
    GeneLocusType,
    GeneGenomicRegion,
    GeneGroupMembership,
    GeneLocation,
)
from .model_etl import DataSource, SourceSystem, ETLProcess, ETLLog

from .model_entities import (
    EntityGroup,
    Entity,
    EntityName,
    EntityRelationshipType,
    EntityRelationship,
)
from .model_curation import (
    ConflictStatus,
    ConflictResolution,
    CurationConflict,
    OmicStatus,
)
from .model_variants import (
    VariantMaster,
    VariantGeneRelationship,
)
from .model_pathways import PathwayMaster

from .model_proteins import (
    ProteinPfam,
    ProteinMaster,
    ProteinEntity,
    ProteinPfamLink,
)
from .model_go import GOMaster, GORelation

__all__ = [
    # # CONFIGURATION MODELS
    "SystemConfig",
    "BiofilterMetadata",
    "GenomeAssembly",
    # # GENE MODELS
    "GeneMaster",
    "GeneGroup",
    "GeneLocusGroup",
    "GeneLocusType",
    "GeneGenomicRegion",
    "GeneGroupMembership",
    "GeneLocation",
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
    "OmicStatus",
    # VARIANTS MODELS
    "VariantMaster",
    "VariantGeneRelationship",
    # PATHWAY MODELS
    "PathwayMaster",
    # PROTEIN MODELS
    "ProteinPfam",
    "ProteinMaster",
    "ProteinEntity",
    "ProteinPfamLink",
    # GENE ONTOLOGY MODELS
    "GOMaster",
    "GORelation",
]
