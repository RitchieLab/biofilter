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
from .model_etl import ETLDataSource, ETLSourceSystem, ETLPackage

from .model_entities import (
    EntityGroup,
    Entity,
    # EntityName,
    EntityAlias,
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
    VariantLocus,
    # VariantLiftedPosition,
    # VariantMergeLog,
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
    "ETLDataSource",
    "ETLSourceSystem",
    "ETLPackage",
    # # ENTITY MODELS
    "EntityGroup",
    "Entity",
    # "EntityName",
    "EntityAlias",
    "EntityRelationshipType",
    "EntityRelationship",
    # CURATION MODELS
    "ConflictStatus",
    "ConflictResolution",
    "CurationConflict",
    "OmicStatus",
    # VARIANTS MODELS
    "VariantMaster",
    "VariantLocus",
    # "VariantLiftedPosition",
    # "VariantMergeLog",
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
