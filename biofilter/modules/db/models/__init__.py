from .model_chemicals import ChemicalMaster
from .model_config import BiofilterMetadata, GenomeAssembly, SystemConfig
from .model_curation import OmicStatus
from .model_diseases import DiseaseGroup, DiseaseGroupMembership, DiseaseMaster
from .model_entities import (
    Entity,
    EntityAlias,
    EntityGroup,
    EntityLocation,
    EntityRelationship,
    EntityRelationshipType,
)
from .model_etl import ETLDataSource, ETLPackage, ETLSourceSystem
from .model_genes import (
    GeneGroup,
    GeneGroupMembership,
    GeneLocusGroup,
    GeneLocusType,
    GeneMaster,
)
from .model_go import GOMaster, GORelation
from .model_pathways import PathwayMaster
from .model_proteins import (  # noqa: E501
    ProteinEntity,
    ProteinMaster,
    ProteinPfam,
    ProteinPfamLink,
)
from .model_variants import (
    VariantBiotype,
    VariantConsequence,
    VariantConsequenceCategory,
    VariantConsequenceGroup,
    VariantGWAS,
    VariantGWASSNP,
    VariantImpact,
    VariantSNPMerge,
)

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
    "GeneGroupMembership",

    # # ETL MODELS
    "ETLDataSource",
    "ETLSourceSystem",
    "ETLPackage",

    # # ENTITY MODELS
    "EntityGroup",
    "Entity",
    "EntityAlias",
    "EntityRelationshipType",
    "EntityRelationship",
    "EntityLocation",

    # CURATION MODELS
    "OmicStatus",

    # VARIANTS MODELS
    "VariantSNP",
    "VariantSNPMerge",
    "VariantGWAS",
    "VariantGWASSNP",
    "VariantConsequenceGroup",
    "VariantConsequenceCategory",
    "VariantConsequence",
    "VariantImpact",
    "VariantBiotype",

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

    # DISEASE MODELS
    "DiseaseGroup",
    "DiseaseGroupMembership",
    "DiseaseMaster",

    # CHEMICAL MODELS
    "ChemicalMaster",
]
