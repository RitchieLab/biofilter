# from .report_etl_status import ETLStatusReport
from .report_template import TemplateReport
from .report_entity_filter import EntityFilterReport
from .report_entity_relationship_model import EntityRelationshipModelReport
from .report_variant_gene_location_model import VariantGeneLocationModelReport
from .report_gene_master_annotation import GeneMasterAnnotationReport
from .report_pathway_master_annotation import PathwayMasterAnnotationReport
from .report_protein_master_annotation import ProteinMasterAnnotationReport
from .report_disease_master_annotation import DiseaseMasterAnnotationReport
from .report_go_master_annotation import GOMasterAnnotationReport
from .report_chemical_master_annotation import ChemicalMasterAnnotationReport


__all__ = [
    # Mapping all Reports
    # "ETLStatusReport",
    "TemplateReport",
    "EntityFilterReport",
    "EntityRelationshipModelReport",
    "VariantGeneLocationModelReport",
    "GeneMasterAnnotationReport",
    "PathwayMasterAnnotationReport",
    "ProteinMasterAnnotationReport",
    "DiseaseMasterAnnotationReport",
    "GOMasterAnnotationReport",
    "ChemicalMasterAnnotationReport",
]
