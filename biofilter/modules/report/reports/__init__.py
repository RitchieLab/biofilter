# from .report_etl_status import ETLStatusReport
from .report_template import TemplateReport
from .report_entity_filter import EntityFilterReport
from .report_entity_relationship_model import EntityRelationshipModelReport
from .report_variant_gene_location_model import VariantGeneLocationModelReport
from .report_snp_snp_model import SNPSNPModelReport
from .report_annotation_master_gene import AnnotationMasterGeneReport
from .report_annotation_master_pathway import AnnotationMasterPathwayReport
from .report_annotation_master_protein import AnnotationMasterProteinReport
from .report_annotation_master_disease import AnnotationMasterDiseaseReport
from .report_annotation_master_go import AnnotationMasterGOReport
from .report_annotation_master_chemical import AnnotationMasterChemicalReport
from .report_platform_data_statistics import PlatformDataStatisticsReport


__all__ = [
    # Mapping all Reports
    # "ETLStatusReport",
    "TemplateReport",
    "EntityFilterReport",
    "EntityRelationshipModelReport",
    "VariantGeneLocationModelReport",
    "SNPSNPModelReport",
    "AnnotationMasterGeneReport",
    "AnnotationMasterPathwayReport",
    "AnnotationMasterProteinReport",
    "AnnotationMasterDiseaseReport",
    "AnnotationMasterGOReport",
    "AnnotationMasterChemicalReport",
    "PlatformDataStatisticsReport",
]
