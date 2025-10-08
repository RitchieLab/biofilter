from biofilter.report.reports.base_report import ReportBase
from sqlalchemy.orm import aliased
from sqlalchemy import or_
from biofilter.db.models import (
    VariantMaster,
    VariantLocus,
    Entity,
    EntityAlias,
    EntityGroup,
    EntityRelationship,
    EntityRelationshipType,
    GenomeAssembly,
)


class PositionToVariantReport(ReportBase):
    name = "position_to_variant"
    description = (
    "Given a genomic position (assembly, chromosome, position), "
    "returns matching variants with allelic and gene information."
    )

    def example_input(self):
        return [
            {"assembly": "38", "chromosome": "Y", "position": 19568371},
            {"assembly": "38", "chromosome": "Y", "position": 19568761},
            {"assembly": "38", "chromosome": "1", "position": 258},
        ]

    @classmethod
    def explain(cls) -> str:
        return """
### 📍 Position to Variant Report

This report takes a list of genomic positions as input and returns:

- Variants overlapping those positions (if any), including:
  - Variant ID, type, alleles, accession, quality
  - Assembly and coordinates
- Gene(s) related to each variant, if any

Input Format:
```python
[
    {"assembly": "38", "chromosome": "1", "position": 123456},
    {"assembly": "GRCh37", "chromosome": "2", "position": 7654321},
]
```

Example:
```python
result = bf.report.run_report(
    "position_to_variant",
    input_data=[
        {"assembly": "38", "chromosome": "Y", "position": 19568371},
        {"assembly": "38", "chromosome": "Y", "position": 19568761}
    ]
)
```
        """

    # NOTE / TODO: Revisar e tentar otimizar esse report
    def run(self):
        if not isinstance(self.input_data, list):
            raise ValueError("Expected a list of position dictionaries.")

        records = []
        for entry in self.input_data:
            try:
                assembly_id = self.resolve_assembly(entry["assembly"])
                chrom = str(entry["chromosome"]).strip()
                pos = int(entry["position"])
            except Exception as e:
                records.append({
                    **entry,
                    "note": f"Invalid input or assembly: {e}"
                })
                continue

            # Find overlapping variants
            matches = (
                self.session.query(
                    VariantMaster.variant_id,
                    Variant.variant_type,
                    Variant.ref,
                    Variant.alt,
                    Variant.quality,
                    VariantLocus.start_pos,
                    VariantLocus.end_pos,
                    VariantLocus.chromosome,
                    VariantLocus.accession,
                    GenomeAssembly.assembly_name,
                    Variant.id.label("variant_db_id"),
                )
                .join(VariantLocus, Variant.id == VariantLocus.variant_id)
                .join(GenomeAssembly, VariantLocus.assembly_id == GenomeAssembly.id)
                .filter(
                    VariantLocus.assembly_id == assembly_id,
                    VariantLocus.chromosome == chrom,
                    VariantLocus.start_pos <= pos,
                    VariantLocus.end_pos >= pos,
                )
                .all()
            )

            if not matches:
                records.append({
                    **entry,
                    "note": "No variant found at this position."
                })
                continue

            for m in matches:
                # Lookup associated genes (optional)
                Gene = aliased(Entity)
                GeneGroup = aliased(EntityGroup)
                link = (
                    self.session.query(EntityAlias.alias_value.label("gene_symbol"))
                    .join(Entity, Entity.id == EntityAlias.entity_id)
                    .join(EntityRelationship, Entity.id == EntityRelationship.entity_1_id)
                    .join(Gene, Gene.id == EntityRelationship.entity_2_id)
                    .join(GeneGroup, Gene.group_id == GeneGroup.id)
                    .filter(
                        EntityRelationship.entity_1_id == m.variant_db_id,
                        GeneGroup.name == "Genes",
                        EntityAlias.is_primary.is_(True),
                    )
                    .limit(1)
                    .scalar()
                )

                records.append({
                    **entry,
                    "variant_id": m.variant_id,
                    "variant_type": m.variant_type,
                    "start_pos": m.start_pos,
                    "end_pos": m.end_pos,
                    "ref": m.ref,
                    "alt": m.alt,
                    "accession": m.accession,
                    "assembly_name": m.assembly_name,
                    "quality": m.quality,
                    "gene_symbol": link,
                    "note": None
                })

        return self.as_dataframe(records)
