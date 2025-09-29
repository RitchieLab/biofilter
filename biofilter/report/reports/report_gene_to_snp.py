# biofilter/report/reports/report_gene_to_snp.py

import pandas as pd
from biofilter.report.reports.base_report import ReportBase
from biofilter.db.models import (
    Entity,
    EntityAlias,
    EntityGroup,
    EntityRelationship,
    VariantMaster,
    VariantLocus,
)

from sqlalchemy.orm import aliased
from sqlalchemy import func


class GeneToSNPReport(ReportBase):
    name = "gene_to_snp"
    description = "Given a list of genes, returns gene metadata and associated variants (SNPs) with positional and allelic info."

    def run(self):
        input_data_raw = self.params.get("input_data")
        input_data = self.resolve_input_list(input_data_raw)

        assembly_input = self.params.get("assembly")
        assembly_id, chrom_to_asm = self.resolve_assembly(assembly_input, return_mapper=True)

        # Em consultas:
        # .filter(VariantLocus.assembly_id == chrom_to_asm["17"])

        if not input_data or not assembly_id:
            raise ValueError("Parameters 'input_data' and 'assembly' are required.")
        
        self.logger.info(f"🔍 Running GeneToSNPReport with {len(input_data)} inputs and assembly={assembly_id}")

        # Step 1: Resolve input genes via EntityAlias (case-insensitive match)
        input_map = {x.lower(): x for x in input_data}
        input_lc = list(input_map.keys())

        GeneGroup = (
            self.session.query(EntityGroup.id)
            .filter(EntityGroup.name.ilike("Genes"))
            .scalar()
        )
        if not GeneGroup:
            raise ValueError("EntityGroup 'Genes' not found in the database.")

        # Aliases
        PrimaryName = aliased(EntityAlias)

        gene_query = (
            self.session.query(
                func.lower(EntityAlias.name).label("input_lc"),
                EntityAlias.name.label("matched_name"),
                Entity.id.label("entity_id"),
                PrimaryName.name.label("symbol"),
                Entity.group_id,
                EntityGroup.name.label("group_name"),
                Entity.has_conflict,
                Entity.is_deactive,
            )
            .join(Entity, Entity.id == EntityAlias.entity_id)
            .join(PrimaryName, PrimaryName.entity_id == Entity.id)
            .join(EntityGroup, Entity.group_id == EntityGroup.id)
            .filter(Entity.group_id == GeneGroup)
            .filter(PrimaryName.is_primary.is_(True))
            .filter(func.lower(EntityAlias.name).in_(input_lc))
        )

        gene_df = pd.DataFrame(gene_query.all())
        gene_df["input"] = gene_df["input_lc"].map(input_map)
        gene_df.drop(columns=["input_lc"], inplace=True)

        if gene_df.empty:
            self.logger.warn("No genes found.")
            return {"genes": pd.DataFrame(), "variants": pd.DataFrame()}

        gene_ids = gene_df["entity_id"].unique().tolist()

        # Step 2: Find variants linked to these genes via EntityRelationship
        rel_query = (
            self.session.query(
                EntityRelationship.entity_1_id.label("gene_entity_id"),
                EntityRelationship.entity_2_id.label("variant_entity_id")
            )
            .filter(EntityRelationship.entity_1_id.in_(gene_ids))
        )

        rel_df = pd.DataFrame(rel_query.all())
        if rel_df.empty:
            self.logger.warn("No variant relationships found.")
            return {"genes": gene_df, "variants": pd.DataFrame()}

        # Step 3: Get variant metadata for the requested assembly
        variant_query = (
            self.session.query(
                VariantMaster.entity_id.label("variant_entity_id"),
                VariantMaster.variant_id,
                VariantMaster.chromosome,
                VariantMaster.accession,
                VariantMaster.assembly,
                VariantMaster.start_pos,
                VariantMaster.end_pos,
                VariantMaster.ref,
                VariantMaster.alt
            )
            .filter(VariantMaster.entity_id.in_(rel_df["varaint_entity_id"].tolist()))
            .filter(VariantMaster.assembly == assembly_id)
        )

        variant_df = pd.DataFrame(variant_query.all())
        if variant_df.empty:
            self.logger.warn("No variant metadata found for selected assembly.")
            return {"genes": gene_df, "variants": pd.DataFrame()}

        # Step 4: Join back to gene relationships to form final output
        merged_df = rel_df.merge(
            gene_df[["entity_id", "symbol"]],
            left_on="gene_entity_id",
            right_on="entity_id",
            how="left"
        ).drop(columns="entity_id")

        final_df = merged_df.merge(
            variant_df,
            on="variant_entity_id",
            how="left"
        )

        self.results = {"genes": gene_df, "variants": final_df}
        return self.results

    def to_dataframe(self, data=None):
        # Default behavior: return variants table
        if not data:
            data = self.results
        return data["variants"]


"""
bf = Biofilter("sqlite:///biofilter.db")

result = bf.report.run_report(
    "report_gene_to_snp",
    input_data=["TP53", "ENSG00000141510", "7157"],
    assembly="GRCh38"
)

# Acesso separado aos dois DataFrames:
genes_df = result["genes"]
variants_df = result["variants"]

"""