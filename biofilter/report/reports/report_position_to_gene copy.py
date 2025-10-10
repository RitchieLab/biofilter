
import pandas as pd
from biofilter.report.reports.base_report import ReportBase
from sqlalchemy.orm import aliased
from sqlalchemy import union_all, select
from sqlalchemy import or_, and_
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


class PositionToGeneReport(ReportBase):
    group = "Annotation"
    name = "position_to_gene"
    description = (
    "Given a genomic position (chromosome, position), "
    "returns matching variants with allelic and gene information."
    )


    @classmethod
    def explain(cls) -> str:
        """
        Returns a markdown-style explanation of what this report does.
        """
        return """\
### 🧬 Position to Variant Report

    This report takes as input a list of (chromosome, position) tuples and returns:

    - All variants (`rsID`, position, alleles) at that location
    - Associated genes from curated relationships
    - Quality scores, accession and assembly name

    **Required arguments:**
    - `assembly` (e.g. `"38"` or `"GRCh38"`)

🧪 EXAMPLE USAGE
================

    ```python
    result = bf.report.run_report(
        "report_position_to_variant",
        assembly="38",
        input_data=[
            ("Y", 19568371),
            ("Y", 19568761),
            ("1", 258)
        ]
    )
    """

    @classmethod
    def example_input(cls):
        """
        Returns a sample input list of chromosome-position pairs.
        """
        return [
            ("Y", 19568371),
            ("Y", 19568761),
            ("1", 258)
        ]
    

    def run(self):
        # --- Step 1: Read Input ---
        input_data_raw = self.params.get("input_data")
        position_list = self.resolve_position_list(input_data_raw)
        if not position_list:
            self.logger.log("No valid positions provided.", "WARNING")
            return None

        # --- Step 2: Read Assembly ---
        assembly_input = self.params.get("assembly")
        chrom_to_assembly_id = self.resolve_assembly(assembly_input)
        if not chrom_to_assembly_id:
            self.logger.log("No assembly ID found for input", "WARNING")
            return None

        assembly_ids = list(set(chrom_to_assembly_id.values()))

        # --- Step 3: Get Group IDs ---
        gene_group_id = (
            self.session.query(EntityGroup.id)
            .filter(EntityGroup.name.ilike("Genes"))
            .scalar()
        )
        variant_group_id = (
            self.session.query(EntityGroup.id)
            .filter(EntityGroup.name.ilike("Variants"))
            .scalar()
        )
        if not gene_group_id or not variant_group_id:
            self.logger.log("Missing group IDs for Genes or Variants.", "WARNING")
            return None

        # --- Step 4: Query Variants at positions ---
        try:
            # Get all matching VariantLocus
            vl = aliased(VariantLocus)
            ga = aliased(GenomeAssembly)
            vm = aliased(VariantMaster)

            # Mount condition (multi-position search)
            conditions = []
            for chrom, pos in position_list:
                assembly_id = chrom_to_assembly_id.get(chrom.upper())
                if assembly_id:
                    conditions.append(
                        and_(
                            vl.chromosome == chrom,
                            vl.start_pos <= pos,
                            vl.end_pos >= pos,
                            vl.assembly_id == assembly_id,
                        )
                    )

            if not conditions:
                self.logger.log("No conditions could be built from input positions.", "WARNING")
                return None

            variant_query = (
                self.session.query(
                    vm.entity_id.label("variant_entity_id"),
                    vm.variant_id,
                    vl.chromosome,
                    vl.start_pos,
                    vl.end_pos,
                    vl.reference_allele.label("ref"),
                    vl.alternate_allele.label("alt"),
                    ga.assembly_name,
                    ga.accession,
                )
                .join(vl, vl.variant_id == vm.id)
                .join(ga, ga.id == vl.assembly_id)
                .filter(or_(*conditions))
            )

            variant_df = pd.read_sql(variant_query.statement, self.session.bind)
            if variant_df.empty:
                self.logger.log("No variants found at input positions.", "WARNING")
                return None

        except Exception as e:
            self.logger.log(f"Error querying variants: {e}", "WARNING")
            return None

        # --- Step 5: Get Variant-Gene Relationships ---
        try:
            variant_entity_ids = variant_df["variant_entity_id"].unique().tolist()

            # Direct and inverse relationships
            q1 = (
                select(
                    EntityRelationship.entity_1_id.label("variant_entity_id"),
                    EntityRelationship.entity_2_id.label("gene_entity_id"),
                    EntityRelationship.relationship_type_id,
                    EntityRelationship.data_source_id,
                )
                .where(
                    EntityRelationship.entity_1_id.in_(variant_entity_ids),
                    EntityRelationship.entity_2_group_id == gene_group_id,
                )
            )
            q2 = (
                select(
                    EntityRelationship.entity_2_id.label("variant_entity_id"),
                    EntityRelationship.entity_1_id.label("gene_entity_id"),
                    EntityRelationship.relationship_type_id,
                    EntityRelationship.data_source_id,
                )
                .where(
                    EntityRelationship.entity_2_id.in_(variant_entity_ids),
                    EntityRelationship.entity_1_group_id == gene_group_id,
                )
            )

            rel_df = pd.read_sql(union_all(q1, q2), self.session.bind)
            if rel_df.empty:
                self.logger.log("No genes found related to variants", "WARNING")
                return variant_df  # Return variant data even if no genes found

        except Exception as e:
            self.logger.log(f"Error querying relationships: {e}", "WARNING")
            return None

        # --- Step 6: Get Gene Metadata ---
        try:
            gene_entity_ids = rel_df["gene_entity_id"].unique().tolist()

            PrimaryAlias = aliased(EntityAlias)
            gene_query = (
                self.session.query(
                    Entity.id.label("gene_entity_id"),
                    PrimaryAlias.alias_value.label("gene_symbol"),
                )
                .join(PrimaryAlias, PrimaryAlias.entity_id == Entity.id)
                .filter(Entity.id.in_(gene_entity_ids))
                .filter(PrimaryAlias.is_primary.is_(True))
            )

            gene_df = pd.read_sql(gene_query.statement, self.session.bind)

        except Exception as e:
            self.logger.log(f"Error querying gene aliases: {e}", "WARNING")
            gene_df = pd.DataFrame()  # Not fatal

        # --- Step 7: Merge All ---
        try:
            # Merge rel with genes
            merged = rel_df.merge(gene_df, on="gene_entity_id", how="left")
            # Merge with variants
            final_df = variant_df.merge(merged, on="variant_entity_id", how="left")

            final_df["position_input"] = (
                final_df["chromosome"].astype(str) + ":" + final_df["start_pos"].astype(str)
            )

            column_order = [
                "position_input",
                "variant_id",
                "chromosome",
                "start_pos",
                "end_pos",
                "ref",
                "alt",
                "accession",
                "assembly_name",
                "gene_symbol",
                "gene_entity_id",
                "relationship_type_id",
                "data_source_id",
            ]

            rename_dict = {
                "position_input": "Input Position",
                "variant_id": "Variant ID",
                "chromosome": "Chr",
                "start_pos": "Start",
                "end_pos": "End",
                "ref": "Ref Allele",
                "alt": "Alt Allele",
                "accession": "Assembly Accession",
                "assembly_name": "Assembly",
                "gene_symbol": "Gene Symbol",
                "gene_entity_id": "Gene ID",
                "relationship_type_id": "Relationship Type",
                "data_source_id": "DataSource",
            }

            # Organize output
            final_df = final_df[column_order].rename(columns=rename_dict)
            return final_df

        except Exception as e:
            self.logger.log(f"Error formatting output: {e}", "WARNING")
            return None
