import ast
import pandas as pd
from biofilter.report.reports.base_report import ReportBase
from sqlalchemy.orm import aliased
from sqlalchemy import union_all, select
from sqlalchemy import or_, and_
from biofilter.db.models import (
    VariantMaster,
    VariantLocus,
    # Entity,
    EntityAlias,
    EntityGroup,
    EntityRelationship,
    # EntityRelationshipType,
    GenomeAssembly,
    GeneMaster,
    GeneLocusGroup,
    GeneLocusType,
    GeneGroupMembership,
    GeneGroup,
    ETLDataSource,
    ETLSourceSystem,
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
                    vm.quality,
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

        # --- Step 6: Merge variant ↔ gene relationships ---
        try:
            # Merge with variant data
            merged_df = pd.merge(variant_df, rel_df, on="variant_entity_id", how="left")

            # Step 7: Gene Metadata
            ea = aliased(EntityAlias)
            gm = aliased(GeneMaster)
            glg = aliased(GeneLocusGroup)
            glt = aliased(GeneLocusType)

            gene_ids = merged_df["gene_entity_id"].dropna().unique().tolist()
            gene_meta_query = (
                self.session.query(
                    ea.entity_id.label("gene_entity_id"),
                    ea.alias_value.label("symbol"),
                    gm.hgnc_status,
                    glg.name.label("locus_group"),
                    glt.name.label("locus_type"),
                )
                .join(gm, gm.entity_id == ea.entity_id)
                .join(glg, glg.id == gm.locus_group_id)
                .join(glt, glt.id == gm.locus_type_id)
                .filter(ea.is_primary.is_(True))
                .filter(ea.entity_id.in_(gene_ids))
            )
            gene_meta_df = pd.read_sql(gene_meta_query.statement, self.session.bind)

            # Step 8: Gene Groups (many-to-many)
            ggm = aliased(GeneGroupMembership)
            gg = aliased(GeneGroup)
            group_query = (
                self.session.query(
                    ggm.gene_id,
                    gg.name.label("gene_group")
                )
                .join(gg, gg.id == ggm.group_id)
                .filter(ggm.gene_id.in_(gene_ids))
            )
            group_df = pd.read_sql(group_query.statement, self.session.bind)
            group_map = group_df.groupby("gene_id")["gene_group"].apply(list).to_dict()

            # Step 9: Data source info
            ds = aliased(ETLDataSource)
            ss = aliased(ETLSourceSystem)
            ds_query = (
                self.session.query(
                    ds.id.label("data_source_id"),
                    ds.name.label("data_source"),
                    ss.name.label("source_system")
                )
                .join(ss, ss.id == ds.source_system_id)
                .filter(ds.id.in_(merged_df["data_source_id"].dropna().unique().tolist()))
            )
            ds_df = pd.read_sql(ds_query.statement, self.session.bind)

            # Step 10: Final merge
            merged_df = pd.merge(merged_df, gene_meta_df, on="gene_entity_id", how="left")
            merged_df["gene_groups"] = merged_df["gene_entity_id"].map(group_map)
            merged_df = pd.merge(merged_df, ds_df, on="data_source_id", how="left")

            # Step 11: Cleanup and output
            merged_df["input_key"] = merged_df["chromosome"] + ":" + merged_df["start_pos"].astype(int).astype(str)

            rename_dict = {
                "input_key": "Input Position",
                "variant_id": "Variant ID",
                "chromosome": "Chr",
                "start_pos": "Start",
                "end_pos": "End",
                "ref": "Ref Allele",
                "alt": "Alt Allele",
                "quality": "Variant Score",
                "accession": "Assembly Accession",
                "assembly_name": "Assembly",
                "symbol": "Gene Symbol",
                "gene_entity_id": "Gene ID",
                "hgnc_status": "HGNC Status",
                "locus_group": "Locus Group",
                "locus_type": "Locus Type",
                "gene_groups": "Gene Groups",
                "data_source": "DataSource",
                "source_system": "SourceSystem",
            }

            column_order = list(rename_dict.values())
            merged_df = merged_df.rename(columns=rename_dict)
            merged_df = merged_df[column_order]

            # Apply better formating
            merged_df["Ref Allele"] = merged_df["Ref Allele"].apply(self.parse_and_join)
            merged_df["Alt Allele"] = merged_df["Alt Allele"].apply(self.parse_and_join)

            merged_df["Gene Groups"] = merged_df["Gene Groups"].apply(
                lambda x: "; ".join(x) if isinstance(x, list) else None
            )
            # TODO: Discusion point
            merged_df["HGNC Status"] = merged_df["HGNC Status"].replace({   
                "Gene from NCBI": "Provisional",
                "unknown": None,
            })


            # --- Step 7: Append missing inputs as rows with note ---
            if "Note" not in merged_df.columns:
                merged_df["Note"] = None

            input_positions_set = set([f"{chrom}:{pos}" for chrom, pos in position_list])
            output_positions_set = set(merged_df["Input Position"].dropna().unique())

            # Posições que não foram mapeadas
            missing_positions = input_positions_set - output_positions_set

            # Criar DataFrame com apenas essas posições e nota
            if missing_positions:
                missing_df = pd.DataFrame({
                    "Input Position": list(missing_positions),
                    "Note": ["No variant or gene found for this position"] * len(missing_positions)
                })

                # if not missing_df.empty:
                #     # Opcional: Remover colunas 100% NA
                #     missing_df = missing_df.dropna(axis=1, how='all')

                # Garante que todas as colunas existam (mesmo que vazias)
                for col in merged_df.columns:
                    if col not in missing_df.columns:
                        missing_df[col] = None

                # Alinha as colunas
                # missing_df = missing_df[merged_df.columns]
                missing_df = missing_df.reindex(columns=merged_df.columns)
                for col in merged_df.columns:
                    if col not in missing_df.columns:
                        missing_df[col] = pd.NA
                    else:
                        try:
                            missing_df[col] = missing_df[col].astype(merged_df[col].dtype)
                        except Exception:
                            pass  # Ignora se não for possível converter

                # Concatena com os resultados anteriores
                merged_df = pd.concat([merged_df, missing_df], ignore_index=True)


            # Move 'Note' to the end
            cols = [c for c in merged_df.columns if c != "Note"] + ["Note"]
            merged_df = merged_df[cols]



            # Print informations after process
            print(f"  ✅ Report successfully generated with {len(merged_df)} rows and {merged_df['Gene ID'].nunique()} unique genes.")
            print("  ℹ️ Note:")
            print("  - Variants without associated genes are annotate in the report.")
            print("  - Some gene metadata is based on external sources (HGNC, NCBI).")

            return merged_df

        except Exception as e:
            self.logger.log(f"Error building final report: {e}", "WARNING")
            return None



