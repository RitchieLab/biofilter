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
    GenomeAssembly

)

from sqlalchemy.orm import aliased
from sqlalchemy import func
from sqlalchemy import union_all, select


class GeneToSNPReport(ReportBase):
    name = "gene_to_snp"
    description = "Given a list of genes, returns gene metadata and associated variants (SNPs) with positional and allelic info."

    @classmethod
    def explain(cls) -> str:
        return """\
🧬 GENE → SNP Report
====================

    This report takes as input a list of genes — accepted formats include:
    - HGNC symbols (e.g., `TP53`)
    - HGNC IDs (e.g., `HGNC:11998`)
    - Entrez IDs (e.g., `7157`)
    - Ensembl IDs (e.g., `ENSG00000141510`)
    - Any other symbols
    - Any other Names or Alias

    It returns:
    - ✅  Gene metadata (ID, symbol, alias type/source, conflict status)
    - 🧬  Associated SNPs (from dbSNP)
    - 📍  Genomic location (chr/start/end/accession)
    - 🧬  Alleles (ref/alt) and quality
    - ⚠️  Notes for duplicates or missing variants


🧪 EXAMPLE USAGE
================

    > result = bf.report.run_report(
        "report_gene_to_snp",
        assembly='38',
        input_data=["TXLNGY", "HGNC:18473", "246126", "ENSG00000131002", "HGNC:5"]
        )


    If you need run a Example Report:
        > result = bf.report.run_example_report("report_gene_to_snp")


    This returns a Pandas DataFrame with columns:
        > print(result)
        Index,Input Gene,HGNC Symbol,Matched Name,Alias Type,Alias Source,Gene ID,Variant ID,Variant Type,Chr,Start,End,Ref Allele,Alt Allele,Accession,Assembly,Quality,Note
        0,246126,TXLNGY,246126,code,ENTREZ,38610,rs3900,SNV,Y,19568371.0,19568371.0,["G"]",["C"]",NC_000024.10,GRCh38.p14,8.9,
        ...
        3233,TXLNGY,TXLNGY,TXLNGY,symbol,HGNC,38610,,,,,,,,,,,Duplicate entity_id: mapped to same gene as another input

    """

    @classmethod
    def example_input(cls) -> list[str]:
        """
        Returns a minimal working example of inputs for testing or tutorials.
        """
        return [
            "TXLNGY",
            "HGNC:18473",
            "246126",
            "ENSG00000131002",
            "HGNC:5"
        ]

    def run(self):

        # Read Input Genes List (Direct List or File)
        input_data_raw = self.params.get("input_data")
        input_data = self.resolve_input_list(input_data_raw)
        input_map = {x.lower(): x for x in input_data}
        input_list = list(input_map.keys())

        # Read Assembly to output positions
        assembly_input = self.params.get("assembly")
        chrom_to_assembly_id = self.resolve_assembly(assembly_input)
        assembly_ids = list(chrom_to_assembly_id.values())
        if not assembly_ids:
            self.logger.log("No Assembly found", "WARNING")
            return None

        # Get Entity Groups to Genes and Variants
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
            self.logger.log("EntityGroup 'Genes' or 'Variants' not found in the database.", "WARNING")
            return None
            
        # ---------------------------------------------------------------------
        # QUERY 1: Resolve input genes via EntityAlias (case-insensitive match)
        # Aliases
        try:
            PrimaryAlias = aliased(EntityAlias)
            gene_query = (
                self.session.query(
                    EntityAlias.alias_norm.label("entity_norm"),
                    EntityAlias.alias_value.label("entity_value"),
                    EntityAlias.alias_type,
                    EntityAlias.xref_source,
                    Entity.id.label("entity_id"),
                    PrimaryAlias.alias_value.label("symbol"),
                    Entity.group_id,
                    EntityGroup.name.label("group_name"),
                    Entity.has_conflict,
                    Entity.is_active,
                )
                .join(Entity, Entity.id == EntityAlias.entity_id)
                .join(PrimaryAlias, PrimaryAlias.entity_id == Entity.id)
                .join(EntityGroup, Entity.group_id == EntityGroup.id)
                .filter(Entity.group_id == gene_group_id)
                .filter(PrimaryAlias.is_primary.is_(True))
                .filter(EntityAlias.alias_norm.in_(input_list))
            )

            gene_df = pd.DataFrame(gene_query.all())
            if gene_df.empty:
                raise ValueError("No genes found.")
        
            # Return input information
            gene_df["input_gene"] = gene_df["entity_norm"].map(input_map)

            # Parse duplicated Entities
            # Agrupar por entity_id e manter apenas a primeira ocorrência
            unique_genes_df = gene_df.drop_duplicates(subset=["entity_id"], keep="first").copy()

            # Obter os demais como duplicatas
            duplicates_df = gene_df[~gene_df.index.isin(unique_genes_df.index)].copy()
            if not duplicates_df.empty:
                # Adicionar nota de duplicação (opcional)
                duplicates_df["note"] = "Duplicate entity_id: mapped to same gene as another input"

            gene_ids = unique_genes_df["entity_id"].unique().tolist()  # List of Genes
        
        except Exception as e:
            self.logger.log(f"Error on Entity Query to get Genes: {e}", "WARNING")
            return None

        # ---------------------------------------------------------------------
        # QUERY 2: Find variants linked to these genes via EntityRelationship
        try:
            # Subquery 1: gene -> variant
            q1 = (
                select(
                    EntityRelationship.entity_1_id.label("gene_entity_id"),
                    EntityRelationship.entity_2_id.label("variant_entity_id"),
                    EntityRelationship.relationship_type_id,
                    EntityRelationship.data_source_id,
                )
                .where(
                    EntityRelationship.entity_1_id.in_(gene_ids),
                    EntityRelationship.entity_2_group_id == variant_group_id,
                )
            )

            # Subquery 2: variant -> gene (Inverse)
            q2 = (
                select(
                    EntityRelationship.entity_2_id.label("gene_entity_id"),
                    EntityRelationship.entity_1_id.label("variant_entity_id"),
                    EntityRelationship.relationship_type_id,
                    EntityRelationship.data_source_id,
                )
                .where(
                    EntityRelationship.entity_2_id.in_(gene_ids),
                    EntityRelationship.entity_1_group_id == variant_group_id,
                )
            )

            # Union of queries
            rel_query = union_all(q1, q2)
            rel_df = pd.read_sql(rel_query, self.session.bind)
            if rel_df.empty:
                self.logger.log("No variants founds to inputs genes", "WARNING")
                return None

        except Exception as e:
            self.logger.log(f"Error on Entity Relationship to get Variants: {e}", "WARNING")
            return None

        # ---------------------------------------------------------------------
        # QUERY 3: Get variant metadata for the requested assembly

        try:
            # Get variant data linked to variant_entity_ids
            variant_entity_ids = rel_df["variant_entity_id"].unique().tolist()

            # Build query
            vm = aliased(VariantMaster)
            vl = aliased(VariantLocus)
            ga = aliased(GenomeAssembly)

            variant_query = (
                self.session.query(
                    vm.entity_id.label("variant_entity_id"),
                    vm.variant_id,
                    vl.chromosome,
                    ga.accession.label("accession"),
                    ga.assembly_name,
                    vm.quality,
                    vm.variant_type,
                    vl.start_pos,
                    vl.end_pos,
                    vl.reference_allele.label("ref"),
                    vl.alternate_allele.label("alt"),
                )
                .join(vl, vl.variant_id == vm.id)
                .join(ga, ga.id == vl.assembly_id)
                .filter(vm.entity_id.in_(variant_entity_ids))
                .filter(vl.assembly_id.in_(assembly_ids))  # Restringe aos assembly_ids válidos
            )

            variant_df = pd.read_sql(variant_query.statement, self.session.bind)
            if variant_df.empty:
                self.logger.log("No variant metadata found for selected assembly.", "WARNING")
                return None
            
        except Exception as e:
            self.logger.log(f"Error Variants Master Data: {e}", "WARNING")
            return None  

        # ---------------------------------------------------------------------
        # Prepare Output Report
        try:
            # 1 - Merge Variants to Genes
            results_df = variant_df.merge(rel_df, on="variant_entity_id")

            # 2 - Merge Result with Genes Attributes
            results_df = unique_genes_df.merge(
                results_df,
                left_on="entity_id",
                right_on="gene_entity_id",
                how="left"  # garante que todas as linhas de unique_genes_df sejam mantidas
            )
            # Após o merge, adicionar nota para genes sem variants
            results_df["note"] = None
            results_df.loc[results_df["variant_id"].isna(), "note"] = "No variants found in system"

            # 3 - Concat Duplicates Genes
            if not duplicates_df.empty:
                results_df = pd.concat([results_df, duplicates_df], ignore_index=True)

            results_df.drop(
                columns=[
                    "entity_norm",
                    "group_id", 
                    "group_name", 
                    "has_conflict", 
                    "is_active",
                    "variant_entity_id",
                    "gene_entity_id",
                    "relationship_type_id",
                    "data_source_id",
                ],
                inplace=True
            )

            # Org Resuls
            column_order = [
                "input_gene",     # Gene buscado
                "symbol",         # Nome oficial
                "entity_value",   # Alias correspondente
                "alias_type",     # Tipo do alias (ex: synonym)
                "xref_source",    # Fonte do alias

                "entity_id",      # ID interno do gene
                "variant_id",     # ID da variante
                "variant_type",   # Tipo da variante (SNP, etc)
                "chromosome",     # Cromossomo
                "start_pos",      # Posição inicial
                "end_pos",        # Posição final
                "ref",            # Alelo de referência
                "alt",            # Alelo alternativo

                "accession",      # Accession do assembly
                "assembly_name",  # Nome do assembly
                "quality",        # Qualidade (se houver)

                "note",           # Observação (ex: duplicated, not found)
            ]

            rename_dict = {
                "input_gene": "Input Gene",
                "symbol": "HGNC Symbol",
                "entity_value": "Matched Name",
                "alias_type": "Alias Type",
                "xref_source": "Alias Source",
                "entity_id": "Gene ID",
                "variant_id": "Variant ID",
                "variant_type": "Variant Type",
                "chromosome": "Chr",
                "start_pos": "Start",
                "end_pos": "End",
                "ref": "Ref Allele",
                "alt": "Alt Allele",
                "accession": "Accession",
                "assembly_name": "Assembly",
                "quality": "Quality",
                "note": "Note",
            }

            # 1. Reordenar colunas (somente as que existem no df)
            ordered_cols = [col for col in column_order if col in results_df.columns]
            results_df = results_df[ordered_cols]

            # 2. Renomear colunas
            results_df = results_df.rename(columns=rename_dict)

            return results_df

        except Exception as e:
            self.logger.log(f"Error to prepare results: {e}", "WARNING")
            return None  
