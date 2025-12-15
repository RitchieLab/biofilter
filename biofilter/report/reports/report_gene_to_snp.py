# biofilter/report/reports/report_gene_to_snp.py

import pandas as pd
from sqlalchemy.orm import aliased
from sqlalchemy import and_, or_, select

from biofilter.report.reports.base_report import ReportBase
from biofilter.db.models import (
    Entity,
    EntityAlias,
    EntityGroup,
    EntityLocation,
    GeneMaster,
    VariantSNP,
    ETLDataSource,
    ETLSourceSystem,
    # VariantSNPMerge,   # optional
)

class GeneToSNPReport(ReportBase):
    name = "gene_to_snp"
    description = (
        "Given a list of genes, returns gene metadata and SNPs overlapping "
        "the gene genomic region (GRCh37/38) using VariantSNP."
    )

    @classmethod
    def explain(cls) -> str:
        return """\
🧬 GENE → SNP Report (v3.2.0)
============================

This report takes a list of gene identifiers (symbols, HGNC IDs, Entrez IDs,
Ensembl IDs, synonyms) and returns SNPs that overlap each gene region.

Key change in v3.2.0:
- Variants are no longer modeled as Entities.
- Gene→SNP is resolved by genomic overlap:
  EntityLocation (gene region) ↔ VariantSNP (position_37/position_38)

Parameters:
- assembly: "37" or "38" (default: "38")
- input_data: list[str] or path to .txt file

Output includes:
- Gene match info (input, symbol, matched alias)
- Gene coordinates (chr/start/end/build)
- SNP info (rs_id, position, ref/alt)
- Provenance (data_source, source_system)
"""

    @classmethod
    def example_input(cls) -> list[str]:
        return ["TXLNGY", "HGNC:18473", "246126", "ENSG00000131002", "HGNC:5"]

    def run(self):
        # -----------------------------
        # Params
        # -----------------------------
        input_data_raw = self.params.get("input_data")
        input_data = self.resolve_input_list(input_data_raw)
        input_map = {x.lower(): x for x in input_data}
        input_list = list(input_map.keys())

        assembly = str(self.params.get("assembly") or "38").strip()
        if assembly not in ("37", "38"):
            assembly = "38"

        # Build-aware column selection
        pos_col = VariantSNP.position_38 if assembly == "38" else VariantSNP.position_37
        build_int = int(assembly)

        # -----------------------------
        # Resolve Gene group id
        # -----------------------------
        gene_group_id = (
            self.session.query(EntityGroup.id)
            .filter(EntityGroup.name.ilike("Genes"))
            .scalar()
        )
        if not gene_group_id:
            self.logger.log("EntityGroup 'Genes' not found in the database.", "ERROR")
            return pd.DataFrame()

        # -----------------------------
        # QUERY 1: Resolve input genes via EntityAlias (case-insensitive)
        # -----------------------------
        PrimaryAlias = aliased(EntityAlias)

        gene_query = (
            self.session.query(
                EntityAlias.alias_norm.label("entity_norm"),
                EntityAlias.alias_value.label("entity_value"),
                EntityAlias.alias_type,
                EntityAlias.xref_source,
                Entity.id.label("entity_id"),
                PrimaryAlias.alias_value.label("symbol"),
                Entity.has_conflict,
                Entity.is_active,
            )
            .join(Entity, Entity.id == EntityAlias.entity_id)
            .join(PrimaryAlias, PrimaryAlias.entity_id == Entity.id)
            .filter(Entity.group_id == gene_group_id)
            .filter(or_(PrimaryAlias.is_primary.is_(True), PrimaryAlias.alias_type == "preferred"))
            .filter(EntityAlias.alias_norm.in_(input_list))
        )

        gene_df = pd.DataFrame(gene_query.all())
        if gene_df.empty:
            self.logger.log("No genes matched the input list.", "WARNING")
            return pd.DataFrame()

        gene_df["input_gene"] = gene_df["entity_norm"].map(input_map)

        # Deduplicate entity_id (keep first)
        unique_genes_df = gene_df.drop_duplicates(subset=["entity_id"], keep="first").copy()
        duplicates_df = gene_df[~gene_df.index.isin(unique_genes_df.index)].copy()
        if not duplicates_df.empty:
            duplicates_df["note"] = "Duplicate entity_id: mapped to same gene as another input"

        gene_entity_ids = unique_genes_df["entity_id"].unique().tolist()

        # -----------------------------
        # QUERY 2: Get gene locations for selected build
        # -----------------------------
        loc_stmt = (
            self.session.query(
                EntityLocation.entity_id.label("gene_entity_id"),
                EntityLocation.build,
                EntityLocation.chromosome,
                EntityLocation.start_pos,
                EntityLocation.end_pos,
                EntityLocation.strand,
                EntityLocation.region_label,
            )
            .filter(EntityLocation.entity_id.in_(gene_entity_ids))
            .filter(EntityLocation.build == build_int)
        )

        loc_df = pd.DataFrame(loc_stmt.all())
        if loc_df.empty:
            self.logger.log(f"No gene locations found for build={build_int}.", "WARNING")
            # return genes only (no SNPs)
            out = unique_genes_df.copy()
            out["note"] = out.get("note", None)
            out.rename(columns={"entity_id": "Gene ID"}, inplace=True)
            return out

        # -----------------------------
        # QUERY 3: Find SNPs that overlap gene regions
        #   chr matches AND position BETWEEN start/end
        # -----------------------------
        # Join VariantSNP to locations by range condition
        snp_stmt = (
            self.session.query(
                EntityLocation.entity_id.label("gene_entity_id"),
                VariantSNP.rs_id,
                VariantSNP.chromosome.label("snp_chr"),
                pos_col.label("snp_pos"),
                VariantSNP.reference_allele.label("ref"),
                VariantSNP.alternate_allele.label("alt"),
                ETLDataSource.name.label("data_source"),
                ETLSourceSystem.name.label("source_system"),
            )
            .select_from(EntityLocation)
            .join(
                VariantSNP,
                and_(
                    VariantSNP.chromosome == EntityLocation.chromosome,
                    pos_col.isnot(None),
                    pos_col >= EntityLocation.start_pos,
                    pos_col <= EntityLocation.end_pos,
                ),
            )
            .outerjoin(ETLDataSource, ETLDataSource.id == VariantSNP.data_source_id)
            .outerjoin(ETLSourceSystem, ETLSourceSystem.id == ETLDataSource.source_system_id)
            .filter(EntityLocation.entity_id.in_(gene_entity_ids))
            .filter(EntityLocation.build == build_int)
        )

        snp_df = pd.DataFrame(snp_stmt.all())
        # snp_df can be empty (valid case)

        # -----------------------------
        # Build final output
        # -----------------------------
        # Merge genes + locations
        out_df = unique_genes_df.merge(
            loc_df,
            left_on="entity_id",
            right_on="gene_entity_id",
            how="left",
        )

        # Merge SNPs
        if not snp_df.empty:
            out_df = out_df.merge(
                snp_df,
                on="gene_entity_id",
                how="left",
            )
        else:
            out_df["rs_id"] = None
            out_df["snp_chr"] = None
            out_df["snp_pos"] = None
            out_df["ref"] = None
            out_df["alt"] = None
            out_df["data_source"] = None
            out_df["source_system"] = None

        # Notes for genes with no SNPs
        out_df["note"] = out_df.get("note", None)
        out_df.loc[out_df["rs_id"].isna(), "note"] = out_df.loc[out_df["rs_id"].isna(), "note"].fillna(
            "No SNPs found overlapping the gene region"
        )

        # Append duplicates (optional, like old report)
        if not duplicates_df.empty:
            out_df = pd.concat([out_df, duplicates_df], ignore_index=True)

        # Rename / order columns (keep it user-friendly)
        rename = {
            "input_gene": "Input Gene",
            "symbol": "HGNC Symbol",
            "entity_value": "Matched Name",
            "alias_type": "Alias Type",
            "xref_source": "Alias Source",
            "entity_id": "Gene Entity ID",
            "build": "Build",
            "chromosome": "Gene Chr",
            "start_pos": "Gene Start",
            "end_pos": "Gene End",
            "strand": "Gene Strand",
            "region_label": "Region Label",
            "rs_id": "rsID",
            "snp_chr": "SNP Chr",
            "snp_pos": "SNP Pos",
            "ref": "Ref Allele",
            "alt": "Alt Allele",
            "data_source": "DataSource",
            "source_system": "SourceSystem",
            "note": "Note",
        }

        out_df = out_df.rename(columns=rename)

        ordered_cols = [
            "Input Gene",
            "HGNC Symbol",
            "Matched Name",
            "Alias Type",
            "Alias Source",
            "Gene Entity ID",
            "Build",
            "Gene Chr",
            "Gene Start",
            "Gene End",
            "Gene Strand",
            "Region Label",
            "rsID",
            "SNP Chr",
            "SNP Pos",
            "Ref Allele",
            "Alt Allele",
            "DataSource",
            "SourceSystem",
            "Note",
        ]
        ordered_cols = [c for c in ordered_cols if c in out_df.columns]
        out_df = out_df[ordered_cols]

        return out_df


"""
para continuidade
Quer manter compatibilidade com “Variant merges”?

Se você quer que rs_id seja “canonicalizado”, dá para adicionar depois um passo:

join em variant_snp_merges para trocar rs_obsolete_id → rs_canonical_id

Eu não incluí por padrão porque você pode querer ver o rs original do dump.

Se você me disser:

como você define “Gene region” (gene body? promoter? +/- window?),
eu já adapto o report para suportar window_bp=50000 (ex: 50kb upstream/downstream) sem mudar mais nada.



rodar:
ANALYZE variant_snps;
ANALYZE entity_locations;
"""