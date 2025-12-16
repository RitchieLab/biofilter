# biofilter/report/reports/report_gene_to_snp.py

import pandas as pd
from sqlalchemy.orm import aliased
from sqlalchemy import and_, or_

from biofilter.report.reports.base_report import ReportBase
from biofilter.db.models import (
    Entity,
    EntityAlias,
    EntityGroup,
    EntityLocation,
    VariantSNP,
    ETLDataSource,
    ETLSourceSystem,
)


class GeneToSNPReport(ReportBase):
    name = "gene_to_snp"
    description = (
        "Given a list of genes, returns gene metadata and SNPs overlapping "
        "the gene genomic region (EntityLocation build38) using VariantSNP."
    )

    # -----------------------------
    # Helpers / Schema contract
    # -----------------------------
    @classmethod
    def available_columns(cls) -> list[str]:
        """
        Internal column keys that can be requested via output_columns=[...].
        (These are stable keys; display names can be changed later.)
        """
        return [
            # input / match
            "input_gene",
            "symbol",
            "entity_value",
            "alias_type",
            "xref_source",
            "entity_id",
            "note",

            # gene (build 38 only)
            "gene_build",
            "gene_chr",
            "gene_start_38",
            "gene_end_38",
            "gene_strand",
            "region_label",

            # snp
            "rs_id",
            "snp_chr",
            "snp_pos_38",
            "snp_pos_37",
            "ref",
            "alt",

            # provenance
            "data_source",
            "source_system",
        ]

    @classmethod
    def explain(cls) -> str:
        return """\
🧬 GENE → SNP Report (v3.2.0)
============================

This report takes gene identifiers (symbols, HGNC IDs, Entrez IDs, Ensembl IDs, synonyms)
and returns SNPs overlapping each gene region.

Business rules (current schema):
- Gene regions come from EntityLocation and are currently available only for build 38.
- SNP lookup uses VariantSNP and overlaps are computed using build 38 coordinates.
- VariantSNP contains both position_38 and position_37; the report can display both.

Parameters:
- input_data: list[str] or path to .txt file (required)
- window_bp: int >= 0 (default: 1000). Extends the gene region +/- window_bp in build 38.
- assembly: "37" or "38" (optional). Controls which SNP position column(s) are returned:
    - None (default): return both snp_pos_38 and snp_pos_37
    - "38": return snp_pos_38 only
    - "37": return snp_pos_37 only
- output_columns: optional list[str]. Restrict output columns using available_columns() keys.

Usage:
    df = bf.report.run(
        "gene_to_snp",
        input_data=["TP53", "HGNC:11998"],
        window_bp=5000,
        assembly=None,
        output_columns=["input_gene","symbol","gene_chr","gene_start_38","gene_end_38","rs_id","snp_pos_38"]
    )
"""

    @classmethod
    def example_input(cls) -> list[str]:
        return ["TXLNGY", "HGNC:18473", "246126", "ENSG00000131002", "HGNC:5"]

    # -----------------------------
    # Core
    # -----------------------------
    def run(self):
        # -----------------------------
        # Params
        # -----------------------------
        input_data_raw = self.params.get("input_data")
        input_data = self.resolve_input_list(input_data_raw)
        if not input_data:
            self.logger.log("No input_data provided.", "ERROR")
            return pd.DataFrame()

        # preserve input order for tie-breaks
        input_order = {x.lower(): i for i, x in enumerate(input_data)}
        input_map = {x.lower(): x for x in input_data}
        input_list = list(input_map.keys())

        # window_bp validation
        window_bp = self.params.get("window_bp", 1000)
        try:
            window_bp = int(window_bp)
            if window_bp < 0:
                raise ValueError()
        except Exception:
            self.logger.log("window_bp must be an int >= 0. Using default=1000.", "WARNING")
            window_bp = 1000

        # assembly output selection (controls only what we *show*, not how we *search*)
        assembly = self.params.get("assembly")
        if assembly is not None:
            assembly = str(assembly).strip()
            if assembly not in ("37", "38"):
                self.logger.log("assembly must be '37', '38', or None. Using None.", "WARNING")
                assembly = None

        # output columns filter
        output_columns = self.params.get("output_columns")
        if output_columns is not None:
            if isinstance(output_columns, str):
                output_columns = [output_columns]
            output_columns = [c.strip() for c in output_columns if c and str(c).strip()]
            allowed = set(self.available_columns())
            unknown = [c for c in output_columns if c not in allowed]
            if unknown:
                self.logger.log(
                    f"Unknown output_columns: {unknown}. Allowed: {sorted(allowed)}",
                    "ERROR",
                )
                return pd.DataFrame()

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

                # scoring fields to select "best" input per entity_id
                PrimaryAlias.is_primary.label("primary_is_primary"),
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
        gene_df["input_rank"] = gene_df["entity_norm"].map(input_order)

        # Priority: primary/preferred first, then first input order
        gene_df["priority_rank"] = gene_df["primary_is_primary"].fillna(False).map({True: 0, False: 1})
        gene_df = gene_df.sort_values(["entity_id", "priority_rank", "input_rank"], ascending=[True, True, True])

        unique_genes_df = gene_df.drop_duplicates(subset=["entity_id"], keep="first").copy()

        duplicates_df = gene_df[~gene_df.index.isin(unique_genes_df.index)].copy()
        if not duplicates_df.empty:
            duplicates_df["note"] = "Duplicate entity_id: mapped to same gene as another input"

        gene_entity_ids = unique_genes_df["entity_id"].unique().tolist()

        # -----------------------------
        # QUERY 2: Gene locations (build 38 only)
        # -----------------------------
        # NOTE: EntityLocations are currently only build 38 by rule.
        loc_stmt = (
            self.session.query(
                EntityLocation.entity_id.label("gene_entity_id"),
                EntityLocation.chromosome.label("gene_chr"),
                EntityLocation.start_pos.label("gene_start_38"),
                EntityLocation.end_pos.label("gene_end_38"),
                EntityLocation.strand.label("gene_strand"),
                EntityLocation.region_label,
            )
            .filter(EntityLocation.entity_id.in_(gene_entity_ids))
            .filter(EntityLocation.build == 38)
        )

        loc_df = pd.DataFrame(loc_stmt.all())
        if loc_df.empty:
            self.logger.log("No gene locations found for build=38.", "WARNING")
            out = unique_genes_df.copy()
            out["note"] = out.get("note", None)
            return self._finalize_output(out, output_columns=output_columns, assembly=assembly)

        # Extend gene region by window_bp (clamp at >=1 if you want; keeping >=0 for now)
        loc_df["gene_start_38_w"] = (loc_df["gene_start_38"] - window_bp).clip(lower=0)
        loc_df["gene_end_38_w"] = loc_df["gene_end_38"] + window_bp

        # -----------------------------
        # QUERY 3: SNP overlap by build 38 coordinates (always)
        # -----------------------------
        # Overlap uses VariantSNP.position_38, regardless of what we output
        snp_stmt = (
            self.session.query(
                EntityLocation.entity_id.label("gene_entity_id"),
                VariantSNP.rs_id.label("rs_id"),
                VariantSNP.chromosome.label("snp_chr"),
                VariantSNP.position_38.label("snp_pos_38"),
                VariantSNP.position_37.label("snp_pos_37"),
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
                    VariantSNP.position_38.isnot(None),
                    VariantSNP.position_38 >= EntityLocation.start_pos - window_bp,
                    VariantSNP.position_38 <= EntityLocation.end_pos + window_bp,
                ),
            )
            .outerjoin(ETLDataSource, ETLDataSource.id == VariantSNP.data_source_id)
            .outerjoin(ETLSourceSystem, ETLSourceSystem.id == ETLDataSource.source_system_id)
            .filter(EntityLocation.entity_id.in_(gene_entity_ids))
            .filter(EntityLocation.build == 38)
        )

        snp_df = pd.DataFrame(snp_stmt.all())

        # -----------------------------
        # Build final output
        # -----------------------------
        out_df = unique_genes_df.merge(
            loc_df.drop(columns=["gene_start_38_w", "gene_end_38_w"], errors="ignore"),
            left_on="entity_id",
            right_on="gene_entity_id",
            how="left",
        )

        out_df["gene_build"] = 38

        if not snp_df.empty:
            out_df = out_df.merge(snp_df, on="gene_entity_id", how="left")
        else:
            # create empty SNP columns
            for c in ["rs_id","snp_chr","snp_pos_38","snp_pos_37","ref","alt","data_source","source_system"]:
                out_df[c] = None

        out_df["note"] = out_df.get("note", None)
        out_df.loc[out_df["rs_id"].isna(), "note"] = out_df.loc[out_df["rs_id"].isna(), "note"].fillna(
            "No SNPs found overlapping the gene region"
        )

        if not duplicates_df.empty:
            out_df = pd.concat([out_df, duplicates_df], ignore_index=True)

        # Hide SNP position columns depending on assembly parameter (output-only)
        if assembly == "38":
            out_df.drop(columns=["snp_pos_37"], inplace=True, errors="ignore")
        elif assembly == "37":
            out_df.drop(columns=["snp_pos_38"], inplace=True, errors="ignore")

        return self._finalize_output(out_df, output_columns=output_columns, assembly=assembly)

    # -----------------------------
    # Output formatting
    # -----------------------------
    def _finalize_output(self, df: pd.DataFrame, output_columns=None, assembly=None) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        # canonical rename map (display names)
        rename = {
            "input_gene": "Input Gene",
            "symbol": "HGNC Symbol",
            "entity_value": "Matched Name",
            "alias_type": "Alias Type",
            "xref_source": "Alias Source",
            "entity_id": "Gene Entity ID",
            "gene_build": "Gene Build",
            "gene_chr": "Gene Chr",
            "gene_start_38": "Gene Start (Build 38)",
            "gene_end_38": "Gene End (Build 38)",
            "gene_strand": "Gene Strand",
            "region_label": "Region Label",
            "rs_id": "rsID",
            "snp_chr": "SNP Chr",
            "snp_pos_38": "SNP Pos (Build 38)",
            "snp_pos_37": "SNP Pos (Build 37)",
            "ref": "Ref Allele",
            "alt": "Alt Allele",
            "data_source": "DataSource",
            "source_system": "SourceSystem",
            "note": "Note",
        }

        # filter columns if requested (using internal keys)
        if output_columns is not None:
            # apply before rename
            keep = []
            for c in output_columns:
                # if assembly was used, a pos column may not exist anymore; skip silently
                if c in df.columns:
                    keep.append(c)
            # always keep note if it exists (helpful for duplicates/no SNP)
            if "note" in df.columns and "note" not in keep:
                keep.append("note")
            df = df[keep]

        df = df.rename(columns=rename)

        # default ordering (display names)
        ordered_cols = [
            "Input Gene",
            "HGNC Symbol",
            "Matched Name",
            "Alias Type",
            "Alias Source",
            "Gene Entity ID",
            "Gene Build",
            "Gene Chr",
            "Gene Start (Build 38)",
            "Gene End (Build 38)",
            "Gene Strand",
            "Region Label",
            "rsID",
            "SNP Chr",
            "SNP Pos (Build 38)",
            "SNP Pos (Build 37)",
            "Ref Allele",
            "Alt Allele",
            "DataSource",
            "SourceSystem",
            "Note",
        ]
        ordered_cols = [c for c in ordered_cols if c in df.columns]
        return df[ordered_cols]
