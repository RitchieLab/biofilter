# biofilter/report/reports/report_gene_to_snp.py

from __future__ import annotations

import pandas as pd
from sqlalchemy.orm import aliased
from sqlalchemy import and_, or_, select

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

    # Display names (what the user sees). We use these same names for output_columns.
    columns = [
        "Input Gene",
        "Matched Name",
        "Alias Type",
        "Alias Source",
        "HGNC Symbol",
        "Gene Entity ID",
        "Gene Build",
        "Gene Chr (23:X/24:Y)",
        "Gene Start (Build 38)",
        "Gene End (Build 38)",
        "Gene Strand",
        "Region Label",
        "Variant Type",
        "Variant ID",
        "SNP Chr (23:X/24:Y)",
        "SNP Pos (Build 38)",
        "SNP Pos (Build 37)",
        "Ref Allele",
        "Alt Allele",
        "DataSource",
        "SourceSystem",
        "Note",
    ]

    @classmethod
    def available_columns(cls) -> list[str]:
        """
        Column names available for output_columns (these are DISPLAY NAMES).
        Users should filter using the same names they see in the output.
        """
        return cls.columns

    @classmethod
    def explain(cls) -> str:
        return """\
🧬 GENE → SNP Report (v3.2.0)
============================

This report takes gene identifiers (symbols, HGNC IDs, Entrez IDs, Ensembl IDs, synonyms)
and returns SNPs overlapping each gene region.

Rules:
- Gene regions come from EntityLocation and are currently available only for build 38.
- SNP lookup uses VariantSNP and overlaps are computed using build 38 coordinates (position_38).
- VariantSNP contains both position_38 and position_37; the report can display both.

Parameters:
- input_data: list[str] or path to .txt file (required)
- window_bp: int >= 0 (default: 1000). Extends the gene region +/- window_bp (build 38).
- assembly: "37" | "38" | None (default None).
    NOTE: assembly is OUTPUT-ONLY:
      - None: show both 'SNP Pos (Build 38)' and 'SNP Pos (Build 37)'
      - "38": show only 'SNP Pos (Build 38)'
      - "37": show only 'SNP Pos (Build 37)'
- output_columns: optional list[str] of DISPLAY NAMES (see available_columns()).

Usage:
    df = bf.report.run(
        "gene_to_snp",
        input_data=["TP53", "HGNC:11998"],
        window_bp=5000,
        assembly=None,
        output_columns=[
            "Input Gene",
            "HGNC Symbol",
            "Gene Chr (23:X/24:Y)",
            "Gene Start (Build 38)",
            "Gene End (Build 38)",
            "Variant Type",
            "Variant ID",
            "SNP Pos (Build 38)",
            "SNP Pos (Build 37)",
            "Note",
        ],
    )
"""

    @classmethod
    def example_input(cls) -> list[str]:
        return ["TXLNGY", "HGNC:18473", "246126", "ENSG00000131002", "HGNC:5"]

    # -----------------------------
    # Core
    # -----------------------------
    def run(self) -> pd.DataFrame:
        # -----------------------------
        # Params
        # -----------------------------
        input_data_raw = self.params.get("input_data")
        input_data = self.resolve_input_list(input_data_raw)
        if not input_data:
            self.logger.log("No input_data provided.", "ERROR")
            return pd.DataFrame()

        # Preserve input order for tie-breaks
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

        # assembly output selection (OUTPUT-ONLY)
        assembly = self.params.get("assembly")
        if assembly is not None:
            assembly = str(assembly).strip()
            if assembly not in ("37", "38"):
                self.logger.log("assembly must be '37', '38', or None. Using None.", "WARNING")
                assembly = None

        # output columns filter (DISPLAY NAMES)
        output_columns = self.params.get("output_columns")
        if output_columns is not None:
            if isinstance(output_columns, str):
                output_columns = [output_columns]
            output_columns = [str(c).strip() for c in output_columns if c and str(c).strip()]
            allowed = set(self.available_columns())
            unknown = [c for c in output_columns if c not in allowed]
            if unknown:
                self.logger.log(
                    f"Unknown output_columns (must match display names): {unknown}. "
                    f"Allowed: {sorted(allowed)}",
                    "ERROR",
                )
                return pd.DataFrame()

        # -----------------------------
        # Resolve Gene group id
        # -----------------------------
                # Avoid idle in transaction
        bind = self.session.get_bind()
        with bind.connect() as conn:
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

        # Avoid idle in transaction (execute outside Session transaction)
        bind = self.session.get_bind()
        with bind.connect() as conn:
            gene_df = pd.read_sql(gene_query.statement, conn)
        # gene_df = pd.DataFrame(gene_query.all())

        if gene_df.empty:
            self.logger.log("No genes matched the input list.", "WARNING")
            return pd.DataFrame()

        gene_df["input_gene"] = gene_df["entity_norm"].map(input_map)
        gene_df["input_rank"] = gene_df["entity_norm"].map(input_order)

        # Priority: primary/preferred first, then first input order
        gene_df["priority_rank"] = (
            gene_df["primary_is_primary"]
            .fillna(False)
            .map({True: 0, False: 1})
        )
        gene_df = gene_df.sort_values(
            ["entity_id", "priority_rank", "input_rank"],
            ascending=[True, True, True],
        )

        unique_genes_df = gene_df.drop_duplicates(subset=["entity_id"], keep="first").copy()

        duplicates_df = gene_df[~gene_df.index.isin(unique_genes_df.index)].copy()
        if not duplicates_df.empty:
            duplicates_df["note"] = "Duplicate entity_id: mapped to same gene as another input"

        gene_entity_ids = unique_genes_df["entity_id"].unique().tolist()

        # -----------------------------
        # QUERY 2: Gene locations (build 38 only)
        # -----------------------------
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

        # Avoid idle in transaction
        bind = self.session.get_bind()
        with bind.connect() as conn:
            loc_df = pd.read_sql(loc_stmt.statement, conn)
        # loc_df = pd.DataFrame(loc_stmt.all())

        if loc_df.empty:
            self.logger.log("No gene locations found for build=38.", "WARNING")
            out = unique_genes_df.copy()
            out["note"] = out.get("note", None)
            return self._finalize_output(
                out,
                assembly=assembly,
                output_columns=output_columns,
            )

        # -----------------------------
        # QUERY 3: SNP overlap by build 38 coordinates (always)
        # -----------------------------
        # snp_stmt = (
        #     self.session.query(
        #         EntityLocation.entity_id.label("gene_entity_id"),
        #         VariantSNP.source_type.label("source_type"),
        #         VariantSNP.source_id.label("source_id"),
        #         VariantSNP.chromosome.label("snp_chr"),
        #         VariantSNP.position_38.label("snp_pos_38"),
        #         VariantSNP.position_37.label("snp_pos_37"),
        #         VariantSNP.reference_allele.label("ref"),
        #         VariantSNP.alternate_allele.label("alt"),
        #         ETLDataSource.name.label("data_source"),
        #         ETLSourceSystem.name.label("source_system"),
        #     )
        #     .select_from(EntityLocation)
        #     .join(
        #         VariantSNP,
        #         and_(
        #             # 🔒 Garantias mínimas de sanidade
        #             EntityLocation.start_pos.isnot(None),
        #             EntityLocation.end_pos.isnot(None),

        #             # 🔑 Join por cromossomo (partition key)
        #             VariantSNP.chromosome == EntityLocation.chromosome,

        #             # 🔍 Overlap sempre usando build 38
        #             VariantSNP.position_38.isnot(None),
        #             VariantSNP.position_38 >= (EntityLocation.start_pos - window_bp),
        #             VariantSNP.position_38 <= (EntityLocation.end_pos + window_bp),
        #         ),
        #     )
        #     .outerjoin(ETLDataSource, ETLDataSource.id == VariantSNP.data_source_id)
        #     .outerjoin(ETLSourceSystem, ETLSourceSystem.id == ETLDataSource.source_system_id)
        #     .filter(EntityLocation.entity_id.in_(gene_entity_ids))
        #     .filter(EntityLocation.build == 38)
        # )

        # # Avoid idle in transaction
        # bind = self.session.get_bind()
        # with bind.connect() as conn:
        #     snp_df = pd.read_sql(snp_stmt.statement, conn)
        # # snp_df = pd.DataFrame(snp_stmt.all())
        # # snp_df can be empty (valid case)
        loc_w = (
            select(
                EntityLocation.entity_id.label("gene_entity_id"),
                EntityLocation.chromosome.label("gene_chr"),
                (EntityLocation.start_pos - window_bp).label("w_start"),
                (EntityLocation.end_pos + window_bp).label("w_end"),
            )
            .where(
                EntityLocation.entity_id.in_(gene_entity_ids),
                EntityLocation.build == 38,
                EntityLocation.start_pos.isnot(None),
                EntityLocation.end_pos.isnot(None),
            )
        ).subquery()

        snp_stmt = (
            self.session.query(
                loc_w.c.gene_entity_id,
                VariantSNP.source_type,
                VariantSNP.source_id,
                VariantSNP.chromosome.label("snp_chr"),
                VariantSNP.position_38.label("snp_pos_38"),
                VariantSNP.position_37.label("snp_pos_37"),
                VariantSNP.reference_allele.label("ref"),
                VariantSNP.alternate_allele.label("alt"),
            )
            .select_from(loc_w)
            .join(
                VariantSNP,
                and_(
                    VariantSNP.chromosome == loc_w.c.gene_chr,
                    VariantSNP.position_38.isnot(None),
                    VariantSNP.position_38 >= loc_w.c.w_start,
                    VariantSNP.position_38 <= loc_w.c.w_end,
                ),
            )
        )
        # Avoid idle in transaction
        bind = self.session.get_bind()
        with bind.connect() as conn:
            snp_df = pd.read_sql(snp_stmt.statement, conn)
        # snp_df = pd.DataFrame(snp_stmt.all())
        # snp_df can be empty (valid case)

        # -----------------------------
        # Build final output (internal cols)
        # -----------------------------
        out_df = unique_genes_df.merge(
            loc_df,
            left_on="entity_id",
            right_on="gene_entity_id",
            how="left",
        )
        out_df["gene_build"] = 38

        if not snp_df.empty:
            out_df = out_df.merge(snp_df, on="gene_entity_id", how="left")
        else:
            for c in [
                "source_type",
                "source_id",
                "snp_chr",
                "snp_pos_38",
                "snp_pos_37",
                "ref",
                "alt",
                "data_source",
                "source_system",
            ]:
                out_df[c] = pd.NA

        # Notes for genes with no SNPs
        out_df["note"] = out_df.get("note", pd.NA)
        out_df.loc[out_df["source_id"].isna(), "note"] = out_df.loc[out_df["source_id"].isna(), "note"].fillna(
            "No SNPs found overlapping the gene region"
        )

        # Append duplicates (align to same internal schema first)
        if not duplicates_df.empty:
            dup_aligned = duplicates_df.reindex(columns=out_df.columns, fill_value=pd.NA)
            out_df = pd.concat([out_df, dup_aligned], ignore_index=True)

        return self._finalize_output(
            out_df,
            assembly=assembly,
            output_columns=output_columns,
        )

    # -----------------------------
    # Output formatting
    # -----------------------------
    def _finalize_output(
        self,
        df: pd.DataFrame,
        assembly: str | None = None,
        output_columns: list[str] | None = None,
    ) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        # Map internal -> display names
        rename = {
            "input_gene": "Input Gene",
            "entity_value": "Matched Name",
            "alias_type": "Alias Type",
            "xref_source": "Alias Source",
            "symbol": "HGNC Symbol",
            "entity_id": "Gene Entity ID",
            "gene_build": "Gene Build",
            "gene_chr": "Gene Chr (23:X/24:Y)",
            "gene_start_38": "Gene Start (Build 38)",
            "gene_end_38": "Gene End (Build 38)",
            "gene_strand": "Gene Strand",
            "region_label": "Region Label",
            "source_type": "Variant Type",
            "source_id": "Variant ID",
            "snp_chr": "SNP Chr (23:X/24:Y)",
            "snp_pos_38": "SNP Pos (Build 38)",
            "snp_pos_37": "SNP Pos (Build 37)",
            "ref": "Ref Allele",
            "alt": "Alt Allele",
            "data_source": "DataSource",
            "source_system": "SourceSystem",
            "note": "Note",
        }

        df = df.rename(columns=rename)

        # Hide SNP position columns depending on assembly parameter (OUTPUT-ONLY)
        if assembly == "38":
            df = df.drop(columns=["SNP Pos (Build 37)"], errors="ignore")
        elif assembly == "37":
            df = df.drop(columns=["SNP Pos (Build 38)"], errors="ignore")

        # Convert int-like columns to "integer-formatted strings" (no .0), preserving NA
        int_like_display_cols = [
            "Gene Build",
            "Gene Chr (23:X/24:Y)",
            "Gene Start (Build 38)",
            "Gene End (Build 38)",
            "Variant ID",
            "SNP Chr (23:X/24:Y)",
            "SNP Pos (Build 38)",
            "SNP Pos (Build 37)",
        ]
        for col in int_like_display_cols:
            if col in df.columns:
                s = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                df[col] = s.astype("string")

        # Filter columns if requested (DISPLAY NAMES)
        if output_columns is not None:
            keep = [c for c in output_columns if c in df.columns]
            # Always keep Note if present (helpful for duplicates/no SNP)
            if "Note" in df.columns and "Note" not in keep:
                keep.append("Note")
            df = df[keep]

        # Default ordering: use display column contract (but skip columns not present)
        ordered_cols = [c for c in self.available_columns() if c in df.columns]
        # If user filtered, preserve user order (already in df)
        if output_columns is not None:
            return df

        return df[ordered_cols]
