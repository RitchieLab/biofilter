from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Any

from sqlalchemy import select as sa_select

import pandas as pd
from sqlalchemy import MetaData, text

from biofilter.modules.db.models import (
    VariantConsequence,
    VariantConsequenceCategory,
    VariantConsequenceGroup,
)
from biofilter.modules.db.models.model_variants import (
    map_variant_effect_predictions,
    map_variant_masters,
    map_variant_molecular_effects,
)
from biofilter.modules.report.reports.base_report import ReportBase


class VariantAnnotationExpandedReport(ReportBase):
    """
    Second-stage variant annotation report.

    Takes the output CSV from OneVariantAnnotationReport (gene-gene list) as input,
    extracts all unique genes (seed + partners), and for each gene fetches all
    variants in its chromosomal range with full annotation from:
      - variant_masters
      - variant_molecular_effects (with consequence group/category lookup)
      - variant_effect_predictions (AlphaMissense, fixed columns)

    Joins molecular_effects and effect_predictions on normalized transcript_id
    (stripping Ensembl version suffix). When one side has more rows than the other,
    overflow rows are added with a sequence number > 1.

    Uses a PostgreSQL TEMPORARY TABLE to accumulate results in batches,
    then returns the full DataFrame at the end.
    """

    name = "variant_annotation_expanded"
    description = (
        "Expands a gene-gene list (from OneVariantAnnotationReport) into a full "
        "per-gene, per-variant annotation table joining variant_masters, "
        "variant_molecular_effects, and variant_effect_predictions (AlphaMissense)."
    )

    BATCH_SIZE = 500
    INSERT_BATCH_SIZE = 1000
    GENE_WINDOW = 50  # genes per window for mol_effects/predictions batch fetch

    columns = [
        # Gene context (which gene this variant belongs to)
        "gene_entity_id",
        "gene_symbol",
        "gene_chromosome",
        "gene_start",
        "gene_end",
        # Variant master
        "chromosome",
        "variant_id",
        "position_start",
        "position_end",
        "reference_allele",
        "alternate_allele",
        "rsid",
        "variant_type",
        "allele_type",
        "an",
        "af",
        "grpmax",
        "cadd_raw_score",
        "cadd_phred",
        # Row sequence within (gene_entity_id, variant_id)
        "sequence",
        # Molecular effects (VEP annotation)
        "vep_gene_id",
        "vep_gene_symbol",
        "transcript_id",
        "feature_type",
        "consequence_raw",
        "consequence_name",
        "consequence_group",
        "consequence_category",
        "consequence_rank",
        "impact_rank",
        "is_most_severe_for_annotation",
        "is_most_severe_for_variant",
        "canonical",
        "mane_select",
        "mane_plus_clinical",
        "hgvsc",
        "hgvsp",
        "lof_confidence",
        "lof_flag",
        "lof_filter",
        # AlphaMissense predictions
        "alphamissense_score",
        "alphamissense_classification",
    ]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_consequence_map(self) -> dict[int, dict[str, Any]]:
        """Load all variant consequences with group and category into memory."""
        rows = (
            self.session.query(
                VariantConsequence.id,
                VariantConsequence.name.label("consequence_name"),
                VariantConsequence.severity_rank.label("consequence_rank"),
                VariantConsequenceGroup.name.label("consequence_group"),
                VariantConsequenceCategory.name.label("consequence_category"),
            )
            .join(
                VariantConsequenceGroup,
                VariantConsequenceGroup.id == VariantConsequence.consequence_group_id,
                isouter=True,
            )
            .join(
                VariantConsequenceCategory,
                VariantConsequenceCategory.id == VariantConsequence.consequence_category_id,
                isouter=True,
            )
            .all()
        )
        return {
            row.id: {
                "consequence_name": row.consequence_name,
                "consequence_rank": row.consequence_rank,
                "consequence_group": row.consequence_group,
                "consequence_category": row.consequence_category,
            }
            for row in rows
        }

    def _extract_unique_genes(self, input_path: str) -> list[dict[str, Any]]:
        """
        Read the first report CSV and return a deduplicated list of genes
        (seed + partners) with their chromosomal range.
        """
        df = pd.read_csv(input_path, dtype=str)

        seed_rename = {
            "seed_gene_entity_id": "gene_entity_id",
            "seed_gene_symbol": "gene_symbol",
            "seed_gene_chromosome": "gene_chromosome",
            "seed_gene_start": "gene_start",
            "seed_gene_end": "gene_end",
        }
        partner_rename = {
            "partner_gene_entity_id": "gene_entity_id",
            "partner_gene_symbol": "gene_symbol",
            "partner_gene_chromosome": "gene_chromosome",
            "partner_gene_start": "gene_start",
            "partner_gene_end": "gene_end",
        }

        def _subset(col_map: dict) -> pd.DataFrame:
            cols = list(col_map.keys())
            missing = [c for c in cols if c not in df.columns]
            if missing:
                return pd.DataFrame(columns=list(col_map.values()))
            sub = df[cols].rename(columns=col_map).copy()
            return sub.dropna(subset=["gene_entity_id"])

        combined = pd.concat(
            [_subset(seed_rename), _subset(partner_rename)],
            ignore_index=True,
        )
        combined = combined.drop_duplicates(subset=["gene_entity_id"])

        for col in ["gene_entity_id", "gene_chromosome", "gene_start", "gene_end"]:
            combined[col] = pd.to_numeric(combined[col], errors="coerce").astype("Int64")

        combined = combined.dropna(
            subset=["gene_entity_id", "gene_chromosome", "gene_start", "gene_end"]
        )
        return combined.to_dict(orient="records")

    # ------------------------------------------------------------------
    # DB queries
    # ------------------------------------------------------------------

    def _fetch_variants_for_gene(self, vm, chromosome: int, start: int, end: int) -> list:
        """Fetch all variant_masters rows within a gene's chromosomal range."""
        conn = self.session.connection()
        result = conn.execute(
            vm.select().where(
                (vm.c.chromosome == chromosome)
                & (vm.c.position_start >= start)
                & (vm.c.position_start <= end)
            )
        )
        return result.fetchall()

    def _fetch_molecular_effects(self, vme, chromosome: int, variant_ids: list[int]) -> list:
        """Fetch variant_molecular_effects for variant_ids in batches.

        Selects only the columns actually used downstream to avoid errors when
        the DB table is missing columns that are defined in the model but not yet
        loaded (e.g. variant_class).
        """
        used_cols = [
            vme.c.variant_id,
            vme.c.chromosome,
            vme.c.gene_id,
            vme.c.gene_symbol,
            vme.c.transcript_id,
            vme.c.feature_type,
            vme.c.consequence_raw,
            vme.c.consequence_id,
            vme.c.impact_rank,
            vme.c.is_most_severe_for_annotation,
            vme.c.is_most_severe_for_variant,
            vme.c.canonical,
            vme.c.mane_select,
            vme.c.mane_plus_clinical,
            vme.c.hgvsc,
            vme.c.hgvsp,
            vme.c.lof_confidence,
            vme.c.lof_flag,
            vme.c.lof_filter,
        ]

        all_rows: list = []
        conn = self.session.connection()
        for i in range(0, len(variant_ids), self.BATCH_SIZE):
            batch = variant_ids[i : i + self.BATCH_SIZE]
            result = conn.execute(
                sa_select(*used_cols).where(
                    (vme.c.chromosome == chromosome) & (vme.c.variant_id.in_(batch))
                )
            )
            all_rows.extend(result.fetchall())
        return all_rows

    def _fetch_predictions(self, vep, chromosome: int, variant_ids: list[int]) -> list:
        """Fetch AlphaMissense predictions for variant_ids in batches."""
        all_rows: list = []
        conn = self.session.connection()
        for i in range(0, len(variant_ids), self.BATCH_SIZE):
            batch = variant_ids[i : i + self.BATCH_SIZE]
            result = conn.execute(
                vep.select().where(
                    (vep.c.chromosome == chromosome)
                    & (vep.c.variant_id.in_(batch))
                    & (vep.c.predictor_name.ilike("alphamissense"))
                )
            )
            all_rows.extend(result.fetchall())
        return all_rows

    # ------------------------------------------------------------------
    # Row builder
    # ------------------------------------------------------------------

    def _build_rows_for_gene(
        self,
        gene: dict,
        vm_rows: list,
        mol_by_variant: dict[int, list],
        pred_by_variant: dict[int, list],
        consequence_map: dict[int, dict],
        gene_idx: int = 0,
        gene_total: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Build output rows for one gene.

        For each variant in the gene's range:
          - FULL OUTER JOIN variant_molecular_effects + variant_effect_predictions
            on normalized transcript_id (version suffix stripped).
          - sequence = row counter within (gene_entity_id, variant_id).
          - Unmatched prediction rows (no mol_effect transcript match) get appended
            with molecular_effect columns as None.
          - Variants with no annotation get a single bare row (sequence=1, all None).
        """
        gene_entity_id = int(gene["gene_entity_id"])
        gene_start = int(gene["gene_start"])
        gene_end = int(gene["gene_end"])
        gene_symbol = gene.get("gene_symbol", "?")
        gene_prefix = f"[{gene_idx}/{gene_total}] {gene_symbol}"
        rows: list[dict[str, Any]] = []

        for v_idx, vm_row in enumerate(vm_rows, 1):
            # Defensive range check (should already be filtered, but be safe)
            if int(vm_row.position_start) < gene_start or int(vm_row.position_start) > gene_end:
                continue

            variant_id = int(vm_row.variant_id)
            chromosome = int(vm_row.chromosome)

            vm_base: dict[str, Any] = {
                "gene_entity_id": gene_entity_id,
                "gene_symbol": gene.get("gene_symbol"),
                "gene_chromosome": chromosome,
                "gene_start": gene_start,
                "gene_end": gene_end,
                "chromosome": chromosome,
                "variant_id": variant_id,
                "position_start": int(vm_row.position_start),
                "position_end": int(vm_row.position_end),
                "reference_allele": vm_row.reference_allele,
                "alternate_allele": vm_row.alternate_allele,
                "rsid": vm_row.rsid,
                "variant_type": vm_row.variant_type,
                "allele_type": vm_row.allele_type,
                "an": vm_row.an,
                "af": vm_row.af,
                "grpmax": vm_row.grpmax,
                "cadd_raw_score": vm_row.cadd_raw_score,
                "cadd_phred": vm_row.cadd_phred,
            }

            mol_rows = mol_by_variant.get(variant_id, [])
            pred_rows = pred_by_variant.get(variant_id, [])

            # Build a lookup: stripped Ensembl transcript_id → prediction row.
            # variant_effect_predictions stores IDs with version suffix (ENST00000252486.9)
            # variant_molecular_effects stores them without (ENST00000252486).
            # Stripping the suffix from the prediction side aligns the two.
            pred_by_tx: dict[str, Any] = {}
            for pred in pred_rows:
                key = pred.transcript_id.split(".")[0] if pred.transcript_id else ""
                if key and key not in pred_by_tx:
                    pred_by_tx[key] = pred

            if not mol_rows and not pred_rows:
                row = {**vm_base, "sequence": 1}
                for col in self.columns:
                    if col not in row:
                        row[col] = None
                rows.append(row)
                continue

            seq = 1

            # Primary rows: one per mol_effect annotation
            for me in mol_rows:
                csq = consequence_map.get(int(me.consequence_id), {}) if me.consequence_id is not None else {}

                # Match prediction by transcript_id (mol_effects side has no version suffix)
                me_tx_key = me.transcript_id.split(".")[0] if me.transcript_id else ""
                pred = pred_by_tx.get(me_tx_key)

                rows.append({
                    **vm_base,
                    "sequence": seq,
                    "vep_gene_id": me.gene_id,
                    "vep_gene_symbol": me.gene_symbol,
                    "transcript_id": me.transcript_id,
                    "feature_type": me.feature_type,
                    "consequence_raw": me.consequence_raw,
                    "consequence_name": csq.get("consequence_name"),
                    "consequence_group": csq.get("consequence_group"),
                    "consequence_category": csq.get("consequence_category"),
                    "consequence_rank": csq.get("consequence_rank"),
                    "impact_rank": me.impact_rank,
                    "is_most_severe_for_annotation": me.is_most_severe_for_annotation,
                    "is_most_severe_for_variant": me.is_most_severe_for_variant,
                    "canonical": me.canonical,
                    "mane_select": me.mane_select,
                    "mane_plus_clinical": me.mane_plus_clinical,
                    "hgvsc": me.hgvsc,
                    "hgvsp": me.hgvsp,
                    "lof_confidence": me.lof_confidence,
                    "lof_flag": me.lof_flag,
                    "lof_filter": me.lof_filter,
                    "alphamissense_score": pred.score if pred else None,
                    "alphamissense_classification": pred.classification if pred else None,
                })
                seq += 1

            # If there are predictions but no mol_effect rows, emit a bare prediction row
            if not mol_rows and pred_rows:
                rows.append({
                    **vm_base,
                    "sequence": seq,
                    "vep_gene_id": None,
                    "vep_gene_symbol": None,
                    "transcript_id": pred_rows[0].transcript_id,
                    "feature_type": None,
                    "consequence_raw": None,
                    "consequence_name": None,
                    "consequence_group": None,
                    "consequence_category": None,
                    "consequence_rank": None,
                    "impact_rank": None,
                    "is_most_severe_for_annotation": None,
                    "is_most_severe_for_variant": None,
                    "canonical": None,
                    "mane_select": None,
                    "mane_plus_clinical": None,
                    "hgvsc": None,
                    "hgvsp": None,
                    "lof_confidence": None,
                    "lof_flag": None,
                    "lof_filter": None,
                    "alphamissense_score": pred_rows[0].score,
                    "alphamissense_classification": pred_rows[0].classification,
                })

            # variant_rows_count = seq - 1 if mol_rows else 1
            # self.logger.log(
            #     f"{gene_prefix} | variant {v_idx}/{len(vm_rows)} "
            #     f"id={variant_id}: {variant_rows_count} row(s) inserted"
            # )

        return rows

    # ------------------------------------------------------------------
    # Temp table helpers
    # ------------------------------------------------------------------

    def _create_temp_table(self, tmp_name: str, is_sqlite: bool) -> None:
        """Create the temporary table for result accumulation."""
        keyword = "TEMP" if is_sqlite else "TEMPORARY"
        col_defs = ",\n    ".join([f"{col} TEXT" for col in self.columns])
        ddl = f"CREATE {keyword} TABLE {tmp_name} (\n    {col_defs}\n)"
        self.session.execute(text(ddl))
        self.session.flush()

    def _insert_batch(self, tmp_name: str, rows: list[dict[str, Any]]) -> None:
        """Insert a list of row dicts into the temp table."""
        if not rows:
            return
        placeholders = ", ".join([f":{col}" for col in self.columns])
        insert_sql = text(
            f"INSERT INTO {tmp_name} ({', '.join(self.columns)}) "
            f"VALUES ({placeholders})"
        )
        # Normalize row dicts: all columns present, None for missing
        params = [
            {col: _safe_str(row.get(col)) for col in self.columns}
            for row in rows
        ]
        for i in range(0, len(params), self.INSERT_BATCH_SIZE):
            self.session.execute(insert_sql, params[i : i + self.INSERT_BATCH_SIZE])
        self.session.flush()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self) -> pd.DataFrame:
        input_path = self.param("input_file", required=True)

        self.logger.log(f"Reading gene list from: {input_path}")

        # 1. Load consequence dimension table
        consequence_map = self._load_consequence_map()
        self.logger.log(f"Loaded {len(consequence_map)} consequence entries")

        # 2. Extract unique genes from first report CSV
        genes = self._extract_unique_genes(input_path)
        self.logger.log(f"{len(genes)} unique genes to process")

        # 3. Map partitioned tables
        engine = self.session.get_bind()
        metadata = MetaData()
        vm = map_variant_masters(engine, metadata)
        vme = map_variant_molecular_effects(engine, metadata)
        vep = map_variant_effect_predictions(engine, metadata)

        is_sqlite = engine.dialect.name == "sqlite"

        # 4. Create temp table
        tmp_name = f"_vae_{uuid.uuid4().hex[:10]}"
        self._create_temp_table(tmp_name, is_sqlite)
        self.logger.log(f"Temp table created: {tmp_name}")

        # 5. Process genes in windows of GENE_WINDOW.
        #
        #    Strategy: variant_masters is queried once per gene (fast — uses the
        #    chromosome partition index + position range). variant_molecular_effects
        #    and variant_effect_predictions are the expensive tables; we batch their
        #    lookups across N genes at a time to reduce round-trips while keeping
        #    memory bounded.
        #
        #    This avoids both extremes:
        #    - "one query per gene" → 8k × 3 queries (too many round-trips)
        #    - "one query per chromosome" → loads entire chr1 into Python RAM
        conn = self.session.connection()

        total_rows = 0
        total_genes = len(genes)

        for window_start in range(0, total_genes, self.GENE_WINDOW):
            window = genes[window_start : window_start + self.GENE_WINDOW]
            window_end_idx = window_start + len(window)

            self.logger.log(
                f"Window genes {window_start + 1}–{window_end_idx} / {total_genes}"
            )

            # --- Step A: fetch variant_masters per gene (one query per gene) ---
            # Grouped by chromosome so we can also batch mol_effects/predictions
            # across genes on the same chromosome within this window.
            chrom_to_window_genes: dict[int, list[dict]] = defaultdict(list)
            for gene in window:
                gene["_vm_rows"] = []  # will be filled below
                chrom = int(gene["gene_chromosome"])
                chrom_to_window_genes[chrom].append(gene)

            all_variant_ids_in_window: dict[int, list[int]] = defaultdict(list)  # chrom → ids

            for chrom, chrom_window_genes in chrom_to_window_genes.items():
                for gene in chrom_window_genes:
                    result = conn.execute(
                        vm.select().where(
                            (vm.c.chromosome == chrom)
                            & (vm.c.position_start >= int(gene["gene_start"]))
                            & (vm.c.position_start <= int(gene["gene_end"]))
                        )
                    )
                    gene["_vm_rows"] = result.fetchall()
                    all_variant_ids_in_window[chrom].extend(
                        int(r.variant_id) for r in gene["_vm_rows"]
                    )

            # --- Step B: batch fetch mol_effects + predictions for all variant_ids
            #             in this window, grouped by chromosome ---
            mol_by_variant: dict[int, list] = defaultdict(list)
            pred_by_variant: dict[int, list] = defaultdict(list)

            for chrom, variant_ids in all_variant_ids_in_window.items():
                if not variant_ids:
                    continue
                unique_ids = list(set(variant_ids))
                mol_rows = self._fetch_molecular_effects(vme, chrom, unique_ids)
                pred_rows = self._fetch_predictions(vep, chrom, unique_ids)
                for r in mol_rows:
                    mol_by_variant[int(r.variant_id)].append(r)
                for r in pred_rows:
                    pred_by_variant[int(r.variant_id)].append(r)

            # --- Step C: build and insert output rows per gene ---
            for gene_idx_in_run, gene in enumerate(window, window_start + 1):
                gene_vm_rows = gene.pop("_vm_rows", [])
                gene_symbol = gene.get("gene_symbol", "?")

                if not gene_vm_rows:
                    self.logger.log(
                        f"[{gene_idx_in_run}/{total_genes}] {gene_symbol}: no variants in range"
                    )
                    continue

                output_rows = self._build_rows_for_gene(
                    gene=gene,
                    vm_rows=gene_vm_rows,
                    mol_by_variant=mol_by_variant,
                    pred_by_variant=pred_by_variant,
                    consequence_map=consequence_map,
                    gene_idx=gene_idx_in_run,
                    gene_total=total_genes,
                )

                self._insert_batch(tmp_name, output_rows)
                total_rows += len(output_rows)

                self.logger.log(
                    f"[{gene_idx_in_run}/{total_genes}] {gene_symbol}: "
                    f"{len(gene_vm_rows)} variants → {len(output_rows)} rows "
                    f"(total so far: {total_rows:,})"
                )

        # 6. Final SELECT from temp table
        self.logger.log(f"Done. Selecting {total_rows} rows from temp table.")
        result = self.session.execute(text(f"SELECT * FROM {tmp_name}"))
        df = pd.DataFrame(result.fetchall(), columns=self.columns)

        # 7. Cast numeric and boolean columns
        int_cols = [
            "gene_entity_id", "gene_chromosome", "gene_start", "gene_end",
            "chromosome", "variant_id", "position_start", "position_end",
            "sequence", "an", "consequence_rank", "impact_rank",
        ]
        float_cols = ["af", "cadd_raw_score", "cadd_phred", "alphamissense_score"]
        bool_cols = [
            "is_most_severe_for_annotation", "is_most_severe_for_variant",
            "canonical", "mane_select", "mane_plus_clinical", "lof_flag",
        ]

        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        for col in float_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        _bool_map = {"True": True, "False": False, "true": True, "false": False, "None": None}
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].map(lambda v: _bool_map.get(v, None) if isinstance(v, str) else v)

        self.results = df
        return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Module-level helper
# ---------------------------------------------------------------------------

def _safe_str(value: Any) -> str | None:
    """Convert a value to string for temp table insertion; None stays None."""
    if value is None:
        return None
    return str(value)
