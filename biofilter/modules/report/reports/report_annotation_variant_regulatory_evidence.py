from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Optional

import pandas as pd
from sqlalchemy import func, text
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import (
    EntityAlias,
    EntityGroup,
    EntityLocation,
    GeneMaster,
)
from biofilter.modules.report.reports.base_report import ReportBase

_TEMP_TABLE = "_bf_vre_ranges"

_RSID_RE = re.compile(r"^rs\d+$", re.IGNORECASE)
_COORD_RE = re.compile(r"^(?:chr)?(?P<chr>[0-9XYMTm]+)\s*[:,\-_\s]\s*(?P<pos>\d+)\s*$")


def _norm(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _to_list(v: Any) -> list[str]:
    if v is None:
        return []
    if isinstance(v, (list, tuple, set)):
        return [_norm(x) for x in v if _norm(x)]
    s = _norm(v)
    return [p.strip() for p in s.split(",") if p.strip()] if s else []


def _parse_chrom(token: str) -> Optional[int]:
    s = str(token or "").strip().lower().replace("chr", "")
    if s in {"x"}:
        return 23
    if s in {"y"}:
        return 24
    if s in {"m", "mt"}:
        return 25
    try:
        c = int(s)
    except Exception:
        return None
    return c if 1 <= c <= 25 else None


def _empty_df(input_value: Any, status: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "resolution_status": status,
                "input_term": str(input_value),
                "input_gene_entity_id": None,
                "input_gene_symbol": None,
                "variant_id": None,
                "chromosome": None,
                "position_start": None,
                "position_end": None,
                "rsid": None,
                "reference_allele": None,
                "alternate_allele": None,
                "position_gene_symbol": None,
                "position_gene_ensembl": None,
                "eqtl_target_symbol": None,
                "eqtl_target_ensembl": None,
                "bio_context": None,
                "qtl_type": None,
                "beta": None,
                "se": None,
                "p_value": None,
                "n": None,
                "effect_allele": None,
                "details": None,
                "data_source_id": None,
                "etl_package_id": None,
            }
        ]
    )


class AnnotationVariantRegulatoryEvidenceReport(ReportBase):
    """
    Annotation report: variant ↔ gene regulatory evidence (eQTL / sQTL).

    Accepts three input modes selected via `--param input_type`:
      - gene  : list of gene symbols (HGNC), Ensembl IDs, Entrez IDs, or any
                alias known to entity_aliases. Resolves to entity → genomic
                locus and pulls variants in [start - flanking_bp,
                end + flanking_bp] from variant_masters.
      - coord : list of "chr:pos" coordinates. Pulls variants in
                [pos - flanking_bp, pos + flanking_bp] per position.
      - rsid  : list of dbSNP rsids. Direct lookup against variant_masters.rsid
                (scans all chromosome partitions; small input lists only).

    Each emitted row joins a variant to one row of
    variant_gene_regulatory_evidence — i.e. one tissue × one gene × one
    qtl_type per row.

    Output is gene-centric: every row carries the eQTL target gene
    (regulated gene from the eQTL table) and the gene whose body contains
    the variant (resolved via entity_locations) — these may differ when the
    variant regulates a neighboring gene in cis.
    """

    name = "annotation_variant_regulatory_evidence"
    description = (
        "Variant → gene regulatory evidence (eQTL / sQTL). Accepts gene "
        "symbols, rsids, or chr:pos coordinates as input. Returns one row "
        "per (variant × tissue × regulated gene) with effect size and "
        "p-value, plus the gene whose body contains the variant."
    )

    _BATCH = 500

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @classmethod
    def example_input(cls) -> dict:
        return {
            "input_data": ["APOE"],
            "input_type": "gene",
            "build": 38,
            "flanking_bp": 0,
            "tissue": None,
            "qtl_type": "eQTL",
            "p_value_max": None,
            "max_rows": 10000,
        }

    @classmethod
    def explain(cls) -> str:
        return (
            "Parameters\n"
            "----------\n"
            "  input_data (required)  list[str], comma-string, or path to a .txt\n"
            "    file (one term per line). Content depends on input_type.\n"
            "\n"
            "  input_type (default 'gene')  one of {'gene', 'coord', 'rsid'}.\n"
            "    - gene  : symbols / Ensembl / Entrez / any alias.\n"
            "    - coord : 'chr1:12345' or '1:12345' or 'chr1-12345'.\n"
            "    - rsid  : 'rs123456'.\n"
            "\n"
            "  build (default 38)  genome assembly build for locus lookup.\n"
            "\n"
            "  flanking_bp (default 0)  for gene/coord input, extend each\n"
            "    range by this many bp on each side. Ignored for rsid input.\n"
            "\n"
            "  tissue (default None)  list/comma-string of bio_context labels\n"
            "    to keep, e.g. 'Brain_Cortex,Brain_Hippocampus'. None = all.\n"
            "\n"
            "  qtl_type (default 'eQTL')  filter on qtl_type column.\n"
            "    Accepts 'eQTL', 'sQTL', etc. Set to None to allow all types.\n"
            "\n"
            "  p_value_max (default None)  keep only evidence rows with\n"
            "    p_value <= p_value_max.\n"
            "\n"
            "  max_rows (default 10000)  cap on the final number of rows\n"
            "    emitted; warns if hit.\n"
        )

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self) -> pd.DataFrame:
        # ── 1. Read parameters ────────────────────────────────────────────
        # `run_example()` passes example_input() as input_data=<dict>; unpack
        # it so individual keys are available via self.param(). The nested
        # input_data must overwrite the outer one (setdefault is not enough,
        # since input_data already exists at this point pointing at the dict).
        _id = self.param("input_data", default=None)
        if isinstance(_id, dict):
            nested = _id
            inner_input = nested.get("input_data")
            self.params["input_data"] = inner_input
            for k, v in nested.items():
                if k == "input_data":
                    continue
                self.params.setdefault(k, v)
            _id = inner_input

        if not _id:
            raise ValueError("Missing required parameter: 'input_data'")

        input_type = str(self.param("input_type", default="gene") or "gene").lower()
        if input_type not in {"gene", "coord", "rsid"}:
            raise ValueError(
                f"Invalid input_type '{input_type}'. "
                "Expected one of: gene, coord, rsid."
            )

        build = int(self.param("build", default=38) or 38)
        flanking_bp = int(self.param("flanking_bp", default=0) or 0)
        tissues = _to_list(self.param("tissue", default=None))
        qtl_type = self.param("qtl_type", default="eQTL")
        if qtl_type is not None:
            qtl_type = str(qtl_type).strip() or None
        p_value_max = self.param("p_value_max", default=None)
        max_rows = int(self.param("max_rows", default=10000) or 10000)

        if isinstance(_id, (list, tuple, set)):
            input_terms = _to_list(_id)
        else:
            input_terms = self.resolve_input_list(_id, param_name="input_data")
        if not input_terms:
            return _empty_df(_id, "empty_input")

        self.logger.log(
            f"input_type={input_type}  terms={len(input_terms)}  "
            f"flanking_bp={flanking_bp}  tissues={tissues or 'all'}  "
            f"qtl_type={qtl_type or 'all'}  p_value_max={p_value_max}"
        )

        assembly_map = self.resolve_assembly_map(str(build))
        assembly_ids = list(assembly_map.values())

        # ── 2. Branch by input_type ───────────────────────────────────────
        if input_type == "gene":
            df = self._run_gene_mode(
                input_terms,
                assembly_ids=assembly_ids,
                flanking_bp=flanking_bp,
                tissues=tissues,
                qtl_type=qtl_type,
                p_value_max=p_value_max,
            )
        elif input_type == "coord":
            df = self._run_coord_mode(
                input_terms,
                flanking_bp=flanking_bp,
                tissues=tissues,
                qtl_type=qtl_type,
                p_value_max=p_value_max,
            )
        else:  # rsid
            df = self._run_rsid_mode(
                input_terms,
                tissues=tissues,
                qtl_type=qtl_type,
                p_value_max=p_value_max,
            )

        if df is None or df.empty:
            return _empty_df(input_terms, "no_evidence_found")

        # ── 3. Resolve eqtl_target ENSG → primary symbol ──────────────────
        df = self._add_eqtl_target_symbol(df)

        # ── 4. Resolve position_gene (entity_locations on variant pos) ────
        df = self._add_position_gene_columns(df, assembly_ids=assembly_ids)

        # ── 5. Apply max_rows cap ─────────────────────────────────────────
        if len(df) > max_rows:
            self.logger.log(
                f"Result has {len(df)} rows — capping at max_rows={max_rows}",
                "WARNING",
            )
            df = df.iloc[:max_rows].copy()

        df["resolution_status"] = None

        col_order = [
            "resolution_status",
            "input_term",
            "input_gene_entity_id",
            "input_gene_symbol",
            "variant_id",
            "chromosome",
            "position_start",
            "position_end",
            "rsid",
            "reference_allele",
            "alternate_allele",
            "position_gene_symbol",
            "position_gene_ensembl",
            "eqtl_target_symbol",
            "eqtl_target_ensembl",
            "bio_context",
            "qtl_type",
            "beta",
            "se",
            "p_value",
            "n",
            "effect_allele",
            "details",
            "data_source_id",
            "etl_package_id",
        ]
        for c in col_order:
            if c not in df.columns:
                df[c] = None
        return df[col_order]

    # ------------------------------------------------------------------
    # Mode: gene
    # ------------------------------------------------------------------

    def _run_gene_mode(
        self,
        input_terms: list[str],
        assembly_ids: list[int],
        flanking_bp: int,
        tissues: list[str],
        qtl_type: Optional[str],
        p_value_max: Any,
    ) -> Optional[pd.DataFrame]:
        gene_entity_map = self._resolve_gene_symbols(input_terms)
        if not gene_entity_map:
            self.logger.log("No genes resolved from input symbols", "WARNING")
            return None
        self.logger.log(
            f"Resolved {len(gene_entity_map)}/{len(input_terms)} input gene terms"
        )

        gene_loci = self._resolve_gene_loci(
            list(gene_entity_map.keys()), assembly_ids, flanking_bp
        )
        if not gene_loci:
            self.logger.log("No genomic loci found for resolved genes", "WARNING")
            return None

        # Build temp-table rows: one per (input_term, entity_id, range)
        # We map back from entity → input_term via gene_entity_map values
        # (which already store the resolved label).
        ranges = []
        for eid, loc in gene_loci.items():
            ranges.append(
                {
                    "input_term": gene_entity_map[eid],
                    "entity_id": eid,
                    "chromosome": loc["chromosome"],
                    "range_start": loc["start_pos"],
                    "range_end": loc["end_pos"],
                }
            )

        return self._query_ranges(
            ranges,
            tissues=tissues,
            qtl_type=qtl_type,
            p_value_max=p_value_max,
            include_input_gene=True,
        )

    # ------------------------------------------------------------------
    # Mode: coord
    # ------------------------------------------------------------------

    def _run_coord_mode(
        self,
        input_terms: list[str],
        flanking_bp: int,
        tissues: list[str],
        qtl_type: Optional[str],
        p_value_max: Any,
    ) -> Optional[pd.DataFrame]:
        ranges: list[dict] = []
        for term in input_terms:
            m = _COORD_RE.match(term.strip())
            if not m:
                self.logger.log(f"Skipped malformed coord: '{term}'", "WARNING")
                continue
            chrom = _parse_chrom(m.group("chr"))
            if chrom is None:
                self.logger.log(
                    f"Skipped coord with unknown chromosome: '{term}'", "WARNING"
                )
                continue
            pos = int(m.group("pos"))
            ranges.append(
                {
                    "input_term": term,
                    "entity_id": None,
                    "chromosome": chrom,
                    "range_start": max(0, pos - flanking_bp),
                    "range_end": pos + flanking_bp,
                }
            )

        if not ranges:
            self.logger.log("No valid coords parsed from input", "WARNING")
            return None

        return self._query_ranges(
            ranges,
            tissues=tissues,
            qtl_type=qtl_type,
            p_value_max=p_value_max,
            include_input_gene=False,
        )

    # ------------------------------------------------------------------
    # Mode: rsid
    # ------------------------------------------------------------------

    def _run_rsid_mode(
        self,
        input_terms: list[str],
        tissues: list[str],
        qtl_type: Optional[str],
        p_value_max: Any,
    ) -> Optional[pd.DataFrame]:
        rsids = [t.strip() for t in input_terms if _RSID_RE.match(t.strip())]
        skipped = len(input_terms) - len(rsids)
        if skipped:
            self.logger.log(
                f"Skipped {skipped} term(s) that don't match rs<digits> pattern",
                "WARNING",
            )
        if not rsids:
            return None

        # Map rsid → input_term (preserving original casing from input)
        rsid_to_input = {r.lower(): r for r in rsids}
        rsids_lower = list(rsid_to_input.keys())

        bind = {"rsids": rsids_lower}
        sql_filters = ["LOWER(vm.rsid) = ANY(:rsids)"]
        bind_params, extra_filters = self._evidence_filters(
            tissues=tissues, qtl_type=qtl_type, p_value_max=p_value_max
        )
        bind.update(bind_params)
        sql_filters.extend(extra_filters)

        sql = f"""
            SELECT
                vm.variant_id            AS variant_id,
                vm.chromosome            AS chromosome,
                vm.position_start        AS position_start,
                vm.position_end          AS position_end,
                vm.rsid                  AS rsid,
                vm.reference_allele      AS reference_allele,
                vm.alternate_allele      AS alternate_allele,
                vgre.gene_id             AS eqtl_target_ensembl,
                vgre.bio_context         AS bio_context,
                vgre.qtl_type            AS qtl_type,
                vgre.beta                AS beta,
                vgre.se                  AS se,
                vgre.p_value             AS p_value,
                vgre.n                   AS n,
                vgre.effect_allele       AS effect_allele,
                vgre.details             AS details,
                vgre.data_source_id      AS data_source_id,
                vgre.etl_package_id      AS etl_package_id
            FROM variant_masters vm
            JOIN variant_gene_regulatory_evidence vgre
                ON vgre.chromosome = vm.chromosome
                AND vgre.variant_id = vm.variant_id
            WHERE {" AND ".join(sql_filters)}
        """  # noqa: S608 — bind params used for all user-provided values

        try:
            rows = self.session.execute(text(sql), bind).mappings().all()
        except Exception as exc:
            self.logger.log(f"rsid mode query failed: {exc}", "ERROR")
            return None

        if not rows:
            return None

        df = pd.DataFrame([dict(r) for r in rows])
        df["input_term"] = df["rsid"].apply(
            lambda r: rsid_to_input.get(str(r).lower()) if r is not None else None
        )
        df["input_gene_entity_id"] = None
        df["input_gene_symbol"] = None
        return df

    # ------------------------------------------------------------------
    # Range-based query (used by gene + coord modes)
    # ------------------------------------------------------------------

    def _query_ranges(
        self,
        ranges: list[dict],
        tissues: list[str],
        qtl_type: Optional[str],
        p_value_max: Any,
        include_input_gene: bool,
    ) -> Optional[pd.DataFrame]:
        if not ranges:
            return None

        self._create_temp_table()
        try:
            self._populate_temp_table(ranges)

            chromosomes = sorted({r["chromosome"] for r in ranges})
            self.logger.log(
                f"Querying {len(chromosomes)} chromosome partition(s) "
                f"with {len(ranges)} range(s)"
            )

            frames: list[pd.DataFrame] = []
            for chrom in chromosomes:
                df_chrom = self._query_chromosome(
                    chromosome=chrom,
                    tissues=tissues,
                    qtl_type=qtl_type,
                    p_value_max=p_value_max,
                    include_input_gene=include_input_gene,
                )
                if df_chrom is not None and not df_chrom.empty:
                    frames.append(df_chrom)
                    self.logger.log(
                        f"  chr{chrom}: {len(df_chrom)} rows, "
                        f"{df_chrom['variant_id'].nunique()} unique variants"
                    )
        finally:
            self._drop_temp_table()

        if not frames:
            return None
        return pd.concat(frames, ignore_index=True)

    def _query_chromosome(
        self,
        chromosome: int,
        tissues: list[str],
        qtl_type: Optional[str],
        p_value_max: Any,
        include_input_gene: bool,
    ) -> Optional[pd.DataFrame]:
        bind: dict[str, Any] = {"chromosome": chromosome}
        sql_filters = [
            "vm.chromosome = :chromosome",
            "gr.chromosome = :chromosome",
        ]

        ev_bind, ev_filters = self._evidence_filters(
            tissues=tissues, qtl_type=qtl_type, p_value_max=p_value_max
        )
        bind.update(ev_bind)
        sql_filters.extend(ev_filters)

        sql = f"""
            SELECT
                gr.input_term            AS input_term,
                gr.entity_id             AS input_gene_entity_id,
                vm.variant_id            AS variant_id,
                vm.chromosome            AS chromosome,
                vm.position_start        AS position_start,
                vm.position_end          AS position_end,
                vm.rsid                  AS rsid,
                vm.reference_allele      AS reference_allele,
                vm.alternate_allele      AS alternate_allele,
                vgre.gene_id             AS eqtl_target_ensembl,
                vgre.bio_context         AS bio_context,
                vgre.qtl_type            AS qtl_type,
                vgre.beta                AS beta,
                vgre.se                  AS se,
                vgre.p_value             AS p_value,
                vgre.n                   AS n,
                vgre.effect_allele       AS effect_allele,
                vgre.details             AS details,
                vgre.data_source_id      AS data_source_id,
                vgre.etl_package_id      AS etl_package_id
            FROM {_TEMP_TABLE} gr
            JOIN variant_masters vm
                ON  vm.chromosome      = gr.chromosome
                AND vm.position_start >= gr.range_start
                AND vm.position_start <= gr.range_end
            JOIN variant_gene_regulatory_evidence vgre
                ON  vgre.chromosome  = vm.chromosome
                AND vgre.variant_id  = vm.variant_id
            WHERE {" AND ".join(sql_filters)}
        """  # noqa: S608

        try:
            rows = self.session.execute(text(sql), bind).mappings().all()
        except Exception as exc:
            self.logger.log(f"Query failed for chr{chromosome}: {exc}", "ERROR")
            return None

        if not rows:
            return None

        df = pd.DataFrame([dict(r) for r in rows])

        if include_input_gene and not df.empty:
            entity_to_symbol = self._batch_entity_primary_symbols(
                [int(e) for e in df["input_gene_entity_id"].dropna().unique()]
            )
            df["input_gene_symbol"] = df["input_gene_entity_id"].map(
                lambda e: entity_to_symbol.get(int(e)) if pd.notna(e) else None
            )
        else:
            df["input_gene_symbol"] = None
            df["input_gene_entity_id"] = None

        return df

    @staticmethod
    def _evidence_filters(
        tissues: list[str],
        qtl_type: Optional[str],
        p_value_max: Any,
    ) -> tuple[dict[str, Any], list[str]]:
        bind: dict[str, Any] = {}
        clauses: list[str] = []
        if tissues:
            bind["tissues"] = tissues
            clauses.append("vgre.bio_context = ANY(:tissues)")
        if qtl_type:
            bind["qtl_type"] = qtl_type
            clauses.append("vgre.qtl_type = :qtl_type")
        if p_value_max is not None:
            bind["p_value_max"] = float(p_value_max)
            clauses.append("vgre.p_value <= :p_value_max")
        return bind, clauses

    # ------------------------------------------------------------------
    # Gene resolution (mirrors report_gene_to_variant_filtering)
    # ------------------------------------------------------------------

    def _resolve_gene_symbols(self, symbols: list[str]) -> dict[int, str]:
        gene_group = (
            self.session.query(EntityGroup)
            .filter(func.lower(EntityGroup.name) == "genes")
            .first()
        )
        if gene_group is None:
            self.logger.log("EntityGroup 'Genes' not found", "ERROR")
            return {}

        symbols_lower = [s.lower() for s in symbols]
        rows = (
            self.session.query(EntityAlias.entity_id, EntityAlias.alias_value)
            .filter(
                EntityAlias.group_id == gene_group.id,
                func.lower(EntityAlias.alias_value).in_(symbols_lower),
            )
            .all()
        )

        entity_ids = {int(r.entity_id) for r in rows}
        # Build {entity_id: matching_input_term} preserving the user's original
        # casing/format, so the report's `input_term` column is faithful.
        symbol_to_input = {s.lower(): s for s in symbols}
        result: dict[int, str] = {}
        for r in rows:
            eid = int(r.entity_id)
            if eid in result:
                continue
            input_match = symbol_to_input.get(str(r.alias_value).lower(), r.alias_value)
            result[eid] = _norm(input_match)

        # If we have GeneMaster.symbol available, prefer it as a stable label
        # only when the user input was not a clean HGNC symbol — but since the
        # user explicitly asked for the original input to flow through, we
        # keep the input term verbatim. Suppress unused warning:
        _ = entity_ids
        return result

    def _resolve_gene_loci(
        self,
        entity_ids: list[int],
        assembly_ids: list[int],
        window_bp: int,
    ) -> dict[int, dict]:
        rows = (
            self.session.query(
                EntityLocation.entity_id,
                EntityLocation.chromosome,
                EntityLocation.start_pos,
                EntityLocation.end_pos,
            )
            .filter(
                EntityLocation.entity_id.in_(entity_ids),
                EntityLocation.assembly_id.in_(assembly_ids),
            )
            .all()
        )
        result: dict[int, dict] = {}
        for r in rows:
            eid = int(r.entity_id)
            if eid in result:
                continue
            result[eid] = {
                "chromosome": int(r.chromosome),
                "start_pos": max(0, int(r.start_pos) - window_bp),
                "end_pos": int(r.end_pos) + window_bp,
            }
        return result

    # ------------------------------------------------------------------
    # Temp table lifecycle
    # ------------------------------------------------------------------

    def _create_temp_table(self) -> None:
        self.session.execute(text(f"DROP TABLE IF EXISTS {_TEMP_TABLE}"))
        self.session.execute(
            text(
                f"""
            CREATE TEMP TABLE {_TEMP_TABLE} (
                input_term  TEXT,
                entity_id   BIGINT,
                chromosome  INTEGER,
                range_start BIGINT,
                range_end   BIGINT
            )
        """
            )
        )
        self.session.flush()

    def _populate_temp_table(self, ranges: list[dict]) -> None:
        for i in range(0, len(ranges), self._BATCH):
            batch = ranges[i : i + self._BATCH]
            self.session.execute(
                text(
                    f"""
                    INSERT INTO {_TEMP_TABLE}
                        (input_term, entity_id, chromosome, range_start, range_end)
                    VALUES (:input_term, :entity_id, :chromosome,
                            :range_start, :range_end)
                """
                ),
                batch,
            )
        self.session.flush()

    def _drop_temp_table(self) -> None:
        try:
            self.session.execute(text(f"DROP TABLE IF EXISTS {_TEMP_TABLE}"))
            self.session.flush()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # eqtl_target_ensembl → eqtl_target_symbol
    # ------------------------------------------------------------------

    def _add_eqtl_target_symbol(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            df["eqtl_target_symbol"] = None
            return df
        ensgs = sorted(
            {str(g).strip() for g in df["eqtl_target_ensembl"].dropna() if str(g).strip()}
        )
        if not ensgs:
            df["eqtl_target_symbol"] = None
            return df

        # ENSG (without version) → entity_id via entity_aliases
        ea_code = aliased(EntityAlias)
        rows = (
            self.session.query(ea_code.entity_id, ea_code.alias_value)
            .filter(
                ea_code.alias_value.in_(ensgs),
                func.upper(ea_code.xref_source) == "ENSEMBL",
            )
            .all()
        )
        ensg_to_entity: dict[str, int] = {}
        for r in rows:
            v = str(r.alias_value)
            if v not in ensg_to_entity:
                ensg_to_entity[v] = int(r.entity_id)

        entity_to_symbol = self._batch_entity_primary_symbols(
            list(ensg_to_entity.values())
        )

        ensg_to_symbol = {
            ensg: entity_to_symbol.get(eid)
            for ensg, eid in ensg_to_entity.items()
        }
        df["eqtl_target_symbol"] = df["eqtl_target_ensembl"].map(
            lambda g: ensg_to_symbol.get(str(g).strip()) if pd.notna(g) else None
        )
        return df

    # ------------------------------------------------------------------
    # position_gene_symbol / position_gene_ensembl
    # ------------------------------------------------------------------

    def _add_position_gene_columns(
        self,
        df: pd.DataFrame,
        assembly_ids: list[int],
    ) -> pd.DataFrame:
        if df.empty:
            df["position_gene_symbol"] = None
            df["position_gene_ensembl"] = None
            return df

        unique_pos = df[["chromosome", "position_start"]].drop_duplicates()
        pos_to_entity: dict[tuple[int, int], int] = {}

        for chrom, group in unique_pos.groupby("chromosome"):
            positions = [int(p) for p in group["position_start"].tolist() if pd.notna(p)]
            if not positions:
                continue
            try:
                rows = self.session.execute(
                    text(
                        """
                        SELECT vp.position AS position, el.entity_id AS entity_id
                        FROM (
                            SELECT UNNEST(CAST(:positions AS BIGINT[])) AS position
                        ) vp
                        JOIN entity_locations el
                          ON el.chromosome = :chromosome
                         AND vp.position BETWEEN el.start_pos AND el.end_pos
                         AND el.assembly_id = ANY(:assembly_ids)
                        """
                    ),
                    {
                        "positions": positions,
                        "chromosome": int(chrom),
                        "assembly_ids": assembly_ids,
                    },
                ).mappings().all()
            except Exception as exc:
                self.logger.log(
                    f"position_gene lookup failed for chr{chrom}: {exc}", "WARNING"
                )
                continue

            # When a position falls into multiple overlapping gene bodies,
            # keep the first match (deterministic by query order). Overlapping
            # genes are biologically real but rare; downstream consumers can
            # cross-check the eqtl_target_* columns when they care.
            seen_positions: set[int] = set()
            for r in rows:
                key = (int(chrom), int(r["position"]))
                if key[1] in seen_positions:
                    continue
                seen_positions.add(key[1])
                pos_to_entity[key] = int(r["entity_id"])

        if not pos_to_entity:
            df["position_gene_symbol"] = None
            df["position_gene_ensembl"] = None
            return df

        entity_ids = list({eid for eid in pos_to_entity.values()})
        entity_to_symbol = self._batch_entity_primary_symbols(entity_ids)
        entity_to_ensembl = self._batch_entity_ensembl_ids(entity_ids)

        def _lookup(row, kind: str) -> Optional[str]:
            if pd.isna(row["chromosome"]) or pd.isna(row["position_start"]):
                return None
            key = (int(row["chromosome"]), int(row["position_start"]))
            eid = pos_to_entity.get(key)
            if eid is None:
                return None
            if kind == "symbol":
                return entity_to_symbol.get(eid)
            return entity_to_ensembl.get(eid)

        df["position_gene_symbol"] = df.apply(lambda r: _lookup(r, "symbol"), axis=1)
        df["position_gene_ensembl"] = df.apply(lambda r: _lookup(r, "ensembl"), axis=1)
        return df

    # ------------------------------------------------------------------
    # Entity-id → label batch helpers
    # ------------------------------------------------------------------

    def _batch_entity_primary_symbols(self, entity_ids: list[int]) -> dict[int, str]:
        if not entity_ids:
            return {}

        # Prefer GeneMaster.symbol when available (canonical HGNC symbol);
        # fall back to entity_aliases preferred / is_primary alias.
        gm_rows = (
            self.session.query(GeneMaster.entity_id, GeneMaster.symbol)
            .filter(GeneMaster.entity_id.in_(entity_ids))
            .all()
        )
        result: dict[int, str] = {
            int(r.entity_id): _norm(r.symbol)
            for r in gm_rows
            if _norm(r.symbol)
        }

        missing = [e for e in entity_ids if e not in result]
        if missing:
            ea_rows = (
                self.session.query(EntityAlias.entity_id, EntityAlias.alias_value)
                .filter(
                    EntityAlias.entity_id.in_(missing),
                    EntityAlias.is_primary.is_(True),
                )
                .all()
            )
            for r in ea_rows:
                eid = int(r.entity_id)
                if eid not in result:
                    result[eid] = _norm(r.alias_value)
        return result

    def _batch_entity_ensembl_ids(self, entity_ids: list[int]) -> dict[int, str]:
        if not entity_ids:
            return {}
        rows = (
            self.session.query(EntityAlias.entity_id, EntityAlias.alias_value)
            .filter(
                EntityAlias.entity_id.in_(entity_ids),
                func.upper(EntityAlias.xref_source) == "ENSEMBL",
            )
            .all()
        )
        result: dict[int, str] = {}
        for r in rows:
            eid = int(r.entity_id)
            if eid not in result:
                result[eid] = _norm(r.alias_value)
        return result
