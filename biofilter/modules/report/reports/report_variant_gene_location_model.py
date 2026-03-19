from __future__ import annotations

import re
from collections import OrderedDict
from typing import Any

import pandas as pd
from sqlalchemy import MetaData, Table, and_, func, or_, select
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import Entity, EntityAlias, EntityGroup, EntityLocation
from biofilter.modules.report.reports.base_report import ReportBase


def _norm_str(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _parse_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(str(value).strip())
    except Exception:
        return None


def _parse_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = _norm_str(value).lower()
    if s in {"true", "1", "yes", "y", "on"}:
        return True
    if s in {"false", "0", "no", "n", "off"}:
        return False
    return default


def _parse_chr_to_int(chr_value: Any) -> int | None:
    s = _norm_str(chr_value).lower()
    if not s:
        return None

    s = s.replace("chromosome", "").replace("chrom", "").replace("chr", "").strip()
    if s == "x":
        return 23
    if s == "y":
        return 24
    if s in {"m", "mt", "mito", "mitochondria"}:
        return 25

    try:
        v = int(s)
        if 1 <= v <= 25:
            return v
        return None
    except Exception:
        return None


def _format_chr(chromosome: int | None) -> str | None:
    if chromosome is None:
        return None
    if chromosome == 23:
        return "chrX"
    if chromosome == 24:
        return "chrY"
    if chromosome == 25:
        return "chrMT"
    return f"chr{chromosome}"


def _overlap_bp(a_start: int, a_end: int, b_start: int, b_end: int) -> int:
    return max(0, min(a_end, b_end) - max(a_start, b_start) + 1)


def _distance_bp(a_start: int, a_end: int, b_start: int, b_end: int) -> int:
    if _overlap_bp(a_start, a_end, b_start, b_end) > 0:
        return 0
    if a_end < b_start:
        return b_start - a_end
    if a_start > b_end:
        return a_start - b_end
    return 0


class VariantGeneLocationModelReport(ReportBase):
    name = "variant_gene_location_model"
    description = (
        "Maps variants to genes by genomic location overlap using variant_masters "
        "and entity_locations (GRCh38 build). Supports input modes: gene, rsid, "
        "position, region, or mixed auto mode."
    )

    columns = [
        "input_original",
        "input_mode",
        "input_normalized",
        "input_matched_alias",
        "input_entity_id",
        "input_primary_name",
        "input_group_name",
        "input_chromosome",
        "input_start",
        "input_end",
        "variant_id",
        "variant_rsid",
        "variant_chromosome",
        "variant_position_start",
        "variant_position_end",
        "reference_allele",
        "alternate_allele",
        "gene_entity_id",
        "gene_primary_name",
        "gene_group_name",
        "gene_chromosome",
        "gene_start",
        "gene_end",
        "overlap_bp",
        "distance_bp",
        "variants_found",
        "genes_found",
        "observation",
        "note",
    ]

    @classmethod
    def available_columns(cls) -> list[str]:
        return cls.columns

    @classmethod
    def example_input(cls):
        return {
            "input_mode": "auto",
            "input_data": [
                "TP53",
                "rs123456",
                "chr17:7673803",
                "chr17:7673700-7673900",
            ],
            "window_bp": 0,
            "build": 38,
        }

    @classmethod
    def explain(cls) -> str:
        return str("DOC IN MD FILE")
#         return """\
# 🧬 Variant-Gene Location Model
# ==============================

# Purpose:
# - Map variants and genes by interval overlap using:
#   - variant_masters (variant intervals)
#   - entity_locations (gene intervals)

# Input modes:
# - auto (default): detect each item as gene, rsid, position, or region
# - gene: input terms resolved through EntityAlias
# - rsid: rs identifiers in variant_masters.rsid
# - position: chr + single position
# - region: chr + start/end interval

# Main params:
# - input_data (required): list[str|dict] or text-file path
# - input_mode (default auto): auto|gene|rsid|position|region
# - window_bp (default 0): symmetric expansion for overlap checks
# - build (default 38): entity_locations.build filter
# - gene_entity_groups (default ["Gene", "Genes"]): group filter applied to genes
# - limit_variants_per_input (default 2000)
# - emit_not_found_rows (default True)
# """

    # ------------------------------------------------------------------
    # Reflection
    # ------------------------------------------------------------------
    def _table(self, table_name: str) -> Table:
        metadata = MetaData()
        return Table(table_name, metadata, autoload_with=self.db.engine)

    # ------------------------------------------------------------------
    # Input parsing
    # ------------------------------------------------------------------
    def _parse_position_str(self, text: str) -> tuple[int, int] | None:
        s = _norm_str(text)
        m = re.match(r"^(?:chr)?([0-9xyXYmMtT]+)\s*[:]\s*(\d+)$", s)
        if not m:
            return None
        chrom = _parse_chr_to_int(m.group(1))
        pos = _parse_int(m.group(2))
        if chrom is None or pos is None or pos <= 0:
            return None
        return chrom, pos

    def _parse_region_str(self, text: str) -> tuple[int, int, int] | None:
        s = _norm_str(text)
        m = re.match(
            r"^(?:chr)?([0-9xyXYmMtT]+)\s*[:]\s*(\d+)\s*[-:]\s*(\d+)$",
            s,
        )
        if not m:
            return None
        chrom = _parse_chr_to_int(m.group(1))
        start = _parse_int(m.group(2))
        end = _parse_int(m.group(3))
        if chrom is None or start is None or end is None:
            return None
        if start <= 0 or end <= 0:
            return None
        if end < start:
            start, end = end, start
        return chrom, start, end

    def _detect_mode(self, item: Any) -> str:
        if isinstance(item, dict):
            keys = {str(k).lower() for k in item.keys()}
            if {"rsid"} & keys or {"rs"} & keys:
                return "rsid"
            if {"chromosome", "chr", "chrom"} & keys and (
                {"start", "end", "pos_start", "pos_end", "position_start", "position_end"}
                & keys
            ):
                return "region"
            if {"chromosome", "chr", "chrom"} & keys and (
                {"position", "pos"} & keys
            ):
                return "position"
            if {"gene", "entity", "name"} & keys:
                return "gene"
            return "unknown"

        s = _norm_str(item)
        if not s:
            return "unknown"
        if re.match(r"^rs\d+$", s, flags=re.IGNORECASE):
            return "rsid"
        if re.match(r"^(?:chr)?[0-9xyXYmMtT]+\s*:", s):
            # Coordinate-like input. Even when malformed, treat as
            # position/region family so parser can emit invalid_input.
            if "-" in s or s.count(":") >= 2:
                return "region"
            return "position"
        if self._parse_region_str(s):
            return "region"
        if self._parse_position_str(s):
            return "position"
        return "gene"

    def _normalize_one(self, item: Any, forced_mode: str) -> dict[str, Any]:
        mode = forced_mode if forced_mode != "auto" else self._detect_mode(item)
        raw = str(item)

        out = {
            "raw": raw,
            "mode": mode,
            "status": "ok",
            "note": None,
            "gene_key": None,
            "gene_raw": None,
            "rsid_norm": None,
            "chromosome": None,
            "start": None,
            "end": None,
        }

        if mode == "gene":
            if isinstance(item, dict):
                val = item.get("gene") or item.get("entity") or item.get("name")
            else:
                val = item
            s = _norm_str(val)
            if not s:
                out["status"] = "invalid_input"
                out["note"] = "Empty gene input."
                return out
            out["gene_raw"] = s
            out["gene_key"] = s.lower()
            return out

        if mode == "rsid":
            if isinstance(item, dict):
                val = item.get("rsid") or item.get("rs")
            else:
                val = item
            s = _norm_str(val).lower()
            if not s.startswith("rs"):
                out["status"] = "invalid_input"
                out["note"] = "Invalid rsID format."
                return out
            out["rsid_norm"] = s
            return out

        if mode == "position":
            if isinstance(item, dict):
                chrom = item.get("chromosome") or item.get("chr") or item.get("chrom")
                pos = item.get("position") or item.get("pos")
                chrom_i = _parse_chr_to_int(chrom)
                pos_i = _parse_int(pos)
                parsed = (
                    (chrom_i, pos_i)
                    if chrom_i is not None and pos_i is not None and pos_i > 0
                    else None
                )
            else:
                parsed = self._parse_position_str(str(item))

            if not parsed:
                out["status"] = "invalid_input"
                out["note"] = "Invalid position input. Expected chr:pos."
                return out
            chrom_i, pos_i = parsed
            out["chromosome"] = chrom_i
            out["start"] = pos_i
            out["end"] = pos_i
            return out

        if mode == "region":
            if isinstance(item, dict):
                chrom = item.get("chromosome") or item.get("chr") or item.get("chrom")
                start = (
                    item.get("start")
                    or item.get("pos_start")
                    or item.get("position_start")
                )
                end = item.get("end") or item.get("pos_end") or item.get("position_end")
                chrom_i = _parse_chr_to_int(chrom)
                start_i = _parse_int(start)
                end_i = _parse_int(end)
                if (
                    chrom_i is None
                    or start_i is None
                    or end_i is None
                    or start_i <= 0
                    or end_i <= 0
                ):
                    parsed = None
                else:
                    if end_i < start_i:
                        start_i, end_i = end_i, start_i
                    parsed = (chrom_i, start_i, end_i)
            else:
                parsed = self._parse_region_str(str(item))

            if not parsed:
                out["status"] = "invalid_input"
                out["note"] = "Invalid region input. Expected chr:start-end."
                return out
            chrom_i, start_i, end_i = parsed
            out["chromosome"] = chrom_i
            out["start"] = start_i
            out["end"] = end_i
            return out

        out["status"] = "invalid_input"
        out["note"] = "Could not detect input mode."
        return out

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    def _resolve_genes(
        self,
        gene_keys: list[str],
        gene_group_filter: set[str],
        build: int,
    ) -> tuple[dict[str, list[dict[str, Any]]], set[str]]:
        input_key_expr = func.lower(
            func.coalesce(EntityAlias.alias_norm, EntityAlias.alias_value)
        )
        primary_alias = aliased(EntityAlias)

        q = (
            self.session.query(
                input_key_expr.label("gene_key"),
                EntityAlias.alias_value.label("matched_alias"),
                Entity.id.label("entity_id"),
                EntityGroup.name.label("group_name"),
                primary_alias.alias_value.label("primary_name"),
                EntityLocation.chromosome.label("chromosome"),
                EntityLocation.start_pos.label("start_pos"),
                EntityLocation.end_pos.label("end_pos"),
            )
            .join(Entity, Entity.id == EntityAlias.entity_id)
            .join(EntityGroup, Entity.group_id == EntityGroup.id, isouter=True)
            .join(
                primary_alias,
                and_(
                    primary_alias.entity_id == Entity.id,
                    primary_alias.is_primary.is_(True),
                ),
                isouter=True,
            )
            .join(EntityLocation, EntityLocation.entity_id == Entity.id)
            .filter(input_key_expr.in_(gene_keys))
            .filter(EntityLocation.build == int(build))
        )

        if gene_group_filter:
            q = q.filter(func.lower(EntityGroup.name).in_(list(gene_group_filter)))

        rows = q.all()
        by_key: dict[str, list[dict[str, Any]]] = {}
        found: set[str] = set()

        for row in rows:
            found.add(row.gene_key)
            by_key.setdefault(row.gene_key, []).append(
                {
                    "gene_key": row.gene_key,
                    "matched_alias": row.matched_alias,
                    "entity_id": int(row.entity_id),
                    "group_name": row.group_name,
                    "primary_name": row.primary_name,
                    "chromosome": int(row.chromosome),
                    "start_pos": int(row.start_pos),
                    "end_pos": int(row.end_pos),
                }
            )

        return by_key, found

    def _query_variants_overlap(
        self,
        vm: Table,
        chrom: int,
        start: int,
        end: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        stmt = (
            select(
                vm.c.variant_id,
                vm.c.rsid,
                vm.c.chromosome,
                vm.c.position_start,
                vm.c.position_end,
                vm.c.reference_allele,
                vm.c.alternate_allele,
            )
            .where(
                and_(
                    vm.c.chromosome == int(chrom),
                    vm.c.position_start <= int(end),
                    vm.c.position_end >= int(start),
                )
            )
            .order_by(vm.c.position_start.asc(), vm.c.variant_id.asc())
        )
        if limit > 0:
            stmt = stmt.limit(limit)
        rows = self.session.execute(stmt).mappings().all()
        return [dict(r) for r in rows]

    def _query_variants_by_rsid(
        self,
        vm: Table,
        rsids_norm: list[str],
        limit_per_rsid: int,
    ) -> dict[str, list[dict[str, Any]]]:
        if not rsids_norm:
            return {}

        stmt = (
            select(
                vm.c.variant_id,
                vm.c.rsid,
                vm.c.chromosome,
                vm.c.position_start,
                vm.c.position_end,
                vm.c.reference_allele,
                vm.c.alternate_allele,
            )
            .where(func.lower(vm.c.rsid).in_(rsids_norm))
            .order_by(vm.c.rsid.asc(), vm.c.position_start.asc(), vm.c.variant_id.asc())
        )
        rows = self.session.execute(stmt).mappings().all()

        out: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            key = str(row["rsid"]).lower() if row.get("rsid") else ""
            if not key:
                continue
            bucket = out.setdefault(key, [])
            if limit_per_rsid > 0 and len(bucket) >= limit_per_rsid:
                continue
            bucket.append(dict(row))
        return out

    def _chromosome_has_variants(
        self,
        vm: Table,
        chrom: int,
        cache: dict[int, bool],
    ) -> bool:
        chrom_i = int(chrom)
        if chrom_i in cache:
            return cache[chrom_i]

        stmt = (
            select(func.count())
            .select_from(vm)
            .where(vm.c.chromosome == chrom_i)
        )
        count_value = int(self.session.execute(stmt).scalar_one() or 0)
        cache[chrom_i] = count_value > 0
        return cache[chrom_i]

    def _query_genes_overlap(
        self,
        chrom: int,
        start: int,
        end: int,
        build: int,
        gene_group_filter: set[str],
    ) -> list[dict[str, Any]]:
        primary_alias = aliased(EntityAlias)
        q = (
            self.session.query(
                Entity.id.label("entity_id"),
                EntityGroup.name.label("group_name"),
                primary_alias.alias_value.label("primary_name"),
                EntityLocation.chromosome.label("chromosome"),
                EntityLocation.start_pos.label("start_pos"),
                EntityLocation.end_pos.label("end_pos"),
            )
            .join(EntityLocation, EntityLocation.entity_id == Entity.id)
            .join(EntityGroup, Entity.group_id == EntityGroup.id, isouter=True)
            .join(
                primary_alias,
                and_(
                    primary_alias.entity_id == Entity.id,
                    primary_alias.is_primary.is_(True),
                ),
                isouter=True,
            )
            .filter(EntityLocation.build == int(build))
            .filter(EntityLocation.chromosome == int(chrom))
            .filter(EntityLocation.start_pos <= int(end))
            .filter(EntityLocation.end_pos >= int(start))
        )

        if gene_group_filter:
            q = q.filter(func.lower(EntityGroup.name).in_(list(gene_group_filter)))

        rows = q.all()
        out = []
        for row in rows:
            out.append(
                {
                    "entity_id": int(row.entity_id),
                    "group_name": row.group_name,
                    "primary_name": row.primary_name,
                    "chromosome": int(row.chromosome),
                    "start_pos": int(row.start_pos),
                    "end_pos": int(row.end_pos),
                }
            )
        return out

    # ------------------------------------------------------------------
    # Row builders
    # ------------------------------------------------------------------
    def _base_row(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            "input_original": item["raw"],
            "input_mode": item["mode"],
            "input_normalized": item.get("gene_key")
            or item.get("rsid_norm")
            or (
                f"{_format_chr(item['chromosome'])}:{item['start']}:{item['end']}"
                if item.get("chromosome") is not None
                else None
            ),
            "input_matched_alias": None,
            "input_entity_id": None,
            "input_primary_name": None,
            "input_group_name": None,
            "input_chromosome": item.get("chromosome"),
            "input_start": item.get("start"),
            "input_end": item.get("end"),
            "variant_id": None,
            "variant_rsid": None,
            "variant_chromosome": None,
            "variant_position_start": None,
            "variant_position_end": None,
            "reference_allele": None,
            "alternate_allele": None,
            "gene_entity_id": None,
            "gene_primary_name": None,
            "gene_group_name": None,
            "gene_chromosome": None,
            "gene_start": None,
            "gene_end": None,
            "overlap_bp": None,
            "distance_bp": None,
            "variants_found": 0,
            "genes_found": 0,
            "observation": "",
            "note": None,
        }

    @staticmethod
    def _attach_variant(row: dict[str, Any], variant: dict[str, Any]) -> None:
        row.update(
            {
                "variant_id": variant.get("variant_id"),
                "variant_rsid": variant.get("rsid"),
                "variant_chromosome": variant.get("chromosome"),
                "variant_position_start": variant.get("position_start"),
                "variant_position_end": variant.get("position_end"),
                "reference_allele": variant.get("reference_allele"),
                "alternate_allele": variant.get("alternate_allele"),
            }
        )

    @staticmethod
    def _attach_gene(row: dict[str, Any], gene: dict[str, Any]) -> None:
        row.update(
            {
                "gene_entity_id": gene.get("entity_id"),
                "gene_primary_name": gene.get("primary_name"),
                "gene_group_name": gene.get("group_name"),
                "gene_chromosome": gene.get("chromosome"),
                "gene_start": gene.get("start_pos"),
                "gene_end": gene.get("end_pos"),
            }
        )

    # ------------------------------------------------------------------
    # Main
    # ------------------------------------------------------------------
    def run(self):
        input_data_raw = self.param("input_data", required=True)
        input_data = self.resolve_input_list(input_data_raw, param_name="input_data")

        forced_mode = str(self.param("input_mode", "auto")).strip().lower()
        if forced_mode not in {"auto", "gene", "rsid", "position", "region"}:
            raise ValueError(
                "input_mode must be one of: auto, gene, rsid, position, region."
            )

        window_bp = max(0, int(self.param("window_bp", 0) or 0))
        build = int(self.param("build", 38) or 38)
        limit_variants_per_input = max(
            1, int(self.param("limit_variants_per_input", 2000) or 2000)
        )
        emit_not_found_rows = _parse_bool(
            self.param("emit_not_found_rows", True), default=True
        )
        gene_group_filter = {
            str(x).strip().lower()
            for x in (self.param("gene_entity_groups", ["Gene", "Genes"]) or [])
            if str(x).strip()
        }

        vm = self._table("variant_masters")
        normalized_inputs = [self._normalize_one(item, forced_mode) for item in input_data]

        rows_out: list[dict[str, Any]] = []

        # Preload gene-mode alias resolution in one query.
        gene_keys = sorted(
            {x["gene_key"] for x in normalized_inputs if x["status"] == "ok" and x["mode"] == "gene"}  # noqa E501
        )
        gene_hits_by_key, found_gene_keys = self._resolve_genes(
            gene_keys=gene_keys,
            gene_group_filter=gene_group_filter,
            build=build,
        )

        # Preload rsid resolution in one query.
        rsid_keys = sorted(
            {x["rsid_norm"] for x in normalized_inputs if x["status"] == "ok" and x["mode"] == "rsid"}  # noqa E501
        )
        variants_by_rsid = self._query_variants_by_rsid(
            vm=vm,
            rsids_norm=rsid_keys,
            limit_per_rsid=limit_variants_per_input,
        )

        chromosome_variant_cache: dict[int, bool] = {}

        # Cache gene-overlap lookups for variants/position/region workflows.
        gene_overlap_cache: dict[tuple[int, int, int], list[dict[str, Any]]] = {}

        for item in normalized_inputs:
            base = self._base_row(item)

            if item["status"] != "ok":
                base["observation"] = "invalid_input"
                base["note"] = item["note"]
                rows_out.append(base)
                continue

            item_rows: list[dict[str, Any]] = []

            # --------------------------------------------------------------
            # Gene input -> resolve genes first, then overlap variants
            # --------------------------------------------------------------
            if item["mode"] == "gene":
                key = item["gene_key"]
                genes = gene_hits_by_key.get(key, [])
                if not genes:
                    if emit_not_found_rows:
                        miss = dict(base)
                        miss["observation"] = "not found"
                        miss["note"] = (
                            "Gene alias not resolved."
                            if key not in found_gene_keys
                            else "No gene location matched filters/build."
                        )
                        item_rows.append(miss)
                else:
                    for gene in genes:
                        gstart = max(1, int(gene["start_pos"]) - window_bp)
                        gend = int(gene["end_pos"]) + window_bp
                        variants = self._query_variants_overlap(
                            vm=vm,
                            chrom=int(gene["chromosome"]),
                            start=gstart,
                            end=gend,
                            limit=limit_variants_per_input,
                        )
                        if not variants and emit_not_found_rows:
                            miss = dict(base)
                            miss["input_matched_alias"] = gene["matched_alias"]
                            miss["input_entity_id"] = gene["entity_id"]
                            miss["input_primary_name"] = gene["primary_name"]
                            miss["input_group_name"] = gene["group_name"]
                            self._attach_gene(miss, gene)
                            miss["input_chromosome"] = gene["chromosome"]
                            miss["input_start"] = gene["start_pos"]
                            miss["input_end"] = gene["end_pos"]
                            miss["observation"] = "not found"
                            if self._chromosome_has_variants(
                                vm=vm,
                                chrom=int(gene["chromosome"]),
                                cache=chromosome_variant_cache,
                            ):
                                miss["note"] = "No variants found for gene interval."
                            else:
                                miss["note"] = (
                                    "No variants found for gene interval. "
                                    f"variant_masters has no rows for chromosome {_format_chr(int(gene['chromosome']))}."  # noqa: E501
                                )
                            item_rows.append(miss)
                            continue

                        for variant in variants:
                            row = dict(base)
                            row["input_matched_alias"] = gene["matched_alias"]
                            row["input_entity_id"] = gene["entity_id"]
                            row["input_primary_name"] = gene["primary_name"]
                            row["input_group_name"] = gene["group_name"]
                            self._attach_gene(row, gene)
                            self._attach_variant(row, variant)
                            row["input_chromosome"] = gene["chromosome"]
                            row["input_start"] = gene["start_pos"]
                            row["input_end"] = gene["end_pos"]
                            row["overlap_bp"] = _overlap_bp(
                                int(variant["position_start"]),
                                int(variant["position_end"]),
                                int(gene["start_pos"]),
                                int(gene["end_pos"]),
                            )
                            row["distance_bp"] = _distance_bp(
                                int(variant["position_start"]),
                                int(variant["position_end"]),
                                int(gene["start_pos"]),
                                int(gene["end_pos"]),
                            )
                            row["observation"] = "ok"
                            item_rows.append(row)

            # --------------------------------------------------------------
            # RSID input -> resolve variants first, then overlap genes
            # --------------------------------------------------------------
            elif item["mode"] == "rsid":
                rsid_key = item["rsid_norm"]
                variants = variants_by_rsid.get(rsid_key, [])
                if not variants:
                    if emit_not_found_rows:
                        miss = dict(base)
                        miss["observation"] = "not found"
                        miss["note"] = "No variants found for rsID."
                        item_rows.append(miss)
                else:
                    for variant in variants:
                        vstart = max(1, int(variant["position_start"]) - window_bp)
                        vend = int(variant["position_end"]) + window_bp
                        cache_key = (int(variant["chromosome"]), int(vstart), int(vend))
                        genes = gene_overlap_cache.get(cache_key)
                        if genes is None:
                            genes = self._query_genes_overlap(
                                chrom=int(variant["chromosome"]),
                                start=vstart,
                                end=vend,
                                build=build,
                                gene_group_filter=gene_group_filter,
                            )
                            gene_overlap_cache[cache_key] = genes

                        if not genes:
                            row = dict(base)
                            self._attach_variant(row, variant)
                            row["input_chromosome"] = variant["chromosome"]
                            row["input_start"] = variant["position_start"]
                            row["input_end"] = variant["position_end"]
                            row["observation"] = "no_gene_match"
                            row["note"] = "Variant found but no overlapping genes."
                            item_rows.append(row)
                            continue

                        for gene in genes:
                            row = dict(base)
                            self._attach_variant(row, variant)
                            self._attach_gene(row, gene)
                            row["input_chromosome"] = variant["chromosome"]
                            row["input_start"] = variant["position_start"]
                            row["input_end"] = variant["position_end"]
                            row["overlap_bp"] = _overlap_bp(
                                int(variant["position_start"]),
                                int(variant["position_end"]),
                                int(gene["start_pos"]),
                                int(gene["end_pos"]),
                            )
                            row["distance_bp"] = _distance_bp(
                                int(variant["position_start"]),
                                int(variant["position_end"]),
                                int(gene["start_pos"]),
                                int(gene["end_pos"]),
                            )
                            row["observation"] = "ok"
                            item_rows.append(row)

            # --------------------------------------------------------------
            # Position / Region input -> overlap variants then genes
            # --------------------------------------------------------------
            else:
                chrom = int(item["chromosome"])
                start = max(1, int(item["start"]) - window_bp)
                end = int(item["end"]) + window_bp

                variants = self._query_variants_overlap(
                    vm=vm,
                    chrom=chrom,
                    start=start,
                    end=end,
                    limit=limit_variants_per_input,
                )

                if not variants:
                    if emit_not_found_rows:
                        miss = dict(base)
                        miss["observation"] = "not found"
                        if self._chromosome_has_variants(
                            vm=vm,
                            chrom=chrom,
                            cache=chromosome_variant_cache,
                        ):
                            miss["note"] = "No variants found in region."
                        else:
                            miss["note"] = (
                                "No variants found in region. "
                                f"variant_masters has no rows for chromosome {_format_chr(chrom)}."  # noqa: E501
                            )
                        item_rows.append(miss)
                else:
                    for variant in variants:
                        vstart = max(1, int(variant["position_start"]) - window_bp)
                        vend = int(variant["position_end"]) + window_bp
                        cache_key = (int(variant["chromosome"]), int(vstart), int(vend))
                        genes = gene_overlap_cache.get(cache_key)
                        if genes is None:
                            genes = self._query_genes_overlap(
                                chrom=int(variant["chromosome"]),
                                start=vstart,
                                end=vend,
                                build=build,
                                gene_group_filter=gene_group_filter,
                            )
                            gene_overlap_cache[cache_key] = genes

                        if not genes:
                            row = dict(base)
                            self._attach_variant(row, variant)
                            row["observation"] = "no_gene_match"
                            row["note"] = "Variant found but no overlapping genes."
                            item_rows.append(row)
                            continue

                        for gene in genes:
                            row = dict(base)
                            self._attach_variant(row, variant)
                            self._attach_gene(row, gene)
                            row["overlap_bp"] = _overlap_bp(
                                int(variant["position_start"]),
                                int(variant["position_end"]),
                                int(gene["start_pos"]),
                                int(gene["end_pos"]),
                            )
                            row["distance_bp"] = _distance_bp(
                                int(variant["position_start"]),
                                int(variant["position_end"]),
                                int(gene["start_pos"]),
                                int(gene["end_pos"]),
                            )
                            row["observation"] = "ok"
                            item_rows.append(row)

            # --------------------------------------------------------------
            # Per-input counters
            # --------------------------------------------------------------
            variant_ids = {
                r["variant_id"] for r in item_rows if r.get("variant_id") is not None
            }
            gene_ids = {
                r["gene_entity_id"]
                for r in item_rows
                if r.get("gene_entity_id") is not None
            }
            for row in item_rows:
                row["variants_found"] = len(variant_ids)
                row["genes_found"] = len(gene_ids)

            rows_out.extend(item_rows if item_rows else [base])

        df = pd.DataFrame(rows_out)
        if not df.empty:
            df = df.sort_values(
                by=[
                    "input_mode",
                    "input_original",
                    "variant_chromosome",
                    "variant_position_start",
                    "gene_primary_name",
                ],
                na_position="last",
            )
        df = df.reindex(columns=self.columns)
        self.results = df
        return df.reset_index(drop=True)
