from __future__ import annotations

import re
from collections import OrderedDict
from itertools import combinations
from typing import Any

import pandas as pd
from sqlalchemy import MetaData, Table, and_, func, select
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import (
    Entity,
    EntityAlias,
    EntityGroup,
    EntityLocation,
    EntityRelationship,
    EntityRelationshipType,
)
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


def _as_ci_set(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, (list, tuple, set)):
        seq = value
    else:
        seq = [value]

    out: set[str] = set()
    for item in seq:
        s = _norm_str(item)
        if not s:
            continue
        if "," in s:
            parts = [x.strip() for x in s.split(",") if x.strip()]
            out.update({p.lower() for p in parts})
        else:
            out.add(s.lower())
    return out


def _as_list_ci_ordered(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        seq = value
    else:
        seq = [value]

    out: OrderedDict[str, str] = OrderedDict()
    for item in seq:
        s = _norm_str(item)
        if not s:
            continue
        if "," in s:
            parts = [x.strip() for x in s.split(",") if x.strip()]
        else:
            parts = [s]
        for part in parts:
            key = part.lower()
            if key not in out:
                out[key] = part
    return list(out.keys())


def _seed_scope(seed_count: int) -> str:
    if seed_count >= 2:
        return "both_from_seed"
    if seed_count == 1:
        return "one_from_seed"
    return "none_from_seed"


def _scope_keep(scope: str, seed_count: int) -> bool:
    if scope == "any_expanded":
        return True
    if scope == "at_least_one_from_seed":
        return seed_count >= 1
    if scope == "both_from_seed":
        return seed_count == 2
    if scope == "one_from_seed":
        return seed_count == 1
    return False


class SNPSNPModelReport(ReportBase):
    name = "snp_snp_model"
    description = (
        "Builds BF4 gene-gene and SNP-SNP candidate models from seed genomic positions "
        "using variant_masters + entity_locations, then expands through biological "
        "group relationships (for example pathways)."
    )

    columns = [
        "row_type",
        "observation",
        "note",
        "input_original",
        "input_chromosome",
        "input_position",
        "seed_variant_id",
        "seed_variant_rsid",
        "seed_variant_chromosome",
        "seed_variant_start",
        "seed_variant_end",
        "seed_gene_id",
        "seed_gene_name",
        "gene_1_id",
        "gene_1_name",
        "gene_2_id",
        "gene_2_name",
        "gene_pair_seed_scope",
        "gene_pair_seed_count",
        "variant_1_id",
        "variant_1_rsid",
        "variant_1_chromosome",
        "variant_1_start",
        "variant_1_end",
        "variant_2_id",
        "variant_2_rsid",
        "variant_2_chromosome",
        "variant_2_start",
        "variant_2_end",
        "snp_pair_seed_scope",
        "snp_pair_seed_count",
        "group_support_count",
        "group_support_ids",
        "group_support_names",
        "relationship_types_used",
        "build",
        "window_bp",
        "input_positions_count",
        "seed_variants_count",
        "seed_genes_count",
        "selected_groups_count",
        "expanded_genes_count",
    ]

    @classmethod
    def available_columns(cls) -> list[str]:
        return cls.columns

    @classmethod
    def example_input(cls):
        return {
            "input_data": ["chr17:150", "chr17:280"],
            "build": 38,
            "window_bp": 0,
            "group_entity_groups": ["Pathway"],
            "relationship_types": ["in_pathway"],
            "gene_pair_scope": "at_least_one_from_seed",
            "snp_pair_scope": "at_least_one_from_seed",
        }

    @classmethod
    def explain(cls) -> str:
        return str("DOC IN MD FILE")

    @staticmethod
    def _parse_scope(value: Any, param_name: str) -> str:
        scope = _norm_str(value or "at_least_one_from_seed").lower()
        valid = {
            "both_from_seed",
            "one_from_seed",
            "at_least_one_from_seed",
            "any_expanded",
        }
        if scope not in valid:
            raise ValueError(
                f"{param_name} must be one of: both_from_seed, one_from_seed, "
                "at_least_one_from_seed, any_expanded."
            )
        return scope

    def _table(self, table_name: str) -> Table:
        metadata = MetaData()
        return Table(table_name, metadata, autoload_with=self.db.engine)

    def _parse_position_input(self, item: Any) -> dict[str, Any]:
        out = {
            "raw": str(item),
            "status": "ok",
            "note": None,
            "chromosome": None,
            "position": None,
        }

        if isinstance(item, dict):
            chrom = item.get("chromosome") or item.get("chr") or item.get("chrom")
            pos = item.get("position") or item.get("pos")
            chrom_i = _parse_chr_to_int(chrom)
            pos_i = _parse_int(pos)
            if chrom_i is None or pos_i is None or pos_i <= 0:
                out["status"] = "invalid_input"
                out["note"] = "Invalid dictionary position input."
                return out
            out["chromosome"] = chrom_i
            out["position"] = pos_i
            out["raw"] = f"{_format_chr(chrom_i)}:{pos_i}"
            return out

        s = _norm_str(item)
        out["raw"] = s
        if not s:
            out["status"] = "invalid_input"
            out["note"] = "Empty position input."
            return out

        m = re.match(r"^(?:chr)?([0-9xyXYmMtT]+)\s*[:;, ]\s*(\d+)$", s)
        if not m:
            out["status"] = "invalid_input"
            out["note"] = "Expected format chr:position."
            return out

        chrom_i = _parse_chr_to_int(m.group(1))
        pos_i = _parse_int(m.group(2))
        if chrom_i is None or pos_i is None or pos_i <= 0:
            out["status"] = "invalid_input"
            out["note"] = "Invalid chromosome or position."
            return out

        out["chromosome"] = chrom_i
        out["position"] = pos_i
        out["raw"] = f"{_format_chr(chrom_i)}:{pos_i}"
        return out

    def _query_variants_at_position(
        self,
        vm: Table,
        chrom: int,
        pos: int,
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
                    vm.c.position_start <= int(pos),
                    vm.c.position_end >= int(pos),
                )
            )
            .order_by(vm.c.position_start.asc(), vm.c.variant_id.asc())
        )
        rows = self.session.execute(stmt).mappings().all()
        return [dict(row) for row in rows]

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
        return [dict(row) for row in rows]

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
        out: list[dict[str, Any]] = []
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

    def _resolve_entities_by_alias(
        self,
        alias_keys: list[str],
        group_filter: set[str],
    ) -> tuple[set[int], set[str]]:
        if not alias_keys:
            return set(), set()

        key_expr = func.lower(func.coalesce(EntityAlias.alias_norm, EntityAlias.alias_value))

        q = (
            self.session.query(
                key_expr.label("input_key"),
                Entity.id.label("entity_id"),
            )
            .join(Entity, Entity.id == EntityAlias.entity_id)
            .join(EntityGroup, Entity.group_id == EntityGroup.id, isouter=True)
            .filter(key_expr.in_(alias_keys))
        )
        if group_filter:
            q = q.filter(func.lower(EntityGroup.name).in_(list(group_filter)))

        rows = q.all()
        ids: set[int] = set()
        found: set[str] = set()
        for row in rows:
            ids.add(int(row.entity_id))
            found.add(str(row.input_key))
        return ids, found

    def _query_seed_gene_to_groups(
        self,
        seed_gene_ids: set[int],
        relationship_type_filter: set[str],
        gene_group_filter: set[str],
        group_group_filter: set[str],
        group_entity_ids_filter: set[int] | None,
    ) -> list[dict[str, Any]]:
        if not seed_gene_ids:
            return []

        rt = aliased(EntityRelationshipType)
        e1 = aliased(Entity)
        e2 = aliased(Entity)
        eg1 = aliased(EntityGroup)
        eg2 = aliased(EntityGroup)
        rows_out: list[dict[str, Any]] = []

        q1 = (
            self.session.query(
                EntityRelationship.id.label("relationship_id"),
                rt.code.label("relationship_type"),
                EntityRelationship.entity_1_id.label("gene_id"),
                EntityRelationship.entity_2_id.label("group_id"),
            )
            .join(rt, rt.id == EntityRelationship.relationship_type_id)
            .join(e1, e1.id == EntityRelationship.entity_1_id)
            .join(eg1, eg1.id == e1.group_id, isouter=True)
            .join(e2, e2.id == EntityRelationship.entity_2_id)
            .join(eg2, eg2.id == e2.group_id, isouter=True)
            .filter(EntityRelationship.entity_1_id.in_(list(seed_gene_ids)))
        )

        if relationship_type_filter:
            q1 = q1.filter(func.lower(rt.code).in_(list(relationship_type_filter)))
        if gene_group_filter:
            q1 = q1.filter(func.lower(eg1.name).in_(list(gene_group_filter)))
        if group_group_filter:
            q1 = q1.filter(func.lower(eg2.name).in_(list(group_group_filter)))
        if group_entity_ids_filter is not None:
            q1 = q1.filter(EntityRelationship.entity_2_id.in_(list(group_entity_ids_filter)))

        for row in q1.all():
            rows_out.append(
                {
                    "relationship_id": int(row.relationship_id),
                    "relationship_type": row.relationship_type,
                    "gene_id": int(row.gene_id),
                    "group_id": int(row.group_id),
                }
            )

        q2 = (
            self.session.query(
                EntityRelationship.id.label("relationship_id"),
                rt.code.label("relationship_type"),
                EntityRelationship.entity_2_id.label("gene_id"),
                EntityRelationship.entity_1_id.label("group_id"),
            )
            .join(rt, rt.id == EntityRelationship.relationship_type_id)
            .join(e1, e1.id == EntityRelationship.entity_2_id)
            .join(eg1, eg1.id == e1.group_id, isouter=True)
            .join(e2, e2.id == EntityRelationship.entity_1_id)
            .join(eg2, eg2.id == e2.group_id, isouter=True)
            .filter(EntityRelationship.entity_2_id.in_(list(seed_gene_ids)))
        )

        if relationship_type_filter:
            q2 = q2.filter(func.lower(rt.code).in_(list(relationship_type_filter)))
        if gene_group_filter:
            q2 = q2.filter(func.lower(eg1.name).in_(list(gene_group_filter)))
        if group_group_filter:
            q2 = q2.filter(func.lower(eg2.name).in_(list(group_group_filter)))
        if group_entity_ids_filter is not None:
            q2 = q2.filter(EntityRelationship.entity_1_id.in_(list(group_entity_ids_filter)))

        for row in q2.all():
            rows_out.append(
                {
                    "relationship_id": int(row.relationship_id),
                    "relationship_type": row.relationship_type,
                    "gene_id": int(row.gene_id),
                    "group_id": int(row.group_id),
                }
            )

        return rows_out

    def _query_groups_to_genes(
        self,
        group_ids: set[int],
        relationship_type_filter: set[str],
        gene_group_filter: set[str],
        group_group_filter: set[str],
    ) -> list[dict[str, Any]]:
        if not group_ids:
            return []

        rt = aliased(EntityRelationshipType)
        e1 = aliased(Entity)
        e2 = aliased(Entity)
        eg1 = aliased(EntityGroup)
        eg2 = aliased(EntityGroup)
        rows_out: list[dict[str, Any]] = []

        q1 = (
            self.session.query(
                EntityRelationship.id.label("relationship_id"),
                rt.code.label("relationship_type"),
                EntityRelationship.entity_1_id.label("group_id"),
                EntityRelationship.entity_2_id.label("gene_id"),
            )
            .join(rt, rt.id == EntityRelationship.relationship_type_id)
            .join(e1, e1.id == EntityRelationship.entity_1_id)
            .join(eg1, eg1.id == e1.group_id, isouter=True)
            .join(e2, e2.id == EntityRelationship.entity_2_id)
            .join(eg2, eg2.id == e2.group_id, isouter=True)
            .filter(EntityRelationship.entity_1_id.in_(list(group_ids)))
        )

        if relationship_type_filter:
            q1 = q1.filter(func.lower(rt.code).in_(list(relationship_type_filter)))
        if group_group_filter:
            q1 = q1.filter(func.lower(eg1.name).in_(list(group_group_filter)))
        if gene_group_filter:
            q1 = q1.filter(func.lower(eg2.name).in_(list(gene_group_filter)))

        for row in q1.all():
            rows_out.append(
                {
                    "relationship_id": int(row.relationship_id),
                    "relationship_type": row.relationship_type,
                    "group_id": int(row.group_id),
                    "gene_id": int(row.gene_id),
                }
            )

        q2 = (
            self.session.query(
                EntityRelationship.id.label("relationship_id"),
                rt.code.label("relationship_type"),
                EntityRelationship.entity_2_id.label("group_id"),
                EntityRelationship.entity_1_id.label("gene_id"),
            )
            .join(rt, rt.id == EntityRelationship.relationship_type_id)
            .join(e1, e1.id == EntityRelationship.entity_2_id)
            .join(eg1, eg1.id == e1.group_id, isouter=True)
            .join(e2, e2.id == EntityRelationship.entity_1_id)
            .join(eg2, eg2.id == e2.group_id, isouter=True)
            .filter(EntityRelationship.entity_2_id.in_(list(group_ids)))
        )

        if relationship_type_filter:
            q2 = q2.filter(func.lower(rt.code).in_(list(relationship_type_filter)))
        if group_group_filter:
            q2 = q2.filter(func.lower(eg1.name).in_(list(group_group_filter)))
        if gene_group_filter:
            q2 = q2.filter(func.lower(eg2.name).in_(list(gene_group_filter)))

        for row in q2.all():
            rows_out.append(
                {
                    "relationship_id": int(row.relationship_id),
                    "relationship_type": row.relationship_type,
                    "group_id": int(row.group_id),
                    "gene_id": int(row.gene_id),
                }
            )

        return rows_out

    def _resolve_entity_metadata(self, entity_ids: set[int]) -> dict[int, dict[str, Any]]:
        if not entity_ids:
            return {}

        primary_alias = aliased(EntityAlias)
        q = (
            self.session.query(
                Entity.id.label("entity_id"),
                EntityGroup.name.label("group_name"),
                primary_alias.alias_value.label("primary_name"),
            )
            .join(EntityGroup, Entity.group_id == EntityGroup.id, isouter=True)
            .join(
                primary_alias,
                and_(
                    primary_alias.entity_id == Entity.id,
                    primary_alias.is_primary.is_(True),
                ),
                isouter=True,
            )
            .filter(Entity.id.in_(list(entity_ids)))
        )

        out: dict[int, dict[str, Any]] = {}
        for row in q.all():
            out[int(row.entity_id)] = {
                "primary_name": row.primary_name,
                "group_name": row.group_name,
            }
        return out

    def _query_gene_locations(
        self,
        gene_ids: set[int],
        build: int,
        gene_group_filter: set[str],
    ) -> list[dict[str, Any]]:
        if not gene_ids:
            return []

        q = (
            self.session.query(
                EntityLocation.entity_id.label("entity_id"),
                EntityLocation.chromosome.label("chromosome"),
                EntityLocation.start_pos.label("start_pos"),
                EntityLocation.end_pos.label("end_pos"),
            )
            .join(Entity, Entity.id == EntityLocation.entity_id)
            .join(EntityGroup, Entity.group_id == EntityGroup.id, isouter=True)
            .filter(EntityLocation.entity_id.in_(list(gene_ids)))
            .filter(EntityLocation.build == int(build))
        )

        if gene_group_filter:
            q = q.filter(func.lower(EntityGroup.name).in_(list(gene_group_filter)))

        rows = q.all()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "entity_id": int(row.entity_id),
                    "chromosome": int(row.chromosome),
                    "start_pos": int(row.start_pos),
                    "end_pos": int(row.end_pos),
                }
            )
        return out

    def _base_row(self) -> dict[str, Any]:
        return {column: None for column in self.columns}

    def run(self):
        input_data_raw = self.param("input_data", required=True)
        input_data = self.resolve_input_list(input_data_raw, param_name="input_data")

        build = int(self.param("build", 38) or 38)
        window_bp = max(0, int(self.param("window_bp", 0) or 0))
        emit_not_found_rows = _parse_bool(self.param("emit_not_found_rows", True), True)
        include_gene_pairs = _parse_bool(self.param("include_gene_pairs", True), True)
        include_snp_pairs = _parse_bool(self.param("include_snp_pairs", True), True)
        expand_variants_from_expanded_genes = _parse_bool(
            self.param("expand_variants_from_expanded_genes", True),
            True,
        )
        limit_variants_per_gene = max(
            1,
            int(self.param("limit_variants_per_gene", 2000) or 2000),
        )
        max_snp_pairs = max(
            0,
            int(self.param("max_snp_pairs", 200000) or 200000),
        )

        gene_pair_scope = self._parse_scope(
            self.param("gene_pair_scope", "at_least_one_from_seed"),
            "gene_pair_scope",
        )
        snp_pair_scope = self._parse_scope(
            self.param("snp_pair_scope", "at_least_one_from_seed"),
            "snp_pair_scope",
        )

        gene_group_filter = _as_ci_set(self.param("gene_entity_groups", ["Gene", "Genes"]))
        group_group_filter = _as_ci_set(
            self.param("group_entity_groups", ["Pathway", "Pathways"])
        )
        relationship_type_filter = _as_ci_set(self.param("relationship_types", ["in_pathway"]))

        group_entities_keys = _as_list_ci_ordered(self.param("group_entities", None))

        vm = self._table("variant_masters")

        normalized_inputs = [self._parse_position_input(item) for item in input_data]
        valid_inputs = [item for item in normalized_inputs if item["status"] == "ok"]

        rows_out: list[dict[str, Any]] = []

        for item in normalized_inputs:
            if item["status"] == "ok":
                continue
            row = self._base_row()
            row["row_type"] = "input"
            row["observation"] = "invalid_input"
            row["note"] = item["note"]
            row["input_original"] = item["raw"]
            row["input_chromosome"] = item.get("chromosome")
            row["input_position"] = item.get("position")
            row["build"] = build
            row["window_bp"] = window_bp
            rows_out.append(row)

        if not valid_inputs:
            df = pd.DataFrame(rows_out).reindex(columns=self.columns)
            self.results = df
            return df.reset_index(drop=True)

        # ------------------------------------------------------------------
        # Step 1: seed variants from input chr:position
        # ------------------------------------------------------------------
        seed_variants_by_id: dict[int, dict[str, Any]] = {}
        for item in valid_inputs:
            variants = self._query_variants_at_position(
                vm=vm,
                chrom=int(item["chromosome"]),
                pos=int(item["position"]),
            )
            if not variants and emit_not_found_rows:
                row = self._base_row()
                row["row_type"] = "input"
                row["observation"] = "not_found"
                row["note"] = "No variants found at position."
                row["input_original"] = item["raw"]
                row["input_chromosome"] = item["chromosome"]
                row["input_position"] = item["position"]
                row["build"] = build
                row["window_bp"] = window_bp
                rows_out.append(row)
            for variant in variants:
                seed_variants_by_id[int(variant["variant_id"])] = variant

        if not seed_variants_by_id:
            df = pd.DataFrame(rows_out).reindex(columns=self.columns)
            self.results = df
            return df.reset_index(drop=True)

        # ------------------------------------------------------------------
        # Step 2: seed variant -> seed genes via entity_locations overlap
        # ------------------------------------------------------------------
        seed_variant_ids = set(seed_variants_by_id.keys())
        seed_gene_ids: set[int] = set()
        gene_overlap_cache: dict[tuple[int, int, int], list[dict[str, Any]]] = {}

        for variant_id, variant in seed_variants_by_id.items():
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

            gene_ids = {int(g["entity_id"]) for g in genes}
            seed_gene_ids.update(gene_ids)

        if not seed_gene_ids:
            if emit_not_found_rows:
                row = self._base_row()
                row["row_type"] = "summary"
                row["observation"] = "no_gene_match"
                row["note"] = "Seed variants found, but no genes overlapped in entity_locations."
                row["seed_variants_count"] = len(seed_variant_ids)
                row["seed_genes_count"] = 0
                row["build"] = build
                row["window_bp"] = window_bp
                rows_out.append(row)
            df = pd.DataFrame(rows_out).reindex(columns=self.columns)
            self.results = df
            return df.reset_index(drop=True)

        # ------------------------------------------------------------------
        # Step 3: seed genes -> biological groups (pathways, etc.)
        # ------------------------------------------------------------------
        group_entity_ids_filter: set[int] | None = None
        if group_entities_keys:
            resolved_ids, found_keys = self._resolve_entities_by_alias(
                alias_keys=group_entities_keys,
                group_filter=group_group_filter,
            )
            group_entity_ids_filter = resolved_ids
            if emit_not_found_rows:
                missing = [k for k in group_entities_keys if k not in found_keys]
                for key in missing:
                    row = self._base_row()
                    row["row_type"] = "input"
                    row["observation"] = "not_found"
                    row["note"] = f"Group entity not resolved: {key}"
                    row["input_original"] = key
                    row["build"] = build
                    row["window_bp"] = window_bp
                    rows_out.append(row)

        seed_group_links = self._query_seed_gene_to_groups(
            seed_gene_ids=seed_gene_ids,
            relationship_type_filter=relationship_type_filter,
            gene_group_filter=gene_group_filter,
            group_group_filter=group_group_filter,
            group_entity_ids_filter=group_entity_ids_filter,
        )

        selected_group_ids: set[int] = {int(x["group_id"]) for x in seed_group_links}

        # ------------------------------------------------------------------
        # Step 4: selected groups -> expanded genes
        # ------------------------------------------------------------------
        group_gene_links = self._query_groups_to_genes(
            group_ids=selected_group_ids,
            relationship_type_filter=relationship_type_filter,
            gene_group_filter=gene_group_filter,
            group_group_filter=group_group_filter,
        )

        group_to_gene_ids: dict[int, set[int]] = {}
        for link in seed_group_links + group_gene_links:
            gid = int(link["group_id"])
            egid = int(link["gene_id"])
            group_to_gene_ids.setdefault(gid, set()).add(egid)

        expanded_gene_ids: set[int] = set(seed_gene_ids)
        for members in group_to_gene_ids.values():
            expanded_gene_ids.update(members)

        metadata_map = self._resolve_entity_metadata(expanded_gene_ids | selected_group_ids)

        def _entity_name(entity_id: int) -> str:
            meta = metadata_map.get(int(entity_id), {})
            return _norm_str(meta.get("primary_name")) or str(entity_id)

        # ------------------------------------------------------------------
        # Step 5: gene-gene from co-membership in selected groups
        # ------------------------------------------------------------------
        pair_to_group_ids: dict[tuple[int, int], set[int]] = {}
        for group_id, members in group_to_gene_ids.items():
            members_sorted = sorted(members)
            for gene_1_id, gene_2_id in combinations(members_sorted, 2):
                pair_to_group_ids.setdefault((gene_1_id, gene_2_id), set()).add(int(group_id))

        relationship_types_used = (
            ",".join(sorted(relationship_type_filter)) if relationship_type_filter else "any"
        )

        stats = {
            "build": build,
            "window_bp": window_bp,
            "input_positions_count": len(valid_inputs),
            "seed_variants_count": len(seed_variant_ids),
            "seed_genes_count": len(seed_gene_ids),
            "selected_groups_count": len(selected_group_ids),
            "expanded_genes_count": len(expanded_gene_ids),
            "relationship_types_used": relationship_types_used,
        }

        gene_pair_models: list[dict[str, Any]] = []

        for (gene_1_id, gene_2_id), support_groups in sorted(pair_to_group_ids.items()):
            seed_count = int(gene_1_id in seed_gene_ids) + int(gene_2_id in seed_gene_ids)
            if not _scope_keep(gene_pair_scope, seed_count):
                continue

            gene_pair_scope_value = _seed_scope(seed_count)
            group_ids_sorted = sorted(support_groups)
            group_names = [
                _entity_name(group_id)
                for group_id in group_ids_sorted
            ]

            model = {
                "gene_1_id": int(gene_1_id),
                "gene_2_id": int(gene_2_id),
                "gene_pair_seed_count": seed_count,
                "gene_pair_seed_scope": gene_pair_scope_value,
                "group_ids": group_ids_sorted,
                "group_names": group_names,
            }
            gene_pair_models.append(model)

            if include_gene_pairs:
                row = self._base_row()
                row.update(stats)
                row["row_type"] = "gene_pair"
                row["observation"] = "ok"
                row["gene_1_id"] = int(gene_1_id)
                row["gene_1_name"] = _entity_name(gene_1_id)
                row["gene_2_id"] = int(gene_2_id)
                row["gene_2_name"] = _entity_name(gene_2_id)
                row["gene_pair_seed_scope"] = gene_pair_scope_value
                row["gene_pair_seed_count"] = seed_count
                row["group_support_count"] = len(group_ids_sorted)
                row["group_support_ids"] = "|".join(str(x) for x in group_ids_sorted)
                row["group_support_names"] = "|".join(group_names)
                rows_out.append(row)

        # ------------------------------------------------------------------
        # Step 6: expand variants for genes and build SNP-SNP per gene pair
        # ------------------------------------------------------------------
        snp_pairs_truncated = False
        if include_snp_pairs and gene_pair_models:
            genes_for_variant_expansion = (
                set(expanded_gene_ids) if expand_variants_from_expanded_genes else set(seed_gene_ids)
            )

            gene_locations = self._query_gene_locations(
                gene_ids=genes_for_variant_expansion,
                build=build,
                gene_group_filter=gene_group_filter,
            )

            gene_to_variants: dict[int, list[dict[str, Any]]] = {}
            overlap_cache: dict[tuple[int, int, int], list[dict[str, Any]]] = {}

            for gloc in gene_locations:
                chrom = int(gloc["chromosome"])
                start = max(1, int(gloc["start_pos"]) - window_bp)
                end = int(gloc["end_pos"]) + window_bp
                cache_key = (chrom, start, end)
                variants = overlap_cache.get(cache_key)
                if variants is None:
                    variants = self._query_variants_overlap(
                        vm=vm,
                        chrom=chrom,
                        start=start,
                        end=end,
                        limit=limit_variants_per_gene,
                    )
                    overlap_cache[cache_key] = variants
                bucket = gene_to_variants.setdefault(int(gloc["entity_id"]), [])
                seen_variant_ids = {int(v["variant_id"]) for v in bucket}
                for variant in variants:
                    vid = int(variant["variant_id"])
                    if vid in seen_variant_ids:
                        continue
                    bucket.append(variant)
                    seen_variant_ids.add(vid)

            snp_rows_count = 0
            stop = False

            for model in gene_pair_models:
                gene_1_id = int(model["gene_1_id"])
                gene_2_id = int(model["gene_2_id"])
                v1_list = gene_to_variants.get(gene_1_id, [])
                v2_list = gene_to_variants.get(gene_2_id, [])
                if not v1_list or not v2_list:
                    continue

                per_gene_pair_seen: set[tuple[int, int]] = set()

                for v1 in v1_list:
                    for v2 in v2_list:
                        id1 = int(v1["variant_id"])
                        id2 = int(v2["variant_id"])
                        if id1 == id2:
                            continue

                        dedup_key = (min(id1, id2), max(id1, id2))
                        if dedup_key in per_gene_pair_seen:
                            continue
                        per_gene_pair_seen.add(dedup_key)

                        seed_count = int(id1 in seed_variant_ids) + int(id2 in seed_variant_ids)
                        if not _scope_keep(snp_pair_scope, seed_count):
                            continue

                        row = self._base_row()
                        row.update(stats)
                        row["row_type"] = "snp_pair"
                        row["observation"] = "ok"
                        row["gene_1_id"] = gene_1_id
                        row["gene_1_name"] = _entity_name(gene_1_id)
                        row["gene_2_id"] = gene_2_id
                        row["gene_2_name"] = _entity_name(gene_2_id)
                        row["gene_pair_seed_scope"] = model["gene_pair_seed_scope"]
                        row["gene_pair_seed_count"] = model["gene_pair_seed_count"]
                        row["group_support_count"] = len(model["group_ids"])
                        row["group_support_ids"] = "|".join(str(x) for x in model["group_ids"])
                        row["group_support_names"] = "|".join(model["group_names"])

                        row["variant_1_id"] = id1
                        row["variant_1_rsid"] = v1.get("rsid")
                        row["variant_1_chromosome"] = v1.get("chromosome")
                        row["variant_1_start"] = v1.get("position_start")
                        row["variant_1_end"] = v1.get("position_end")

                        row["variant_2_id"] = id2
                        row["variant_2_rsid"] = v2.get("rsid")
                        row["variant_2_chromosome"] = v2.get("chromosome")
                        row["variant_2_start"] = v2.get("position_start")
                        row["variant_2_end"] = v2.get("position_end")

                        row["snp_pair_seed_count"] = seed_count
                        row["snp_pair_seed_scope"] = _seed_scope(seed_count)
                        rows_out.append(row)

                        snp_rows_count += 1
                        if max_snp_pairs and snp_rows_count >= max_snp_pairs:
                            stop = True
                            snp_pairs_truncated = True
                            break
                    if stop:
                        break
                if stop:
                    break

        if snp_pairs_truncated and emit_not_found_rows:
            row = self._base_row()
            row.update(stats)
            row["row_type"] = "summary"
            row["observation"] = "truncated"
            row["note"] = f"SNP-SNP output truncated by max_snp_pairs={max_snp_pairs}."
            rows_out.append(row)

        if not gene_pair_models and emit_not_found_rows:
            row = self._base_row()
            row.update(stats)
            row["row_type"] = "summary"
            row["observation"] = "not_found"
            row["note"] = "No gene pairs matched filters/scope."
            rows_out.append(row)

        df = pd.DataFrame(rows_out)
        if not df.empty:
            df = df.sort_values(
                by=[
                    "row_type",
                    "gene_1_name",
                    "gene_2_name",
                    "variant_1_chromosome",
                    "variant_1_start",
                    "variant_2_start",
                ],
                na_position="last",
            )

        df = df.reindex(columns=self.columns)
        self.results = df
        return df.reset_index(drop=True)
