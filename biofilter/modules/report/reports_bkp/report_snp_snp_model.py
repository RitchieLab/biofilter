from __future__ import annotations

import re
from bisect import bisect_right
from collections import OrderedDict
from itertools import combinations
from typing import Any

import pandas as pd
from sqlalchemy import MetaData, Table, and_, func, or_, select
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import (
    Entity,
    EntityAlias,
    EntityGroup,
    EntityLocation,
    EntityRelationship,
    EntityRelationshipType,
    ETLDataSource,
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


def _iter_group_gene_pairs(
    members: set[int],
    seed_gene_ids: set[int],
    scope: str,
):
    """
    Generate candidate gene pairs for one group, already pruned by scope.
    """
    seed_members = sorted(members & seed_gene_ids)
    non_seed_members = sorted(members - seed_gene_ids)

    if scope == "both_from_seed":
        for gene_1_id, gene_2_id in combinations(seed_members, 2):
            yield (gene_1_id, gene_2_id)
        return

    if scope == "one_from_seed":
        for seed_gene_id in seed_members:
            for non_seed_gene_id in non_seed_members:
                yield (
                    min(seed_gene_id, non_seed_gene_id),
                    max(seed_gene_id, non_seed_gene_id),
                )
        return

    if scope == "at_least_one_from_seed":
        for gene_1_id, gene_2_id in combinations(seed_members, 2):
            yield (gene_1_id, gene_2_id)
        for seed_gene_id in seed_members:
            for non_seed_gene_id in non_seed_members:
                yield (
                    min(seed_gene_id, non_seed_gene_id),
                    max(seed_gene_id, non_seed_gene_id),
                )
        return

    # any_expanded
    members_sorted = sorted(members)
    for gene_1_id, gene_2_id in combinations(members_sorted, 2):
        yield (gene_1_id, gene_2_id)


def _prune_group_to_gene_ids_for_scope(
    group_to_gene_ids: dict[int, set[int]],
    seed_gene_ids: set[int],
    scope: str,
) -> dict[int, set[int]]:
    """
    Drop groups that cannot produce pairs for the requested scope.
    """
    out: dict[int, set[int]] = {}
    for group_id, members in group_to_gene_ids.items():
        seed_members = members & seed_gene_ids
        non_seed_members = members - seed_gene_ids

        if scope == "both_from_seed":
            if len(seed_members) < 2:
                continue
            out[group_id] = set(seed_members)
            continue

        if scope == "one_from_seed":
            if not seed_members or not non_seed_members:
                continue
            out[group_id] = set(seed_members | non_seed_members)
            continue

        if scope == "at_least_one_from_seed":
            if not seed_members:
                continue
            out[group_id] = set(members)
            continue

        out[group_id] = set(members)
    return out


_DIRECT_GENE_TOKENS = {
    "direct gene",
    "direct_gene",
    "directgene",
    "gene-gene",
    "gene_gene",
    "direct",
}


class SNPSNPModelReport(ReportBase):
    name = "snp_snp_model"
    description = (
        "Builds BF4 gene-gene and SNP-SNP candidate models from seed genomic positions "
        "using SNV rows from variant_masters + entity_locations, collapsing "
        "multi-allelic variant rows, then expands through biological group "
        "relationships (for example pathways) or direct gene-gene links."
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
        "variant_1_an",
        "variant_1_grpmax",
        "variant_1_cadd_raw_score",
        "variant_1_cadd_phred",
        "variant_1_consequence_ids",
        "variant_1_consequence_names",
        "variant_1_consequence_groups",
        "variant_1_consequence_categories",
        "variant_1_lof_confidences",
        "variant_1_predictor_names",
        "variant_1_prediction_scores",
        "variant_1_prediction_classifications",
        "variant_2_id",
        "variant_2_rsid",
        "variant_2_chromosome",
        "variant_2_start",
        "variant_2_end",
        "variant_2_an",
        "variant_2_grpmax",
        "variant_2_cadd_raw_score",
        "variant_2_cadd_phred",
        "variant_2_consequence_ids",
        "variant_2_consequence_names",
        "variant_2_consequence_groups",
        "variant_2_consequence_categories",
        "variant_2_lof_confidences",
        "variant_2_predictor_names",
        "variant_2_prediction_scores",
        "variant_2_prediction_classifications",
        "snp_pair_seed_scope",
        "snp_pair_seed_count",
        "group_support_count",
        "group_support_ids",
        "group_support_names",
        "data_source_support_count",
        "data_source_support_ids",
        "data_source_support_names",
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
            "group_data_sources": ["Reactome"],
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
        table_resolver = getattr(self.db, "table", None)
        if callable(table_resolver):
            return table_resolver(table_name)

        metadata = MetaData()
        return Table(table_name, metadata, autoload_with=self.db.engine)

    def _optional_table(self, table_name: str) -> Table | None:
        try:
            return self._table(table_name)
        except NoSuchTableError:
            return None

    @staticmethod
    def _variant_projection_columns(vm: Table) -> list[Any]:
        cols: list[Any] = [
            vm.c.variant_id,
            vm.c.rsid,
            vm.c.chromosome,
            vm.c.position_start,
            vm.c.position_end,
            vm.c.reference_allele,
            vm.c.alternate_allele,
        ]
        for col_name in ("an", "grpmax", "cadd_raw_score", "cadd_phred"):
            if col_name in vm.c:
                cols.append(vm.c[col_name])
        return cols

    @staticmethod
    def _join_tokens(values: set[str]) -> str | None:
        cleaned = sorted(
            {_norm_str(value) for value in values if _norm_str(value)},
            key=lambda x: x.lower(),
        )
        if not cleaned:
            return None
        return "|".join(cleaned)

    @staticmethod
    def _join_ints(values: set[int]) -> str | None:
        if not values:
            return None
        return "|".join(str(v) for v in sorted(values))

    @staticmethod
    def _format_score(value: Any) -> str | None:
        if value is None:
            return None
        try:
            return format(float(value), "g")
        except Exception:
            out = _norm_str(value)
            return out or None

    def _query_variant_molecular_effect_annotations(
        self,
        variant_keys: set[tuple[int, int]],
        chunk_size: int = 2000,
    ) -> dict[tuple[int, int], dict[str, Any]]:
        vme = self._optional_table("variant_molecular_effects")
        if vme is None or not variant_keys:
            return {}

        has_consequence = "consequence_id" in vme.c
        has_lof_conf = "lof_confidence" in vme.c
        if not has_consequence and not has_lof_conf:
            return {}

        key_to_data: dict[tuple[int, int], dict[str, set[Any]]] = {}
        all_consequence_ids: set[int] = set()
        keys_by_chrom: dict[int, set[int]] = {}
        for chrom, variant_id in variant_keys:
            keys_by_chrom.setdefault(int(chrom), set()).add(int(variant_id))

        query_cols = [vme.c.chromosome, vme.c.variant_id]
        if has_consequence:
            query_cols.append(vme.c.consequence_id)
        if has_lof_conf:
            query_cols.append(vme.c.lof_confidence)

        for chrom, variant_ids in keys_by_chrom.items():
            for chunk in self._iter_chunks(sorted(variant_ids), chunk_size):
                stmt = select(*query_cols).where(
                    and_(
                        vme.c.chromosome == int(chrom),
                        vme.c.variant_id.in_(chunk),
                    )
                )
                rows = self.session.execute(stmt).mappings().all()
                for row in rows:
                    key = (int(row["chromosome"]), int(row["variant_id"]))
                    bucket = key_to_data.setdefault(
                        key,
                        {
                            "consequence_ids": set(),
                            "lof_confidences": set(),
                        },
                    )
                    if has_consequence and row.get("consequence_id") is not None:
                        consequence_id = int(row["consequence_id"])
                        bucket["consequence_ids"].add(consequence_id)
                        all_consequence_ids.add(consequence_id)
                    if has_lof_conf:
                        lof_conf = _norm_str(row.get("lof_confidence"))
                        if lof_conf:
                            bucket["lof_confidences"].add(lof_conf)

        consequence_meta: dict[int, dict[str, Any]] = {}
        group_name_by_id: dict[int, str] = {}
        category_name_by_id: dict[int, str] = {}

        vc = self._optional_table("variant_consequences")
        if vc is not None and all_consequence_ids:
            vc_cols = [vc.c.id]
            if "name" in vc.c:
                vc_cols.append(vc.c.name)
            if "consequence_group_id" in vc.c:
                vc_cols.append(vc.c.consequence_group_id)
            if "consequence_category_id" in vc.c:
                vc_cols.append(vc.c.consequence_category_id)

            for chunk in self._iter_chunks(sorted(all_consequence_ids), chunk_size):
                stmt = select(*vc_cols).where(vc.c.id.in_(chunk))
                for row in self.session.execute(stmt).mappings().all():
                    cid = int(row["id"])
                    consequence_meta[cid] = {
                        "name": row.get("name"),
                        "group_id": row.get("consequence_group_id"),
                        "category_id": row.get("consequence_category_id"),
                    }

            group_ids = {
                int(meta["group_id"])
                for meta in consequence_meta.values()
                if meta.get("group_id") is not None
            }
            category_ids = {
                int(meta["category_id"])
                for meta in consequence_meta.values()
                if meta.get("category_id") is not None
            }

            vcg = self._optional_table("variant_consequence_groups")
            if vcg is not None and group_ids and "name" in vcg.c:
                for chunk in self._iter_chunks(sorted(group_ids), chunk_size):
                    stmt = select(vcg.c.id, vcg.c.name).where(vcg.c.id.in_(chunk))
                    for row in self.session.execute(stmt).mappings().all():
                        group_name_by_id[int(row["id"])] = _norm_str(row["name"])

            vcc = self._optional_table("variant_consequence_categories")
            if vcc is not None and category_ids and "name" in vcc.c:
                for chunk in self._iter_chunks(sorted(category_ids), chunk_size):
                    stmt = select(vcc.c.id, vcc.c.name).where(vcc.c.id.in_(chunk))
                    for row in self.session.execute(stmt).mappings().all():
                        category_name_by_id[int(row["id"])] = _norm_str(row["name"])

        out: dict[tuple[int, int], dict[str, Any]] = {}
        for key, bucket in key_to_data.items():
            consequence_ids = {
                int(cid)
                for cid in bucket.get("consequence_ids", set())
                if cid is not None
            }
            consequence_names: set[str] = set()
            consequence_groups: set[str] = set()
            consequence_categories: set[str] = set()

            for consequence_id in consequence_ids:
                meta = consequence_meta.get(consequence_id)
                if meta is None:
                    continue
                name = _norm_str(meta.get("name"))
                if name:
                    consequence_names.add(name)
                group_id = _parse_int(meta.get("group_id"))
                if group_id is not None:
                    group_name = _norm_str(group_name_by_id.get(int(group_id)))
                    if group_name:
                        consequence_groups.add(group_name)
                category_id = _parse_int(meta.get("category_id"))
                if category_id is not None:
                    category_name = _norm_str(category_name_by_id.get(int(category_id)))
                    if category_name:
                        consequence_categories.add(category_name)

            out[key] = {
                "consequence_ids": self._join_ints(consequence_ids),
                "consequence_names": self._join_tokens(consequence_names),
                "consequence_groups": self._join_tokens(consequence_groups),
                "consequence_categories": self._join_tokens(consequence_categories),
                "lof_confidences": self._join_tokens(
                    {
                        _norm_str(x)
                        for x in bucket.get("lof_confidences", set())
                        if _norm_str(x)
                    }
                ),
            }
        return out

    def _query_variant_effect_prediction_annotations(
        self,
        variant_keys: set[tuple[int, int]],
        chunk_size: int = 2000,
    ) -> dict[tuple[int, int], dict[str, Any]]:
        vep = self._optional_table("variant_effect_predictions")
        if vep is None or not variant_keys:
            return {}

        has_predictor_name = "predictor_name" in vep.c
        has_score = "score" in vep.c
        has_classification = "classification" in vep.c
        if not has_predictor_name and not has_score and not has_classification:
            return {}

        keys_by_chrom: dict[int, set[int]] = {}
        for chrom, variant_id in variant_keys:
            keys_by_chrom.setdefault(int(chrom), set()).add(int(variant_id))

        query_cols = [vep.c.chromosome, vep.c.variant_id]
        if has_predictor_name:
            query_cols.append(vep.c.predictor_name)
        if has_score:
            query_cols.append(vep.c.score)
        if has_classification:
            query_cols.append(vep.c.classification)

        raw: dict[tuple[int, int], dict[str, set[str]]] = {}
        for chrom, variant_ids in keys_by_chrom.items():
            for chunk in self._iter_chunks(sorted(variant_ids), chunk_size):
                stmt = select(*query_cols).where(
                    and_(
                        vep.c.chromosome == int(chrom),
                        vep.c.variant_id.in_(chunk),
                    )
                )
                rows = self.session.execute(stmt).mappings().all()
                for row in rows:
                    key = (int(row["chromosome"]), int(row["variant_id"]))
                    bucket = raw.setdefault(
                        key,
                        {
                            "predictor_names": set(),
                            "prediction_scores": set(),
                            "prediction_classifications": set(),
                        },
                    )
                    if has_predictor_name:
                        predictor_name = _norm_str(row.get("predictor_name"))
                        if predictor_name:
                            bucket["predictor_names"].add(predictor_name)
                    if has_score:
                        score = self._format_score(row.get("score"))
                        if score:
                            bucket["prediction_scores"].add(score)
                    if has_classification:
                        classification = _norm_str(row.get("classification"))
                        if classification:
                            bucket["prediction_classifications"].add(classification)

        out: dict[tuple[int, int], dict[str, Any]] = {}
        for key, bucket in raw.items():
            out[key] = {
                "predictor_names": self._join_tokens(bucket.get("predictor_names", set())),
                "prediction_scores": self._join_tokens(
                    bucket.get("prediction_scores", set())
                ),
                "prediction_classifications": self._join_tokens(
                    bucket.get("prediction_classifications", set())
                ),
            }
        return out

    @staticmethod
    def _variant_dedupe_key(variant: dict[str, Any]) -> tuple[Any, ...]:
        """
        Collapse alternate-allele rows representing the same logical variant.

        Priority for identity:
        1) rsID (when available)
        2) genomic locus + reference allele
        """
        rsid = _norm_str(variant.get("rsid")).lower()
        if rsid:
            return ("rsid", rsid)
        return (
            "locus",
            int(variant.get("chromosome") or 0),
            int(variant.get("position_start") or 0),
            int(variant.get("position_end") or 0),
            _norm_str(variant.get("reference_allele")).upper(),
        )

    def _dedupe_variants(self, variants: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        seen: set[tuple[Any, ...]] = set()
        for variant in variants:
            key = self._variant_dedupe_key(variant)
            if key in seen:
                continue
            seen.add(key)
            out.append(variant)
        return out

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
            select(*self._variant_projection_columns(vm))
            .where(
                and_(
                    vm.c.chromosome == int(chrom),
                    vm.c.position_start <= int(pos),
                    vm.c.position_end >= int(pos),
                )
            )
            .order_by(vm.c.position_start.asc(), vm.c.variant_id.asc())
        )
        if "allele_type" in vm.c:
            stmt = stmt.where(func.lower(vm.c.allele_type) == "snv")
        rows = self.session.execute(stmt).mappings().all()
        return self._dedupe_variants([dict(row) for row in rows])

    @staticmethod
    def _iter_chunks(values: list[int], chunk_size: int):
        size = max(1, int(chunk_size))
        for i in range(0, len(values), size):
            yield values[i : i + size]

    def _query_seed_variants_for_inputs(
        self,
        vm: Table,
        valid_inputs: list[dict[str, Any]],
        chunk_size: int = 500,
    ) -> tuple[dict[int, dict[str, Any]], set[tuple[int, int]]]:
        """
        Resolve seed variants for input positions with batched DB reads.

        Fast path (default when `allele_type` exists):
        - groups inputs by chromosome
        - queries SNV rows with `position_start IN (...)` in chunks

        Fallback path:
        - keeps the previous per-position overlap query behavior
        """
        seed_variants_by_id: dict[int, dict[str, Any]] = {}
        found_positions: set[tuple[int, int]] = set()

        if not valid_inputs:
            return seed_variants_by_id, found_positions

        # If table does not expose allele_type, keep old exact behavior (range overlap).
        # This avoids semantic changes in non-SNV datasets.
        if "allele_type" not in vm.c:
            for item in valid_inputs:
                chrom = int(item["chromosome"])
                pos = int(item["position"])
                variants = self._query_variants_at_position(vm=vm, chrom=chrom, pos=pos)
                if variants:
                    found_positions.add((chrom, pos))
                for variant in variants:
                    seed_variants_by_id[int(variant["variant_id"])] = variant
            return seed_variants_by_id, found_positions

        positions_by_chrom: dict[int, set[int]] = {}
        for item in valid_inputs:
            chrom = int(item["chromosome"])
            pos = int(item["position"])
            positions_by_chrom.setdefault(chrom, set()).add(pos)

        total_unique_positions = sum(len(positions) for positions in positions_by_chrom.values())
        processed_positions = 0

        for chrom in sorted(positions_by_chrom.keys()):
            positions_sorted = sorted(positions_by_chrom[chrom])
            for chunk in self._iter_chunks(positions_sorted, chunk_size):
                stmt = (
                    select(*self._variant_projection_columns(vm))
                    .where(
                        and_(
                            vm.c.chromosome == int(chrom),
                            vm.c.position_start.in_(chunk),
                            func.lower(vm.c.allele_type) == "snv",
                        )
                    )
                    .order_by(vm.c.position_start.asc(), vm.c.variant_id.asc())
                )

                rows = self.session.execute(stmt).mappings().all()
                rows_by_pos: dict[int, list[dict[str, Any]]] = {}
                for row in rows:
                    drow = dict(row)
                    pos_key = int(drow["position_start"])
                    rows_by_pos.setdefault(pos_key, []).append(drow)

                for pos_key, pos_rows in rows_by_pos.items():
                    variants = self._dedupe_variants(pos_rows)
                    if variants:
                        found_positions.add((int(chrom), int(pos_key)))
                    for variant in variants:
                        seed_variants_by_id[int(variant["variant_id"])] = variant

                processed_positions += len(chunk)
                if (
                    processed_positions == total_unique_positions
                    or processed_positions % 200000 == 0
                ):
                    self.logger.log(
                        (
                            "Step 1 (seed variants): "
                            f"{processed_positions:,}/{total_unique_positions:,} "
                            "unique positions processed."
                        ),
                        "INFO",
                    )

        return seed_variants_by_id, found_positions

    def _query_variants_overlap(
        self,
        vm: Table,
        chrom: int,
        start: int,
        end: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        stmt = (
            select(*self._variant_projection_columns(vm))
            .where(
                and_(
                    vm.c.chromosome == int(chrom),
                    vm.c.position_start <= int(end),
                    vm.c.position_end >= int(start),
                )
            )
            .order_by(vm.c.position_start.asc(), vm.c.variant_id.asc())
        )
        if "allele_type" in vm.c:
            stmt = stmt.where(func.lower(vm.c.allele_type) == "snv")
        if limit > 0:
            # Overfetch to avoid underfilling after dedupe of alternate alleles.
            stmt = stmt.limit(max(limit * 5, limit))
        rows = self.session.execute(stmt).mappings().all()
        deduped = self._dedupe_variants([dict(row) for row in rows])
        if limit > 0:
            return deduped[:limit]
        return deduped

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

    def _query_gene_locations_for_chrom(
        self,
        chrom: int,
        build: int,
        gene_group_filter: set[str],
    ) -> list[dict[str, Any]]:
        q = (
            self.session.query(
                EntityLocation.entity_id.label("entity_id"),
                EntityLocation.start_pos.label("start_pos"),
                EntityLocation.end_pos.label("end_pos"),
            )
            .join(Entity, Entity.id == EntityLocation.entity_id)
            .join(EntityGroup, Entity.group_id == EntityGroup.id, isouter=True)
            .filter(EntityLocation.build == int(build))
            .filter(EntityLocation.chromosome == int(chrom))
        )

        if gene_group_filter:
            q = q.filter(func.lower(EntityGroup.name).in_(list(gene_group_filter)))

        rows = q.all()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "entity_id": int(row.entity_id),
                    "start_pos": int(row.start_pos),
                    "end_pos": int(row.end_pos),
                }
            )
        return out

    @staticmethod
    def _merge_seed_variant_windows(
        seed_variants_by_id: dict[int, dict[str, Any]],
        window_bp: int,
    ) -> dict[int, list[tuple[int, int]]]:
        windows_by_chrom: dict[int, list[tuple[int, int]]] = {}
        for variant in seed_variants_by_id.values():
            chrom = int(variant["chromosome"])
            start = max(1, int(variant["position_start"]) - int(window_bp))
            end = int(variant["position_end"]) + int(window_bp)
            windows_by_chrom.setdefault(chrom, []).append((start, end))

        merged_by_chrom: dict[int, list[tuple[int, int]]] = {}
        for chrom, windows in windows_by_chrom.items():
            if not windows:
                merged_by_chrom[chrom] = []
                continue
            windows_sorted = sorted(windows, key=lambda x: (x[0], x[1]))
            merged: list[tuple[int, int]] = []
            cur_start, cur_end = windows_sorted[0]
            for start, end in windows_sorted[1:]:
                if start <= cur_end + 1:
                    if end > cur_end:
                        cur_end = end
                    continue
                merged.append((cur_start, cur_end))
                cur_start, cur_end = start, end
            merged.append((cur_start, cur_end))
            merged_by_chrom[chrom] = merged
        return merged_by_chrom

    @staticmethod
    def _partition_variants_by_seed(
        variants: list[dict[str, Any]],
        seed_variant_ids: set[int],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        seed_list: list[dict[str, Any]] = []
        non_seed_list: list[dict[str, Any]] = []
        for variant in variants:
            vid = int(variant["variant_id"])
            if vid in seed_variant_ids:
                seed_list.append(variant)
            else:
                non_seed_list.append(variant)
        return seed_list, non_seed_list

    @staticmethod
    def _iter_snp_pair_candidates(
        *,
        scope: str,
        v1_seed: list[dict[str, Any]],
        v1_non_seed: list[dict[str, Any]],
        v2_seed: list[dict[str, Any]],
        v2_non_seed: list[dict[str, Any]],
    ):
        if scope == "both_from_seed":
            for v1 in v1_seed:
                for v2 in v2_seed:
                    yield v1, v2, 2
            return

        if scope == "one_from_seed":
            for v1 in v1_seed:
                for v2 in v2_non_seed:
                    yield v1, v2, 1
            for v1 in v1_non_seed:
                for v2 in v2_seed:
                    yield v1, v2, 1
            return

        if scope == "at_least_one_from_seed":
            for v1 in v1_seed:
                for v2 in v2_seed:
                    yield v1, v2, 2
            for v1 in v1_seed:
                for v2 in v2_non_seed:
                    yield v1, v2, 1
            for v1 in v1_non_seed:
                for v2 in v2_seed:
                    yield v1, v2, 1
            return

        # any_expanded
        all_1 = [*v1_seed, *v1_non_seed]
        all_2 = [*v2_seed, *v2_non_seed]
        v1_seed_ids = {int(v["variant_id"]) for v in v1_seed}
        v2_seed_ids = {int(v["variant_id"]) for v in v2_seed}
        for v1 in all_1:
            id1 = int(v1["variant_id"])
            is_seed_1 = id1 in v1_seed_ids
            for v2 in all_2:
                id2 = int(v2["variant_id"])
                is_seed_2 = id2 in v2_seed_ids
                yield v1, v2, int(is_seed_1) + int(is_seed_2)

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

    def _available_group_name_map(self) -> dict[str, str]:
        rows = self.session.query(EntityGroup.name).all()
        out: dict[str, str] = {}
        for row in rows:
            name = _norm_str(row[0])
            if not name:
                continue
            key = name.lower()
            if key not in out:
                out[key] = name
        return out

    @staticmethod
    def _resolve_group_tokens(
        tokens: set[str],
        available_map: dict[str, str],
    ) -> tuple[set[str], list[str]]:
        resolved: set[str] = set()
        unresolved: list[str] = []
        for token in tokens:
            token_norm = token.strip().lower()
            if not token_norm:
                continue

            candidates = [token_norm]
            if token_norm.endswith("s") and len(token_norm) > 1:
                candidates.append(token_norm[:-1])
            else:
                candidates.append(token_norm + "s")

            matched = False
            for candidate in candidates:
                if candidate in available_map:
                    resolved.add(candidate)
                    matched = True
                    break
            if not matched:
                unresolved.append(token_norm)
        return resolved, unresolved

    @staticmethod
    def _group_help_message(available_map: dict[str, str]) -> str:
        options = ["Direct Gene"] + sorted(
            {name for name in available_map.values()},
            key=lambda x: x.lower(),
        )
        return (
            "You need to inform one or more group_entity_groups. "
            "Available options include: " + ", ".join(options)
        )

    def _available_data_source_maps(self) -> tuple[dict[str, int], dict[int, str]]:
        rows = self.session.query(ETLDataSource.id, ETLDataSource.name).all()
        name_to_id: dict[str, int] = {}
        id_to_name: dict[int, str] = {}
        for row in rows:
            ds_id = _parse_int(row.id)
            ds_name = _norm_str(row.name)
            if ds_id is None or not ds_name:
                continue
            id_to_name[int(ds_id)] = ds_name
            key = ds_name.lower()
            if key not in name_to_id:
                name_to_id[key] = int(ds_id)
        return name_to_id, id_to_name

    @staticmethod
    def _resolve_data_source_tokens(
        tokens: set[str],
        name_to_id: dict[str, int],
        id_to_name: dict[int, str],
    ) -> tuple[set[int], list[str]]:
        resolved: set[int] = set()
        unresolved: list[str] = []
        for token in tokens:
            token_norm = _norm_str(token).lower()
            if not token_norm:
                continue

            token_id = _parse_int(token_norm)
            if token_id is not None and int(token_id) in id_to_name:
                resolved.add(int(token_id))
                continue

            if token_norm in name_to_id:
                resolved.add(int(name_to_id[token_norm]))
                continue

            unresolved.append(token_norm)
        return resolved, unresolved

    @staticmethod
    def _data_source_help_message(id_to_name: dict[int, str]) -> str:
        options = sorted({name for name in id_to_name.values()}, key=lambda x: x.lower())
        if not options:
            return "No data sources are available in ETLDataSource."
        return (
            "Invalid group_data_sources. Available options include: "
            + ", ".join(options)
        )

    def _query_seed_gene_to_groups(
        self,
        seed_gene_ids: set[int],
        relationship_type_filter: set[str],
        gene_group_filter: set[str],
        group_group_filter: set[str],
        group_entity_ids_filter: set[int] | None,
        group_data_source_ids_filter: set[int] | None,
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
                EntityRelationship.data_source_id.label("data_source_id"),
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
        if group_data_source_ids_filter is not None:
            q1 = q1.filter(
                EntityRelationship.data_source_id.in_(list(group_data_source_ids_filter))
            )

        for row in q1.all():
            rows_out.append(
                {
                    "relationship_id": int(row.relationship_id),
                    "relationship_type": row.relationship_type,
                    "gene_id": int(row.gene_id),
                    "group_id": int(row.group_id),
                    "data_source_id": _parse_int(row.data_source_id),
                }
            )

        q2 = (
            self.session.query(
                EntityRelationship.id.label("relationship_id"),
                rt.code.label("relationship_type"),
                EntityRelationship.entity_2_id.label("gene_id"),
                EntityRelationship.entity_1_id.label("group_id"),
                EntityRelationship.data_source_id.label("data_source_id"),
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
        if group_data_source_ids_filter is not None:
            q2 = q2.filter(
                EntityRelationship.data_source_id.in_(list(group_data_source_ids_filter))
            )

        for row in q2.all():
            rows_out.append(
                {
                    "relationship_id": int(row.relationship_id),
                    "relationship_type": row.relationship_type,
                    "gene_id": int(row.gene_id),
                    "group_id": int(row.group_id),
                    "data_source_id": _parse_int(row.data_source_id),
                }
            )

        return rows_out

    def _query_direct_gene_links(
        self,
        seed_gene_ids: set[int],
        relationship_type_filter: set[str],
        gene_group_filter: set[str],
        group_data_source_ids_filter: set[int] | None,
    ) -> list[dict[str, Any]]:
        if not seed_gene_ids:
            return []

        rt = aliased(EntityRelationshipType)
        e1 = aliased(Entity)
        e2 = aliased(Entity)
        eg1 = aliased(EntityGroup)
        eg2 = aliased(EntityGroup)

        q = (
            self.session.query(
                EntityRelationship.id.label("relationship_id"),
                rt.code.label("relationship_type"),
                EntityRelationship.entity_1_id.label("gene_1_id"),
                EntityRelationship.entity_2_id.label("gene_2_id"),
                EntityRelationship.data_source_id.label("data_source_id"),
            )
            .join(rt, rt.id == EntityRelationship.relationship_type_id)
            .join(e1, e1.id == EntityRelationship.entity_1_id)
            .join(eg1, eg1.id == e1.group_id, isouter=True)
            .join(e2, e2.id == EntityRelationship.entity_2_id)
            .join(eg2, eg2.id == e2.group_id, isouter=True)
            .filter(
                or_(
                    EntityRelationship.entity_1_id.in_(list(seed_gene_ids)),
                    EntityRelationship.entity_2_id.in_(list(seed_gene_ids)),
                )
            )
        )

        if relationship_type_filter:
            q = q.filter(func.lower(rt.code).in_(list(relationship_type_filter)))

        if gene_group_filter:
            q = q.filter(func.lower(eg1.name).in_(list(gene_group_filter)))
            q = q.filter(func.lower(eg2.name).in_(list(gene_group_filter)))
        if group_data_source_ids_filter is not None:
            q = q.filter(
                EntityRelationship.data_source_id.in_(list(group_data_source_ids_filter))
            )

        out: list[dict[str, Any]] = []
        for row in q.all():
            gene_1_id = int(row.gene_1_id)
            gene_2_id = int(row.gene_2_id)
            if gene_1_id == gene_2_id:
                continue
            out.append(
                {
                    "relationship_id": int(row.relationship_id),
                    "relationship_type": row.relationship_type,
                    "gene_1_id": min(gene_1_id, gene_2_id),
                    "gene_2_id": max(gene_1_id, gene_2_id),
                    "data_source_id": _parse_int(row.data_source_id),
                }
            )
        return out

    def _query_groups_to_genes(
        self,
        group_ids: set[int],
        relationship_type_filter: set[str],
        gene_group_filter: set[str],
        group_group_filter: set[str],
        group_data_source_ids_filter: set[int] | None,
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
                EntityRelationship.data_source_id.label("data_source_id"),
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
        if group_data_source_ids_filter is not None:
            q1 = q1.filter(
                EntityRelationship.data_source_id.in_(list(group_data_source_ids_filter))
            )

        for row in q1.all():
            rows_out.append(
                {
                    "relationship_id": int(row.relationship_id),
                    "relationship_type": row.relationship_type,
                    "group_id": int(row.group_id),
                    "gene_id": int(row.gene_id),
                    "data_source_id": _parse_int(row.data_source_id),
                }
            )

        q2 = (
            self.session.query(
                EntityRelationship.id.label("relationship_id"),
                rt.code.label("relationship_type"),
                EntityRelationship.entity_2_id.label("group_id"),
                EntityRelationship.entity_1_id.label("gene_id"),
                EntityRelationship.data_source_id.label("data_source_id"),
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
        if group_data_source_ids_filter is not None:
            q2 = q2.filter(
                EntityRelationship.data_source_id.in_(list(group_data_source_ids_filter))
            )

        for row in q2.all():
            rows_out.append(
                {
                    "relationship_id": int(row.relationship_id),
                    "relationship_type": row.relationship_type,
                    "group_id": int(row.group_id),
                    "gene_id": int(row.gene_id),
                    "data_source_id": _parse_int(row.data_source_id),
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
            # self.param("gene_pair_scope", "at_least_one_from_seed"),  
            self.param("gene_pair_scope", "both_from_seed"),
            "gene_pair_scope",
        )
        snp_pair_scope = self._parse_scope(
            self.param("snp_pair_scope", "at_least_one_from_seed"),
            "snp_pair_scope",
        )

        gene_group_filter = _as_ci_set(self.param("gene_entity_groups", ["Gene", "Genes"]))
        group_filter_input = self.param("group_entity_groups", None)
        if group_filter_input is None:
            group_filter_input = self.param("group_group_filter", None)

        if group_filter_input is None:
            group_group_filter_raw = {"pathway", "pathways"}
        else:
            group_group_filter_raw = _as_ci_set(group_filter_input)

        has_direct_gene_mode = any(
            token in _DIRECT_GENE_TOKENS for token in group_group_filter_raw
        )
        group_group_filter_raw = {
            token for token in group_group_filter_raw if token not in _DIRECT_GENE_TOKENS
        }

        available_group_map = self._available_group_name_map()
        group_group_filter, unresolved_groups = self._resolve_group_tokens(
            tokens=group_group_filter_raw,
            available_map=available_group_map,
        )

        if unresolved_groups and not group_group_filter and not has_direct_gene_mode:
            raise ValueError(
                "Invalid group_entity_groups: "
                + ", ".join(unresolved_groups)
                + ". "
                + self._group_help_message(available_group_map)
            )

        if not group_group_filter and not has_direct_gene_mode:
            raise ValueError(self._group_help_message(available_group_map))

        group_data_sources_input = self.param("group_data_sources", None)
        if group_data_sources_input is None:
            group_data_sources_input = self.param("data_sources", None)
        group_data_source_tokens = _as_ci_set(group_data_sources_input)
        group_data_source_ids_filter: set[int] | None = None
        data_source_name_by_id: dict[int, str] = {}
        if group_data_source_tokens:
            ds_name_to_id, data_source_name_by_id = self._available_data_source_maps()
            resolved_ds_ids, unresolved_ds = self._resolve_data_source_tokens(
                tokens=group_data_source_tokens,
                name_to_id=ds_name_to_id,
                id_to_name=data_source_name_by_id,
            )
            if unresolved_ds and not resolved_ds_ids:
                raise ValueError(
                    "Invalid group_data_sources: "
                    + ", ".join(unresolved_ds)
                    + ". "
                    + self._data_source_help_message(data_source_name_by_id)
                )
            group_data_source_ids_filter = resolved_ds_ids if resolved_ds_ids else None
        else:
            _, data_source_name_by_id = self._available_data_source_maps()

        # Disabled by default: keep all relationship types unless explicitly provided.
        relationship_type_filter = _as_ci_set(self.param("relationship_types", None))

        group_entities_keys = _as_list_ci_ordered(self.param("group_entities", None))

        vm = self._table("variant_masters")

        normalized_inputs = [self._parse_position_input(item) for item in input_data]
        valid_inputs = [item for item in normalized_inputs if item["status"] == "ok"]

        rows_out: list[dict[str, Any]] = []

        # If variant got any problem during parsing, emit a row for each invalid input and then stop.
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
        # Step 1: seed variants from input chr:position # TODO melhor query por bloco ou por variant?
        # ------------------------------------------------------------------
        seed_variants_by_id, found_positions = self._query_seed_variants_for_inputs(
            vm=vm,
            valid_inputs=valid_inputs,
        )

        if emit_not_found_rows:
            for item in valid_inputs:
                key = (int(item["chromosome"]), int(item["position"]))
                if key in found_positions:
                    continue
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

        if not seed_variants_by_id:
            df = pd.DataFrame(rows_out).reindex(columns=self.columns)
            self.results = df
            return df.reset_index(drop=True)

        # ------------------------------------------------------------------
        # Step 2: seed variant -> seed genes via entity_locations overlap
        # ------------------------------------------------------------------
        seed_variant_ids = set(seed_variants_by_id.keys())
        seed_gene_ids: set[int] = set()

        merged_windows_by_chrom = self._merge_seed_variant_windows(
            seed_variants_by_id=seed_variants_by_id,
            window_bp=window_bp,
        )
        total_windows = sum(
            len(intervals) for intervals in merged_windows_by_chrom.values()
        )
        self.logger.log(
            (
                "Step 2 (variant->gene): "
                f"{len(seed_variants_by_id):,} variants merged into "
                f"{total_windows:,} windows."
            ),
            "INFO",
        )

        processed_chroms = 0
        total_chroms = len(merged_windows_by_chrom)

        for chrom, intervals in sorted(merged_windows_by_chrom.items()):
            if not intervals:
                continue

            gene_rows = self._query_gene_locations_for_chrom(
                chrom=chrom,
                build=build,
                gene_group_filter=gene_group_filter,
            )
            if not gene_rows:
                processed_chroms += 1
                continue

            interval_starts = [start for start, _ in intervals]
            for gene in gene_rows:
                gene_start = int(gene["start_pos"])
                gene_end = int(gene["end_pos"])

                idx = bisect_right(interval_starts, gene_end) - 1
                if idx < 0:
                    continue
                if int(intervals[idx][1]) < gene_start:
                    continue
                seed_gene_ids.add(int(gene["entity_id"]))

            processed_chroms += 1
            self.logger.log(
                (
                    "Step 2 (variant->gene): "
                    f"chromosome {chrom} processed "
                    f"({processed_chroms}/{total_chroms})."
                ),
                "INFO",
            )

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
        seed_group_links: list[dict[str, Any]] = []
        selected_group_ids: set[int] = set()

        if group_group_filter:
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
                group_data_source_ids_filter=group_data_source_ids_filter,
            )

            selected_group_ids = {int(x["group_id"]) for x in seed_group_links}

        # ------------------------------------------------------------------
        # Step 4: selected groups -> expanded genes
        # ------------------------------------------------------------------
        group_gene_links: list[dict[str, Any]] = []
        if selected_group_ids:
            group_gene_links = self._query_groups_to_genes(
                group_ids=selected_group_ids,
                relationship_type_filter=relationship_type_filter,
                gene_group_filter=gene_group_filter,
                group_group_filter=group_group_filter,
                group_data_source_ids_filter=group_data_source_ids_filter,
            )

        group_to_gene_ids: dict[int, set[int]] = {}
        group_to_data_source_ids: dict[int, set[int]] = {}
        for link in seed_group_links + group_gene_links:
            gid = int(link["group_id"])
            egid = int(link["gene_id"])
            group_to_gene_ids.setdefault(gid, set()).add(egid)
            ds_id = _parse_int(link.get("data_source_id"))
            if ds_id is not None:
                group_to_data_source_ids.setdefault(gid, set()).add(int(ds_id))

        group_to_gene_ids = _prune_group_to_gene_ids_for_scope(
            group_to_gene_ids=group_to_gene_ids,
            seed_gene_ids=seed_gene_ids,
            scope=gene_pair_scope,
        )
        group_to_data_source_ids = {
            group_id: group_to_data_source_ids.get(group_id, set())
            for group_id in group_to_gene_ids
        }

        expanded_gene_ids: set[int] = set(seed_gene_ids)
        for members in group_to_gene_ids.values():
            expanded_gene_ids.update(members)

        # ------------------------------------------------------------------
        # Step 5: gene-gene from co-membership in selected groups
        # ------------------------------------------------------------------
        pair_to_group_ids: dict[tuple[int, int], set[int]] = {}
        pair_to_data_source_ids: dict[tuple[int, int], set[int]] = {}
        support_name_by_id: dict[int, str] = {}

        for group_id, members in group_to_gene_ids.items():
            for gene_1_id, gene_2_id in _iter_group_gene_pairs(
                members=members,
                seed_gene_ids=seed_gene_ids,
                scope=gene_pair_scope,
            ):
                pair_key = (gene_1_id, gene_2_id)
                pair_to_group_ids.setdefault(pair_key, set()).add(int(group_id))
                pair_to_data_source_ids.setdefault(pair_key, set()).update(
                    group_to_data_source_ids.get(int(group_id), set())
                )

        if has_direct_gene_mode:
            direct_links = self._query_direct_gene_links(
                seed_gene_ids=seed_gene_ids,
                relationship_type_filter=relationship_type_filter,
                gene_group_filter=gene_group_filter,
                group_data_source_ids_filter=group_data_source_ids_filter,
            )
            for link in direct_links:
                gene_pair = (int(link["gene_1_id"]), int(link["gene_2_id"]))
                synthetic_support_id = -int(link["relationship_id"])
                pair_to_group_ids.setdefault(gene_pair, set()).add(synthetic_support_id)
                support_name_by_id[synthetic_support_id] = "Direct Gene"
                ds_id = _parse_int(link.get("data_source_id"))
                if ds_id is not None:
                    pair_to_data_source_ids.setdefault(gene_pair, set()).add(int(ds_id))

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
        genes_in_pair_models: set[int] = set()

        metadata_map = self._resolve_entity_metadata(
            {gene_id for pair in pair_to_group_ids for gene_id in pair} | selected_group_ids
        )

        def _entity_name(entity_id: int) -> str:
            meta = metadata_map.get(int(entity_id), {})
            return _norm_str(meta.get("primary_name")) or str(entity_id)

        for (gene_1_id, gene_2_id), support_groups in sorted(pair_to_group_ids.items()):
            seed_count = int(gene_1_id in seed_gene_ids) + int(gene_2_id in seed_gene_ids)
            if not _scope_keep(gene_pair_scope, seed_count):
                continue

            gene_pair_scope_value = _seed_scope(seed_count)
            group_ids_sorted = sorted(support_groups)
            group_names = [
                support_name_by_id.get(group_id) or _entity_name(group_id)
                for group_id in group_ids_sorted
            ]
            ds_ids_sorted = sorted(pair_to_data_source_ids.get((gene_1_id, gene_2_id), set()))
            ds_names = [
                data_source_name_by_id.get(data_source_id) or str(data_source_id)
                for data_source_id in ds_ids_sorted
            ]

            model = {
                "gene_1_id": int(gene_1_id),
                "gene_2_id": int(gene_2_id),
                "gene_pair_seed_count": seed_count,
                "gene_pair_seed_scope": gene_pair_scope_value,
                "group_ids": group_ids_sorted,
                "group_names": group_names,
                "data_source_ids": ds_ids_sorted,
                "data_source_names": ds_names,
            }
            gene_pair_models.append(model)
            genes_in_pair_models.add(int(gene_1_id))
            genes_in_pair_models.add(int(gene_2_id))

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
                row["data_source_support_count"] = len(ds_ids_sorted)
                row["data_source_support_ids"] = "|".join(str(x) for x in ds_ids_sorted)
                row["data_source_support_names"] = "|".join(ds_names)
                rows_out.append(row)

        # ------------------------------------------------------------------
        # Step 6: expand variants for genes and build SNP-SNP per gene pair
        # ------------------------------------------------------------------
        snp_pairs_truncated = False
        if include_snp_pairs and gene_pair_models:
            if expand_variants_from_expanded_genes:
                genes_for_variant_expansion = set(genes_in_pair_models)
            else:
                genes_for_variant_expansion = set(genes_in_pair_models) & set(seed_gene_ids)

            gene_locations = self._query_gene_locations(
                gene_ids=genes_for_variant_expansion,
                build=build,
                gene_group_filter=gene_group_filter,
            )

            gene_to_variants: dict[int, list[dict[str, Any]]] = {}
            gene_to_variant_ids: dict[int, set[int]] = {}
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

                gene_id = int(gloc["entity_id"])
                bucket = gene_to_variants.setdefault(gene_id, [])
                seen_variant_ids = gene_to_variant_ids.setdefault(gene_id, set())

                if limit_variants_per_gene > 0 and len(seen_variant_ids) >= limit_variants_per_gene:
                    continue

                for variant in variants:
                    vid = int(variant["variant_id"])
                    if vid in seen_variant_ids:
                        continue
                    if (
                        limit_variants_per_gene > 0
                        and len(seen_variant_ids) >= limit_variants_per_gene
                    ):
                        break
                    bucket.append(variant)
                    seen_variant_ids.add(vid)

            gene_to_seed_variants: dict[int, list[dict[str, Any]]] = {}
            gene_to_non_seed_variants: dict[int, list[dict[str, Any]]] = {}
            for gene_id, variants in gene_to_variants.items():
                seed_list, non_seed_list = self._partition_variants_by_seed(
                    variants=variants,
                    seed_variant_ids=seed_variant_ids,
                )
                gene_to_seed_variants[gene_id] = seed_list
                gene_to_non_seed_variants[gene_id] = non_seed_list

            self.logger.log(
                (
                    "Step 6 (snp expansion): "
                    f"{len(gene_to_variants):,} genes with variants after limit."
                ),
                "INFO",
            )

            variant_keys_for_annotations: set[tuple[int, int]] = set()
            for variants in gene_to_variants.values():
                for variant in variants:
                    chrom = _parse_int(variant.get("chromosome"))
                    variant_id = _parse_int(variant.get("variant_id"))
                    if chrom is None or variant_id is None:
                        continue
                    variant_keys_for_annotations.add((int(chrom), int(variant_id)))

            molecular_effects_by_variant = self._query_variant_molecular_effect_annotations(
                variant_keys=variant_keys_for_annotations
            )
            predictions_by_variant = self._query_variant_effect_prediction_annotations(
                variant_keys=variant_keys_for_annotations
            )

            snp_rows_count = 0
            stop = False
            processed_gene_pairs = 0
            total_gene_pairs = len(gene_pair_models)

            for model in gene_pair_models:
                gene_1_id = int(model["gene_1_id"])
                gene_2_id = int(model["gene_2_id"])
                v1_seed = gene_to_seed_variants.get(gene_1_id, [])
                v1_non_seed = gene_to_non_seed_variants.get(gene_1_id, [])
                v2_seed = gene_to_seed_variants.get(gene_2_id, [])
                v2_non_seed = gene_to_non_seed_variants.get(gene_2_id, [])
                if (not v1_seed and not v1_non_seed) or (not v2_seed and not v2_non_seed):
                    processed_gene_pairs += 1
                    continue

                per_gene_pair_seen: set[tuple[int, int]] = set()

                for v1, v2, seed_count in self._iter_snp_pair_candidates(
                    scope=snp_pair_scope,
                    v1_seed=v1_seed,
                    v1_non_seed=v1_non_seed,
                    v2_seed=v2_seed,
                    v2_non_seed=v2_non_seed,
                ):
                    id1 = int(v1["variant_id"])
                    id2 = int(v2["variant_id"])
                    if id1 == id2:
                        continue
                    dedup_key = (min(id1, id2), max(id1, id2))
                    if dedup_key in per_gene_pair_seen:
                        continue
                    per_gene_pair_seen.add(dedup_key)

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
                    row["data_source_support_count"] = len(model["data_source_ids"])
                    row["data_source_support_ids"] = "|".join(
                        str(x) for x in model["data_source_ids"]
                    )
                    row["data_source_support_names"] = "|".join(
                        model["data_source_names"]
                    )

                    row["variant_1_id"] = id1
                    row["variant_1_rsid"] = v1.get("rsid")
                    row["variant_1_chromosome"] = v1.get("chromosome")
                    row["variant_1_start"] = v1.get("position_start")
                    row["variant_1_end"] = v1.get("position_end")
                    row["variant_1_an"] = v1.get("an")
                    row["variant_1_grpmax"] = v1.get("grpmax")
                    row["variant_1_cadd_raw_score"] = v1.get("cadd_raw_score")
                    row["variant_1_cadd_phred"] = v1.get("cadd_phred")

                    row["variant_2_id"] = id2
                    row["variant_2_rsid"] = v2.get("rsid")
                    row["variant_2_chromosome"] = v2.get("chromosome")
                    row["variant_2_start"] = v2.get("position_start")
                    row["variant_2_end"] = v2.get("position_end")
                    row["variant_2_an"] = v2.get("an")
                    row["variant_2_grpmax"] = v2.get("grpmax")
                    row["variant_2_cadd_raw_score"] = v2.get("cadd_raw_score")
                    row["variant_2_cadd_phred"] = v2.get("cadd_phred")

                    variant_1_key = (_parse_int(v1.get("chromosome")), id1)
                    variant_2_key = (_parse_int(v2.get("chromosome")), id2)
                    v1_mol = (
                        molecular_effects_by_variant.get(
                            (int(variant_1_key[0]), int(variant_1_key[1]))
                        )
                        if variant_1_key[0] is not None
                        else None
                    )
                    v2_mol = (
                        molecular_effects_by_variant.get(
                            (int(variant_2_key[0]), int(variant_2_key[1]))
                        )
                        if variant_2_key[0] is not None
                        else None
                    )
                    v1_pred = (
                        predictions_by_variant.get(
                            (int(variant_1_key[0]), int(variant_1_key[1]))
                        )
                        if variant_1_key[0] is not None
                        else None
                    )
                    v2_pred = (
                        predictions_by_variant.get(
                            (int(variant_2_key[0]), int(variant_2_key[1]))
                        )
                        if variant_2_key[0] is not None
                        else None
                    )

                    row["variant_1_consequence_ids"] = (
                        v1_mol.get("consequence_ids") if v1_mol else None
                    )
                    row["variant_1_consequence_names"] = (
                        v1_mol.get("consequence_names") if v1_mol else None
                    )
                    row["variant_1_consequence_groups"] = (
                        v1_mol.get("consequence_groups") if v1_mol else None
                    )
                    row["variant_1_consequence_categories"] = (
                        v1_mol.get("consequence_categories") if v1_mol else None
                    )
                    row["variant_1_lof_confidences"] = (
                        v1_mol.get("lof_confidences") if v1_mol else None
                    )
                    row["variant_1_predictor_names"] = (
                        v1_pred.get("predictor_names") if v1_pred else None
                    )
                    row["variant_1_prediction_scores"] = (
                        v1_pred.get("prediction_scores") if v1_pred else None
                    )
                    row["variant_1_prediction_classifications"] = (
                        v1_pred.get("prediction_classifications") if v1_pred else None
                    )

                    row["variant_2_consequence_ids"] = (
                        v2_mol.get("consequence_ids") if v2_mol else None
                    )
                    row["variant_2_consequence_names"] = (
                        v2_mol.get("consequence_names") if v2_mol else None
                    )
                    row["variant_2_consequence_groups"] = (
                        v2_mol.get("consequence_groups") if v2_mol else None
                    )
                    row["variant_2_consequence_categories"] = (
                        v2_mol.get("consequence_categories") if v2_mol else None
                    )
                    row["variant_2_lof_confidences"] = (
                        v2_mol.get("lof_confidences") if v2_mol else None
                    )
                    row["variant_2_predictor_names"] = (
                        v2_pred.get("predictor_names") if v2_pred else None
                    )
                    row["variant_2_prediction_scores"] = (
                        v2_pred.get("prediction_scores") if v2_pred else None
                    )
                    row["variant_2_prediction_classifications"] = (
                        v2_pred.get("prediction_classifications") if v2_pred else None
                    )

                    row["snp_pair_seed_count"] = seed_count
                    row["snp_pair_seed_scope"] = _seed_scope(seed_count)
                    rows_out.append(row)

                    snp_rows_count += 1
                    if max_snp_pairs and snp_rows_count >= max_snp_pairs:
                        stop = True
                        snp_pairs_truncated = True
                        break

                processed_gene_pairs += 1
                if (
                    processed_gene_pairs == total_gene_pairs
                    or processed_gene_pairs % 5000 == 0
                ):
                    self.logger.log(
                        (
                            "Step 6 (snp pairs): "
                            f"{processed_gene_pairs:,}/{total_gene_pairs:,} gene pairs "
                            f"processed, {snp_rows_count:,} rows emitted."
                        ),
                        "INFO",
                    )
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
