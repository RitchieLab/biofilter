from __future__ import annotations

import re
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
    ETLDataSource,
)
from biofilter.modules.report.reports.base_report import ReportBase

_RSID_RE = re.compile(r"^rs\d+$", re.IGNORECASE)
_CHR_POS_RE = re.compile(r"^(?:chr)?([0-9xyXYmMtT]+)\s*[:;,\s]\s*(\d+)$")


def _norm(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _parse_int(value: Any) -> int | None:
    try:
        return int(str(value).strip()) if value is not None else None
    except Exception:
        return None


def _parse_chr(value: Any) -> int | None:
    s = (
        _norm(value)
        .lower()
        .replace("chromosome", "")
        .replace("chrom", "")
        .replace("chr", "")
        .strip()
    )
    if s == "x":
        return 23
    if s == "y":
        return 24
    if s in {"m", "mt", "mito", "mitochondria"}:
        return 25
    try:
        v = int(s)
        return v if 1 <= v <= 25 else None
    except Exception:
        return None


def _fmt_chr(c: int | None) -> str | None:
    if c is None:
        return None
    return {23: "chrX", 24: "chrY", 25: "chrMT"}.get(c, f"chr{c}")


def _as_ci_set(value: Any) -> set[str]:
    if value is None:
        return set()
    seq = value if isinstance(value, (list, tuple, set)) else [value]
    out: set[str] = set()
    for item in seq:
        s = _norm(item)
        for part in (s.split(",") if "," in s else [s]):
            p = part.strip().lower()
            if p:
                out.add(p)
    return out


def _parse_input_item(item: Any) -> dict[str, Any]:
    """Returns dict with keys: raw, kind ('rsid'|'chr_pos'|'invalid'), rsid, chromosome, position."""
    out: dict[str, Any] = {"raw": str(item), "kind": "invalid", "rsid": None, "chromosome": None, "position": None, "note": None}

    if isinstance(item, dict):
        chrom = item.get("chromosome") or item.get("chr")
        pos = item.get("position") or item.get("pos")
        c = _parse_chr(chrom)
        p = _parse_int(pos)
        if c and p and p > 0:
            out.update(kind="chr_pos", chromosome=c, position=p, raw=f"{_fmt_chr(c)}:{p}")
        else:
            out["note"] = "Invalid dict input — expected chromosome and position keys."
        return out

    s = _norm(item)
    out["raw"] = s
    if not s:
        out["note"] = "Empty input."
        return out

    if _RSID_RE.match(s):
        out.update(kind="rsid", rsid=s.lower())
        return out

    m = _CHR_POS_RE.match(s)
    if m:
        c = _parse_chr(m.group(1))
        p = _parse_int(m.group(2))
        if c and p and p > 0:
            out.update(kind="chr_pos", chromosome=c, position=p)
            return out

    out["note"] = "Expected rsID (rs12345) or chr:pos (chr1:100000)."
    return out


class VariantModelingReport(ReportBase):
    name = "variant_modeling"
    description = (
        "Given an input list of variants (rsID or chr:pos), maps each to overlapping genes "
        "(+ optional window_bp), connects genes via shared biological groups (pathways, GO, "
        "diseases, …), and generates all variant×variant interaction pairs where BOTH variants "
        "come from the input list. group_support_count is a weight reflecting how many distinct "
        "groups link the gene pair."
    )

    columns = [
        "variant_1_id",
        "variant_1_rsid",
        "variant_1_chr",
        "variant_1_pos",
        "gene_1_id",
        "gene_1_name",
        "variant_2_id",
        "variant_2_rsid",
        "variant_2_chr",
        "variant_2_pos",
        "gene_2_id",
        "gene_2_name",
        "group_support_count",
        "group_support_names",
        "data_source_support_count",
        "data_source_support_names",
        "build",
        "window_bp",
    ]

    @classmethod
    def available_columns(cls) -> list[str]:
        return cls.columns

    @classmethod
    def example_input(cls) -> dict:
        return {
            "input_data": ["rs429358", "rs7412", "chr2:21044574", "chr4:186486470"],
            "build": 38,
            "window_bp": 0,
            "group_entity_groups": ["Pathway"],
            "group_data_sources": None,
            "max_pairs": 1_000_000,
        }

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    def _vm(self) -> Table:
        from sqlalchemy import MetaData as _Meta

        meta = _Meta()
        return Table("variant_masters", meta, autoload_with=self.db.engine)

    def _query_variant_by_rsid(self, vm: Table, rsid: str) -> list[dict[str, Any]]:
        stmt = select(
            vm.c.variant_id,
            vm.c.rsid,
            vm.c.chromosome,
            vm.c.position_start,
            vm.c.position_end,
        ).where(func.lower(vm.c.rsid) == rsid.lower())
        rows = self.session.execute(stmt).mappings().all()
        seen: set[int] = set()
        out = []
        for row in rows:
            vid = int(row["variant_id"])
            if vid not in seen:
                seen.add(vid)
                out.append(dict(row))
        return out

    def _query_variants_at_position(
        self, vm: Table, chrom: int, pos: int
    ) -> list[dict[str, Any]]:
        stmt = (
            select(
                vm.c.variant_id,
                vm.c.rsid,
                vm.c.chromosome,
                vm.c.position_start,
                vm.c.position_end,
            )
            .where(
                and_(
                    vm.c.chromosome == chrom,
                    vm.c.position_start <= pos,
                    vm.c.position_end >= pos,
                )
            )
            .order_by(vm.c.position_start.asc(), vm.c.variant_id.asc())
        )
        if "allele_type" in vm.c:
            stmt = stmt.where(func.lower(vm.c.allele_type) == "snv")
        rows = self.session.execute(stmt).mappings().all()
        seen: set[int] = set()
        out = []
        for row in rows:
            vid = int(row["variant_id"])
            if vid not in seen:
                seen.add(vid)
                out.append(dict(row))
        return out

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
            .filter(EntityLocation.build == build)
            .filter(EntityLocation.chromosome == chrom)
            .filter(EntityLocation.start_pos <= end)
            .filter(EntityLocation.end_pos >= start)
        )
        if gene_group_filter:
            q = q.filter(func.lower(EntityGroup.name).in_(list(gene_group_filter)))
        out = []
        for row in q.all():
            out.append(
                {
                    "entity_id": int(row.entity_id),
                    "primary_name": row.primary_name,
                    "chromosome": int(row.chromosome),
                    "start_pos": int(row.start_pos),
                    "end_pos": int(row.end_pos),
                }
            )
        return out

    def _query_gene_to_groups(
        self,
        seed_gene_ids: set[int],
        gene_group_filter: set[str],
        group_group_filter: set[str],
        relationship_type_filter: set[str],
        group_data_source_ids_filter: set[int] | None,
    ) -> list[dict[str, Any]]:
        """Return all (gene_id, group_id, data_source_id) links for seed genes."""
        if not seed_gene_ids:
            return []

        rt = aliased(EntityRelationshipType)
        e1 = aliased(Entity)
        e2 = aliased(Entity)
        eg1 = aliased(EntityGroup)
        eg2 = aliased(EntityGroup)
        rows_out: list[dict[str, Any]] = []

        def _run_q(gene_col, group_col, gene_filter, group_filter):
            q = (
                self.session.query(
                    gene_col.label("gene_id"),
                    group_col.label("group_id"),
                    EntityRelationship.data_source_id.label("data_source_id"),
                )
                .join(rt, rt.id == EntityRelationship.relationship_type_id)
                .join(e1, e1.id == EntityRelationship.entity_1_id)
                .join(eg1, eg1.id == e1.group_id, isouter=True)
                .join(e2, e2.id == EntityRelationship.entity_2_id)
                .join(eg2, eg2.id == e2.group_id, isouter=True)
                .filter(gene_col.in_(list(seed_gene_ids)))
            )
            if relationship_type_filter:
                q = q.filter(func.lower(rt.code).in_(list(relationship_type_filter)))
            if gene_group_filter:
                q = q.filter(func.lower(gene_filter).in_(list(gene_group_filter)))
            if group_group_filter:
                q = q.filter(func.lower(group_filter).in_(list(group_group_filter)))
            if group_data_source_ids_filter is not None:
                q = q.filter(
                    EntityRelationship.data_source_id.in_(list(group_data_source_ids_filter))
                )
            for row in q.all():
                rows_out.append(
                    {
                        "gene_id": int(row.gene_id),
                        "group_id": int(row.group_id),
                        "data_source_id": _parse_int(row.data_source_id),
                    }
                )

        # gene is entity_1, group is entity_2
        _run_q(
            EntityRelationship.entity_1_id,
            EntityRelationship.entity_2_id,
            eg1.name,
            eg2.name,
        )
        # gene is entity_2, group is entity_1
        _run_q(
            EntityRelationship.entity_2_id,
            EntityRelationship.entity_1_id,
            eg2.name,
            eg1.name,
        )
        return rows_out

    def _query_groups_to_seed_genes(
        self,
        group_ids: set[int],
        seed_gene_ids: set[int],
        gene_group_filter: set[str],
        group_group_filter: set[str],
        relationship_type_filter: set[str],
        group_data_source_ids_filter: set[int] | None,
    ) -> list[dict[str, Any]]:
        """Return (gene_id, group_id) for group members that are also seed genes."""
        if not group_ids or not seed_gene_ids:
            return []

        rt = aliased(EntityRelationshipType)
        e1 = aliased(Entity)
        e2 = aliased(Entity)
        eg1 = aliased(EntityGroup)
        eg2 = aliased(EntityGroup)
        rows_out: list[dict[str, Any]] = []

        def _run_q(group_col, gene_col, group_filter, gene_filter):
            q = (
                self.session.query(
                    gene_col.label("gene_id"),
                    group_col.label("group_id"),
                    EntityRelationship.data_source_id.label("data_source_id"),
                )
                .join(rt, rt.id == EntityRelationship.relationship_type_id)
                .join(e1, e1.id == EntityRelationship.entity_1_id)
                .join(eg1, eg1.id == e1.group_id, isouter=True)
                .join(e2, e2.id == EntityRelationship.entity_2_id)
                .join(eg2, eg2.id == e2.group_id, isouter=True)
                .filter(group_col.in_(list(group_ids)))
                .filter(gene_col.in_(list(seed_gene_ids)))
            )
            if relationship_type_filter:
                q = q.filter(func.lower(rt.code).in_(list(relationship_type_filter)))
            if group_group_filter:
                q = q.filter(func.lower(group_filter).in_(list(group_group_filter)))
            if gene_group_filter:
                q = q.filter(func.lower(gene_filter).in_(list(gene_group_filter)))
            if group_data_source_ids_filter is not None:
                q = q.filter(
                    EntityRelationship.data_source_id.in_(list(group_data_source_ids_filter))
                )
            for row in q.all():
                rows_out.append(
                    {
                        "gene_id": int(row.gene_id),
                        "group_id": int(row.group_id),
                        "data_source_id": _parse_int(row.data_source_id),
                    }
                )

        _run_q(
            EntityRelationship.entity_1_id,
            EntityRelationship.entity_2_id,
            eg1.name,
            eg2.name,
        )
        _run_q(
            EntityRelationship.entity_2_id,
            EntityRelationship.entity_1_id,
            eg2.name,
            eg1.name,
        )
        return rows_out

    def _group_name_map(self, group_ids: set[int]) -> dict[int, str]:
        if not group_ids:
            return {}
        primary_alias = aliased(EntityAlias)
        q = (
            self.session.query(
                Entity.id.label("entity_id"),
                primary_alias.alias_value.label("primary_name"),
            )
            .join(
                primary_alias,
                and_(
                    primary_alias.entity_id == Entity.id,
                    primary_alias.is_primary.is_(True),
                ),
                isouter=True,
            )
            .filter(Entity.id.in_(list(group_ids)))
        )
        return {int(row.entity_id): (_norm(row.primary_name) or str(row.entity_id)) for row in q.all()}

    def _data_source_name_map(self) -> dict[int, str]:
        rows = self.session.query(ETLDataSource.id, ETLDataSource.name).all()
        return {int(row.id): _norm(row.name) for row in rows if row.id}

    def _resolve_group_filter(self, raw: Any) -> set[str]:
        if raw is None:
            return {"pathway", "pathways"}
        tokens = _as_ci_set(raw)
        q = self.session.query(EntityGroup.name).all()
        available = {_norm(r[0]).lower() for r in q if _norm(r[0])}
        resolved: set[str] = set()
        for t in tokens:
            if t in available:
                resolved.add(t)
            elif t + "s" in available:
                resolved.add(t + "s")
            elif t.rstrip("s") in available:
                resolved.add(t.rstrip("s"))
        if not resolved and tokens:
            options = ", ".join(sorted(available))
            raise ValueError(
                f"No valid group_entity_groups found for {sorted(tokens)}. "
                f"Available: {options}"
            )
        return resolved or {"pathway", "pathways"}

    def _resolve_data_source_filter(
        self, raw: Any, ds_name_map: dict[int, str]
    ) -> set[int] | None:
        tokens = _as_ci_set(raw)
        if not tokens:
            return None
        name_to_id = {v.lower(): k for k, v in ds_name_map.items()}
        resolved: set[int] = set()
        for t in tokens:
            if t in name_to_id:
                resolved.add(name_to_id[t])
        if not resolved:
            options = ", ".join(sorted(ds_name_map.values()))
            raise ValueError(
                f"No valid group_data_sources found for {sorted(tokens)}. "
                f"Available: {options}"
            )
        return resolved

    # ------------------------------------------------------------------
    # run()
    # ------------------------------------------------------------------

    def run(self) -> pd.DataFrame:
        input_raw = self.param("input_data", required=True)
        input_list = self.resolve_input_list(input_raw, param_name="input_data")

        build = int(self.param("build", 38) or 38)
        window_bp = max(0, int(self.param("window_bp", 0) or 0))
        max_pairs = max(0, int(self.param("max_pairs", 1_000_000) or 1_000_000))
        gene_group_filter = _as_ci_set(self.param("gene_entity_groups", ["Gene", "Genes"]))
        relationship_type_filter = _as_ci_set(self.param("relationship_types", None))

        group_group_filter = self._resolve_group_filter(
            self.param("group_entity_groups", None)
        )

        ds_name_map = self._data_source_name_map()
        group_data_source_ids_filter = self._resolve_data_source_filter(
            self.param("group_data_sources", None), ds_name_map
        )

        vm = self._vm()

        # ------------------------------------------------------------------
        # Step 1: parse and look up input variants
        # ------------------------------------------------------------------
        parsed = [_parse_input_item(item) for item in input_list]

        variant_by_id: dict[int, dict[str, Any]] = {}
        not_found: list[str] = []

        for item in parsed:
            if item["kind"] == "invalid":
                self.logger.log(
                    f"Skipped invalid input '{item['raw']}': {item['note']}", "WARNING"
                )
                continue
            if item["kind"] == "rsid":
                found = self._query_variant_by_rsid(vm, item["rsid"])
            else:
                found = self._query_variants_at_position(
                    vm, item["chromosome"], item["position"]
                )
            if not found:
                not_found.append(item["raw"])
                self.logger.log(f"No variant found for input '{item['raw']}'", "WARNING")
            for v in found:
                variant_by_id[int(v["variant_id"])] = v

        if not variant_by_id:
            self.logger.log("No variants matched any input — returning empty DataFrame.", "WARNING")
            return pd.DataFrame(columns=self.columns)

        # ------------------------------------------------------------------
        # Step 2: map each variant to overlapping genes
        # ------------------------------------------------------------------
        gene_to_variants: dict[int, list[dict[str, Any]]] = {}
        gene_name_map: dict[int, str] = {}
        _pos_cache: dict[tuple[int, int, int], list[dict[str, Any]]] = {}

        for variant in variant_by_id.values():
            chrom = int(variant["chromosome"])
            vstart = max(1, int(variant["position_start"]) - window_bp)
            vend = int(variant["position_end"]) + window_bp
            cache_key = (chrom, vstart, vend)

            if cache_key not in _pos_cache:
                _pos_cache[cache_key] = self._query_genes_overlap(
                    chrom=chrom,
                    start=vstart,
                    end=vend,
                    build=build,
                    gene_group_filter=gene_group_filter,
                )
            for gene in _pos_cache[cache_key]:
                gid = int(gene["entity_id"])
                gene_to_variants.setdefault(gid, [])
                if not any(v["variant_id"] == variant["variant_id"] for v in gene_to_variants[gid]):
                    gene_to_variants[gid].append(variant)
                gene_name_map[gid] = _norm(gene["primary_name"]) or str(gid)

        seed_gene_ids = set(gene_to_variants.keys())

        if not seed_gene_ids:
            self.logger.log("No genes found for input variants — returning empty DataFrame.", "WARNING")
            return pd.DataFrame(columns=self.columns)

        # ------------------------------------------------------------------
        # Step 3: seed genes → group memberships
        # ------------------------------------------------------------------
        gene_group_links = self._query_gene_to_groups(
            seed_gene_ids=seed_gene_ids,
            gene_group_filter=gene_group_filter,
            group_group_filter=group_group_filter,
            relationship_type_filter=relationship_type_filter,
            group_data_source_ids_filter=group_data_source_ids_filter,
        )
        group_ids_found = {int(x["group_id"]) for x in gene_group_links}

        # ------------------------------------------------------------------
        # Step 4: groups → back to seed genes (complete group membership)
        # ------------------------------------------------------------------
        group_gene_links = self._query_groups_to_seed_genes(
            group_ids=group_ids_found,
            seed_gene_ids=seed_gene_ids,
            gene_group_filter=gene_group_filter,
            group_group_filter=group_group_filter,
            relationship_type_filter=relationship_type_filter,
            group_data_source_ids_filter=group_data_source_ids_filter,
        )

        # Build: group_id → set of seed gene_ids in that group
        group_to_genes: dict[int, set[int]] = {}
        group_to_ds: dict[int, set[int]] = {}
        for link in gene_group_links + group_gene_links:
            gid = int(link["group_id"])
            eid = int(link["gene_id"])
            group_to_genes.setdefault(gid, set()).add(eid)
            ds = _parse_int(link.get("data_source_id"))
            if ds is not None:
                group_to_ds.setdefault(gid, set()).add(ds)

        # ------------------------------------------------------------------
        # Step 5: Gene×Gene pairs — accumulate group support
        # ------------------------------------------------------------------
        pair_to_groups: dict[tuple[int, int], set[int]] = {}
        pair_to_ds: dict[tuple[int, int], set[int]] = {}

        for group_id, members in group_to_genes.items():
            seed_members = sorted(members & seed_gene_ids)
            if len(seed_members) < 2:
                continue
            for g1, g2 in combinations(seed_members, 2):
                key = (min(g1, g2), max(g1, g2))
                pair_to_groups.setdefault(key, set()).add(group_id)
                pair_to_ds.setdefault(key, set()).update(group_to_ds.get(group_id, set()))

        if not pair_to_groups:
            self.logger.log("No gene pairs found — returning empty DataFrame.", "WARNING")
            return pd.DataFrame(columns=self.columns)

        group_names = self._group_name_map(
            {gid for groups in pair_to_groups.values() for gid in groups}
        )

        # ------------------------------------------------------------------
        # Step 6: estimate pairs before materialising (safety check)
        # ------------------------------------------------------------------
        estimated = sum(
            len(gene_to_variants.get(g1, [])) * len(gene_to_variants.get(g2, []))
            for g1, g2 in pair_to_groups
        )
        if max_pairs and estimated > max_pairs:
            self.logger.log(
                f"Estimated {estimated:,} variant pairs exceed max_pairs={max_pairs:,}. "
                "Reduce input size, apply stricter group filters, or increase max_pairs.",
                "ERROR",
            )
            return pd.DataFrame(
                [
                    {
                        "resolution_status": "pair_limit_exceeded",
                        "estimated_pairs": estimated,
                        "max_pairs": max_pairs,
                        "suggestion": (
                            f"Estimated {estimated:,} pairs exceed max_pairs={max_pairs:,}. "
                            "Try: stricter group_entity_groups, group_data_sources filter, "
                            "or increase max_pairs."
                        ),
                    }
                ]
            )

        # ------------------------------------------------------------------
        # Step 7: Variant×Variant pairs
        # ------------------------------------------------------------------
        rows: list[dict[str, Any]] = []
        seen_pairs: set[tuple[int, int]] = set()

        for (g1, g2), support_group_ids in sorted(pair_to_groups.items()):
            v1_list = gene_to_variants.get(g1, [])
            v2_list = gene_to_variants.get(g2, [])
            if not v1_list or not v2_list:
                continue

            support_names = "|".join(
                group_names.get(gid, str(gid)) for gid in sorted(support_group_ids)
            )
            ds_ids = sorted(pair_to_ds.get((g1, g2), set()))
            ds_names = "|".join(ds_name_map.get(did, str(did)) for did in ds_ids)

            for v1 in v1_list:
                for v2 in v2_list:
                    id1, id2 = int(v1["variant_id"]), int(v2["variant_id"])
                    if id1 == id2:
                        continue
                    dedup = (min(id1, id2), max(id1, id2))
                    if dedup in seen_pairs:
                        continue
                    seen_pairs.add(dedup)
                    rows.append(
                        {
                            "variant_1_id": id1,
                            "variant_1_rsid": v1.get("rsid"),
                            "variant_1_chr": _fmt_chr(v1.get("chromosome")),
                            "variant_1_pos": v1.get("position_start"),
                            "gene_1_id": g1,
                            "gene_1_name": gene_name_map.get(g1, str(g1)),
                            "variant_2_id": id2,
                            "variant_2_rsid": v2.get("rsid"),
                            "variant_2_chr": _fmt_chr(v2.get("chromosome")),
                            "variant_2_pos": v2.get("position_start"),
                            "gene_2_id": g2,
                            "gene_2_name": gene_name_map.get(g2, str(g2)),
                            "group_support_count": len(support_group_ids),
                            "group_support_names": support_names,
                            "data_source_support_count": len(ds_ids),
                            "data_source_support_names": ds_names,
                            "build": build,
                            "window_bp": window_bp,
                        }
                    )

        df = pd.DataFrame(rows).reindex(columns=self.columns)
        if not df.empty:
            df = df.sort_values(
                by=["group_support_count", "gene_1_name", "gene_2_name"],
                ascending=[False, True, True],
                na_position="last",
            ).reset_index(drop=True)

        self.results = df
        return df
