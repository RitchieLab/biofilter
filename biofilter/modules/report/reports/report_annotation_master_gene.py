from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd
from sqlalchemy import MetaData, Table, and_, func, or_
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import (
    Entity,
    EntityAlias,
    EntityGroup,
    EntityLocation,
    EntityRelationship,
    GeneGroup,
    GeneGroupMembership,
    GeneLocusGroup,
    GeneLocusType,
    GeneMaster,
    OmicStatus,
)
from biofilter.modules.report.reports.base_report import ReportBase


def _norm(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _parse_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = _norm(value).lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _alias_rank(alias_type: Optional[str], is_primary: Optional[bool]) -> int:
    if is_primary:
        return 0
    t = _norm(alias_type).lower()
    if t in {"symbol", "preferred"}:
        return 1
    if t == "code":
        return 2
    if t in {"synonym", "name"}:
        return 3
    return 4


@dataclass
class AliasRow:
    alias_value: str
    alias_type: Optional[str]
    xref_source: Optional[str]
    is_primary: Optional[bool]


class AnnotationMasterGeneReport(ReportBase):
    name = "annotation_master_gene"
    description = (
        "Compact gene annotation report for input genes/aliases. Returns canonical "
        "IDs, gene metadata, build38 coordinates, relationship counts by related "
        "entity group, total relationships, and optional variant count in gene range."
    )

    columns = [
        "input_value",
        "input_matched_alias",
        "entity_id",
        "gene_symbol",
        "hgnc_id",
        "ensembl_id",
        "entrez_id",
        "hgnc_status",
        "omic_status",
        "gene_locus_group",
        "gene_locus_type",
        "gene_groups",
        "build",
        "chromosome",
        "start_position",
        "end_position",
        "entity_relationships_by_group",
        "total_entity_relationships",
        "variant_count_in_gene_range",
        "other_aliases",
        "status",
        "note",
    ]

    @classmethod
    def available_columns(cls) -> list[str]:
        return cls.columns

    @classmethod
    def explain(cls) -> str:
        return str("DOC IN MD FILE")

    @classmethod
    def example_input(cls):
        return {
            "input_data": "__ALL__",
            "include_relationships": True,
            "include_variant_summary": True,
            "emit_not_found_rows": True,
        }

    @staticmethod
    def _cast_nullable_int_columns(df: pd.DataFrame) -> pd.DataFrame:
        int_cols = [
            "entity_id",
            "build",
            "chromosome",
            "start_position",
            "end_position",
            "total_entity_relationships",
            "variant_count_in_gene_range",
        ]
        for col in int_cols:
            if col not in df.columns:
                continue
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        return df

    def _table(self, table_name: str) -> Table:
        metadata = MetaData()
        return Table(table_name, metadata, autoload_with=self.db.engine)

    @staticmethod
    def _pick_alias(
        aliases: list[AliasRow],
        *,
        source: str,
        alias_type: Optional[str] = None,
        startswith: Optional[str] = None,
    ) -> Optional[str]:
        source_up = source.upper()
        alias_type_low = _norm(alias_type).lower() if alias_type else None
        prefix = _norm(startswith)

        candidates: list[str] = []
        for a in aliases:
            src = _norm(a.xref_source).upper()
            if src != source_up:
                continue
            t = _norm(a.alias_type).lower()
            if alias_type_low and t != alias_type_low:
                continue
            v = _norm(a.alias_value)
            if not v:
                continue
            if prefix and not v.startswith(prefix):
                continue
            candidates.append(v)

        if not candidates:
            return None
        return sorted(set(candidates))[0]

    @staticmethod
    def _is_all_input(input_data_raw: Any) -> bool:
        if isinstance(input_data_raw, str):
            return _norm(input_data_raw).lower() == "__all__"
        if isinstance(input_data_raw, (list, tuple, set)):
            items = [_norm(v) for v in input_data_raw if _norm(v)]
            return len(items) == 1 and items[0].lower() == "__all__"
        return False

    def _resolve_all_gene_entities(
        self,
    ) -> tuple[list[dict[str, str]], dict[str, dict[str, Any]], list[int]]:
        primary_alias = aliased(EntityAlias)

        rows = (
            self.session.query(
                Entity.id.label("entity_id"),
                GeneMaster.symbol.label("gene_symbol"),
                primary_alias.alias_value.label("primary_name"),
            )
            .join(GeneMaster, GeneMaster.entity_id == Entity.id)
            .join(EntityGroup, Entity.group_id == EntityGroup.id, isouter=True)
            .join(
                primary_alias,
                and_(
                    primary_alias.entity_id == Entity.id,
                    primary_alias.is_primary.is_(True),
                ),
                isouter=True,
            )
            .filter(func.lower(EntityGroup.name).in_(["gene", "genes"]))
            .order_by(Entity.id)
            .all()
        )

        input_entries: list[dict[str, str]] = []
        resolved_by_key: dict[str, dict[str, Any]] = {}
        entity_ids: list[int] = []

        for r in rows:
            entity_id = int(r.entity_id)
            display_alias = _norm(r.primary_name) or _norm(r.gene_symbol) or f"ENTITY:{entity_id}"  # noqa: E501
            input_key = f"__entity__:{entity_id}"
            input_entries.append({"input_value": display_alias, "input_key": input_key})
            resolved_by_key[input_key] = {
                "entity_id": entity_id,
                "matched_alias": display_alias,
                "alias_type": "preferred",
                "is_primary": True,
                "primary_name": _norm(r.primary_name) or None,
            }
            entity_ids.append(entity_id)

        return input_entries, resolved_by_key, entity_ids

    def _resolve_input_entities(
        self,
        input_entries: list[dict[str, str]],
    ) -> tuple[dict[str, dict[str, Any]], list[int]]:
        input_keys = sorted({e["input_key"] for e in input_entries})
        input_expr = func.lower(func.coalesce(EntityAlias.alias_norm, EntityAlias.alias_value))  # noqa: E501
        primary_alias = aliased(EntityAlias)

        rows = (
            self.session.query(
                input_expr.label("input_key"),
                EntityAlias.alias_value.label("matched_alias"),
                EntityAlias.alias_type.label("alias_type"),
                EntityAlias.is_primary.label("is_primary"),
                Entity.id.label("entity_id"),
                primary_alias.alias_value.label("primary_name"),
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
            .filter(input_expr.in_(input_keys))
            .filter(func.lower(EntityGroup.name).in_(["gene", "genes"]))
            .all()
        )

        best_by_key: dict[str, dict[str, Any]] = {}
        for r in rows:
            k = str(r.input_key)
            candidate = {
                "entity_id": int(r.entity_id),
                "matched_alias": r.matched_alias,
                "alias_type": r.alias_type,
                "is_primary": bool(r.is_primary) if r.is_primary is not None else False,
                "primary_name": r.primary_name,
            }
            score = (
                _alias_rank(candidate["alias_type"], candidate["is_primary"]),
                str(candidate["matched_alias"] or ""),
                candidate["entity_id"],
            )

            current = best_by_key.get(k)
            if current is None:
                best_by_key[k] = {**candidate, "_score": score}
                continue

            if score < current["_score"]:
                best_by_key[k] = {**candidate, "_score": score}

        for k in list(best_by_key.keys()):
            best_by_key[k].pop("_score", None)

        entity_ids = sorted({v["entity_id"] for v in best_by_key.values()})
        return best_by_key, entity_ids

    def _fetch_gene_core(self, entity_ids: list[int]) -> dict[int, dict[str, Any]]:
        if not entity_ids:
            return {}

        rows = (
            self.session.query(
                GeneMaster.id.label("gene_master_id"),
                GeneMaster.entity_id.label("entity_id"),
                GeneMaster.symbol.label("symbol"),
                GeneMaster.hgnc_status.label("hgnc_status"),
                OmicStatus.name.label("omic_status"),
                GeneLocusGroup.name.label("gene_locus_group"),
                GeneLocusType.name.label("gene_locus_type"),
            )
            .join(OmicStatus, OmicStatus.id == GeneMaster.omic_status_id, isouter=True)
            .join(
                GeneLocusGroup,
                GeneLocusGroup.id == GeneMaster.locus_group_id,
                isouter=True,
            )
            .join(
                GeneLocusType,
                GeneLocusType.id == GeneMaster.locus_type_id,
                isouter=True,
            )
            .filter(GeneMaster.entity_id.in_(entity_ids))
            .all()
        )

        out: dict[int, dict[str, Any]] = {}
        for r in rows:
            eid = int(r.entity_id)
            if eid in out:
                continue
            out[eid] = {
                "gene_master_id": int(r.gene_master_id),
                "gene_symbol": r.symbol,
                "hgnc_status": r.hgnc_status,
                "omic_status": r.omic_status,
                "gene_locus_group": r.gene_locus_group,
                "gene_locus_type": r.gene_locus_type,
            }
        return out

    def _fetch_locations_build38(self, entity_ids: list[int]) -> dict[int, dict[str, Any]]:
        if not entity_ids:
            return {}

        rows = (
            self.session.query(
                EntityLocation.entity_id.label("entity_id"),
                EntityLocation.build.label("build"),
                EntityLocation.chromosome.label("chromosome"),
                EntityLocation.start_pos.label("start_position"),
                EntityLocation.end_pos.label("end_position"),
            )
            .filter(EntityLocation.entity_id.in_(entity_ids))
            .filter(EntityLocation.build == 38)
            .order_by(EntityLocation.entity_id, EntityLocation.start_pos)
            .all()
        )

        out: dict[int, dict[str, Any]] = {}
        for r in rows:
            eid = int(r.entity_id)
            if eid in out:
                continue
            out[eid] = {
                "build": int(r.build),
                "chromosome": int(r.chromosome),
                "start_position": int(r.start_position),
                "end_position": int(r.end_position),
            }
        return out

    def _fetch_aliases(self, entity_ids: list[int]) -> dict[int, list[AliasRow]]:
        if not entity_ids:
            return {}

        rows = (
            self.session.query(
                EntityAlias.entity_id,
                EntityAlias.alias_value,
                EntityAlias.alias_type,
                EntityAlias.xref_source,
                EntityAlias.is_primary,
            )
            .filter(EntityAlias.entity_id.in_(entity_ids))
            .all()
        )

        out: dict[int, list[AliasRow]] = defaultdict(list)
        for r in rows:
            out[int(r.entity_id)].append(
                AliasRow(
                    alias_value=_norm(r.alias_value),
                    alias_type=r.alias_type,
                    xref_source=r.xref_source,
                    is_primary=r.is_primary,
                )
            )
        return dict(out)

    def _fetch_gene_groups(self, entity_ids: list[int]) -> dict[int, list[str]]:
        if not entity_ids:
            return {}

        rows = (
            self.session.query(
                GeneMaster.entity_id.label("entity_id"),
                GeneGroup.name.label("group_name"),
            )
            .join(GeneGroupMembership, GeneGroupMembership.gene_id == GeneMaster.id)
            .join(GeneGroup, GeneGroup.id == GeneGroupMembership.group_id)
            .filter(GeneMaster.entity_id.in_(entity_ids))
            .all()
        )

        out: dict[int, set[str]] = defaultdict(set)
        for r in rows:
            out[int(r.entity_id)].add(str(r.group_name))
        return {eid: sorted(names) for eid, names in out.items()}

    def _fetch_relationship_summary(
        self,
        entity_ids: list[int],
        include_relationships: bool,
    ) -> tuple[dict[int, list[tuple[str, int]]], dict[int, int]]:
        by_group: dict[int, list[tuple[str, int]]] = {}
        totals: dict[int, int] = {eid: 0 for eid in entity_ids}

        if not include_relationships or not entity_ids:
            return by_group, totals

        rows = (
            self.session.query(
                EntityRelationship.entity_1_id.label("entity_1_id"),
                EntityRelationship.entity_2_id.label("entity_2_id"),
                EntityRelationship.entity_1_group_id.label("entity_1_group_id"),
                EntityRelationship.entity_2_group_id.label("entity_2_group_id"),
            )
            .filter(
                or_(
                    EntityRelationship.entity_1_id.in_(entity_ids),
                    EntityRelationship.entity_2_id.in_(entity_ids),
                )
            )
            .all()
        )

        group_ids = set()
        for r in rows:
            if r.entity_1_group_id is not None:
                group_ids.add(int(r.entity_1_group_id))
            if r.entity_2_group_id is not None:
                group_ids.add(int(r.entity_2_group_id))

        group_name_map = {}
        if group_ids:
            group_rows = (
                self.session.query(EntityGroup.id, EntityGroup.name)
                .filter(EntityGroup.id.in_(sorted(group_ids)))
                .all()
            )
            group_name_map = {int(gid): str(name) for gid, name in group_rows}

        counts: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        entity_id_set = set(entity_ids)
        for r in rows:
            e1 = int(r.entity_1_id)
            e2 = int(r.entity_2_id)
            g1 = int(r.entity_1_group_id) if r.entity_1_group_id is not None else None
            g2 = int(r.entity_2_group_id) if r.entity_2_group_id is not None else None

            if e1 in entity_id_set:
                related_name = group_name_map.get(g2, "Unknown")
                counts[e1][related_name] += 1

            if e2 in entity_id_set and e2 != e1:
                related_name = group_name_map.get(g1, "Unknown")
                counts[e2][related_name] += 1

        for eid in entity_ids:
            pairs = list(counts[eid].items())
            pairs.sort(key=lambda x: (-x[1], x[0]))
            by_group[eid] = pairs
            totals[eid] = int(sum(v for _, v in pairs))

        return by_group, totals

    def _fetch_variant_counts(
        self,
        entity_ids: list[int],
        include_variant_summary: bool,
        location_by_entity: dict[int, dict[str, Any]],
    ) -> dict[int, int]:
        if not include_variant_summary or not entity_ids:
            return {}

        loc_entity_ids = sorted(set(entity_ids).intersection(location_by_entity.keys()))
        if not loc_entity_ids:
            return {}

        try:
            variant_masters = self._table("variant_masters")
        except Exception:
            self.logger.log(
                "variant_masters table not available. variant_count_in_gene_range will be null.",  # noqa: E501
                "WARNING",
            )
            return {}

        rows = (
            self.session.query(
                EntityLocation.entity_id.label("entity_id"),
                func.count(func.distinct(variant_masters.c.variant_id)).label(
                    "variant_count"
                ),
            )
            .join(
                variant_masters,
                and_(
                    variant_masters.c.chromosome == EntityLocation.chromosome,
                    variant_masters.c.position_start <= EntityLocation.end_pos,
                    variant_masters.c.position_end >= EntityLocation.start_pos,
                ),
            )
            .filter(EntityLocation.entity_id.in_(loc_entity_ids))
            .filter(EntityLocation.build == 38)
            .group_by(EntityLocation.entity_id)
            .all()
        )
        return {int(r.entity_id): int(r.variant_count or 0) for r in rows}

    def run(self):
        input_data_raw = self.param("input_data", required=True)

        include_relationships = _parse_bool(
            self.param("include_relationships", True), True
        )
        include_variant_summary = _parse_bool(
            self.param("include_variant_summary", True), True
        )
        emit_not_found_rows = _parse_bool(
            self.param("emit_not_found_rows", True), True
        )

        all_mode = self._is_all_input(input_data_raw)
        if all_mode:
            input_entries, resolved_by_key, entity_ids = self._resolve_all_gene_entities()
            if not input_entries:
                raise ValueError("No gene entities found in database for input_data='__ALL__'.")  # noqa: E501
        else:
            input_data = self.resolve_input_list(input_data_raw, param_name="input_data")
            input_entries = []
            for item in input_data:
                raw = _norm(item)
                if not raw:
                    continue
                input_entries.append({"input_value": raw, "input_key": raw.lower()})

            if not input_entries:
                raise ValueError("input_data must contain at least one non-empty value.")

            resolved_by_key, entity_ids = self._resolve_input_entities(input_entries)
        gene_core_by_entity = self._fetch_gene_core(entity_ids)
        location_by_entity = self._fetch_locations_build38(entity_ids)
        aliases_by_entity = self._fetch_aliases(entity_ids)
        gene_groups_by_entity = self._fetch_gene_groups(entity_ids)
        rel_by_group, rel_totals = self._fetch_relationship_summary(
            entity_ids, include_relationships=include_relationships
        )
        variant_counts = self._fetch_variant_counts(
            entity_ids=entity_ids,
            include_variant_summary=include_variant_summary,
            location_by_entity=location_by_entity,
        )

        records: list[dict[str, Any]] = []
        for entry in input_entries:
            input_value = entry["input_value"]
            input_key = entry["input_key"]
            resolved = resolved_by_key.get(input_key)

            if not resolved:
                if not emit_not_found_rows:
                    continue
                records.append(
                    {
                        "input_value": input_value,
                        "input_matched_alias": None,
                        "entity_id": None,
                        "gene_symbol": None,
                        "hgnc_id": None,
                        "ensembl_id": None,
                        "entrez_id": None,
                        "hgnc_status": None,
                        "omic_status": None,
                        "gene_locus_group": None,
                        "gene_locus_type": None,
                        "gene_groups": [],
                        "build": None,
                        "chromosome": None,
                        "start_position": None,
                        "end_position": None,
                        "entity_relationships_by_group": [],
                        "total_entity_relationships": 0,
                        "variant_count_in_gene_range": None,
                        "other_aliases": [],
                        "status": "not_found",
                        "note": "Input not resolved to a Gene entity.",
                    }
                )
                continue

            entity_id = int(resolved["entity_id"])
            gene_core = gene_core_by_entity.get(entity_id, {})
            location = location_by_entity.get(entity_id, {})
            aliases = aliases_by_entity.get(entity_id, [])
            gene_groups = gene_groups_by_entity.get(entity_id, [])

            hgnc_id = self._pick_alias(
                aliases, source="HGNC", alias_type="code", startswith="HGNC:"
            )
            if not hgnc_id:
                hgnc_id = self._pick_alias(aliases, source="HGNC", alias_type="code")

            ensembl_id = self._pick_alias(aliases, source="ENSEMBL", alias_type="code")
            entrez_id = self._pick_alias(aliases, source="ENTREZ", alias_type="code")

            symbol = (
                _norm(gene_core.get("gene_symbol"))
                or _norm(resolved.get("primary_name"))
                or _norm(resolved.get("matched_alias"))
            )
            symbol = symbol or None

            canonical_values = {v for v in [symbol, hgnc_id, ensembl_id, entrez_id] if v}
            other_aliases = sorted(
                {
                    _norm(a.alias_value)
                    for a in aliases
                    if _norm(a.alias_value) and _norm(a.alias_value) not in canonical_values
                }
            )

            notes: list[str] = []
            status = "ok"
            if not gene_core:
                status = "partial"
                notes.append("Gene resolved but no GeneMaster row found.")
            if not location:
                status = "partial"
                notes.append("No EntityLocation build=38 found for this gene.")

            if include_relationships:
                rel_list = rel_by_group.get(entity_id, [])
                rel_total = rel_totals.get(entity_id, 0)
            else:
                rel_list = []
                rel_total = 0

            if include_variant_summary:
                if entity_id in location_by_entity:
                    variant_count = variant_counts.get(entity_id, 0)
                else:
                    variant_count = None
            else:
                variant_count = None

            records.append(
                {
                    "input_value": input_value,
                    "input_matched_alias": resolved.get("matched_alias"),
                    "entity_id": entity_id,
                    "gene_symbol": symbol,
                    "hgnc_id": hgnc_id,
                    "ensembl_id": ensembl_id,
                    "entrez_id": entrez_id,
                    "hgnc_status": gene_core.get("hgnc_status"),
                    "omic_status": gene_core.get("omic_status"),
                    "gene_locus_group": gene_core.get("gene_locus_group"),
                    "gene_locus_type": gene_core.get("gene_locus_type"),
                    "gene_groups": gene_groups,
                    "build": location.get("build"),
                    "chromosome": location.get("chromosome"),
                    "start_position": location.get("start_position"),
                    "end_position": location.get("end_position"),
                    "entity_relationships_by_group": rel_list,
                    "total_entity_relationships": rel_total,
                    "variant_count_in_gene_range": variant_count,
                    "other_aliases": other_aliases,
                    "status": status,
                    "note": " ".join(notes) if notes else None,
                }
            )

        out = pd.DataFrame(records)
        out = out.reindex(columns=self.columns)
        out = self._cast_nullable_int_columns(out)
        self.results = out
        return out.reset_index(drop=True)
