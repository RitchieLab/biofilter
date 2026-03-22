from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import (
    ETLDataSource,
    ETLSourceSystem,
    Entity,
    EntityAlias,
    EntityGroup,
    EntityRelationship,
    GOMaster,
    GORelation,
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


def _parse_int(value: Any, default: int) -> int:
    try:
        v = int(str(value).strip())
        return v if v >= 0 else default
    except Exception:
        return default


def _alias_rank(alias_type: Optional[str], is_primary: Optional[bool]) -> int:
    if is_primary:
        return 0
    t = _norm(alias_type).lower()
    if t in {"code", "preferred"}:
        return 1
    if t in {"name", "label", "synonym"}:
        return 2
    return 3


@dataclass
class AliasRow:
    alias_value: str
    alias_type: Optional[str]
    xref_source: Optional[str]
    is_primary: Optional[bool]


class AnnotationMasterGOReport(ReportBase):
    name = "annotation_master_go"
    description = (
        "Compact GO annotation report with GOMaster identity, namespace, optional "
        "GO DAG relation summary/details, and optional relationship summary by "
        "entity group."
    )

    columns = [
        "input_value",
        "input_matched_alias",
        "entity_id",
        "go_master_id",
        "go_id",
        "go_name",
        "go_namespace",
        "go_source_system",
        "go_data_source",
        "go_etl_package_id",
        "go_parent_count",
        "go_child_count",
        "go_parent_relation_types",
        "go_child_relation_types",
        "go_parent_ids",
        "go_child_ids",
        "entity_relationships_by_group",
        "total_entity_relationships",
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
            "emit_not_found_rows": True,
            "include_aliases": True,
            "include_go_relation_summary": True,
            "include_go_relation_details": False,
            "max_go_terms_per_side": 20,
            "include_relationships": False,
        }

    @staticmethod
    def _cast_nullable_int_columns(df: pd.DataFrame) -> pd.DataFrame:
        int_cols = [
            "entity_id",
            "go_master_id",
            "go_etl_package_id",
            "go_parent_count",
            "go_child_count",
            "total_entity_relationships",
        ]
        for col in int_cols:
            if col not in df.columns:
                continue
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        return df

    @staticmethod
    def _is_all_input(input_data_raw: Any) -> bool:
        if isinstance(input_data_raw, str):
            return _norm(input_data_raw).lower() == "__all__"
        if isinstance(input_data_raw, (list, tuple, set)):
            items = [_norm(v) for v in input_data_raw if _norm(v)]
            return len(items) == 1 and items[0].lower() == "__all__"
        return False

    def _resolve_all_go_entities(
        self,
    ) -> tuple[list[dict[str, str]], dict[str, dict[str, Any]], list[int]]:
        primary_alias = aliased(EntityAlias)

        rows = (
            self.session.query(
                Entity.id.label("entity_id"),
                GOMaster.go_id.label("go_id"),
                GOMaster.name.label("go_name"),
                primary_alias.alias_value.label("primary_name"),
            )
            .join(GOMaster, GOMaster.entity_id == Entity.id)
            .join(EntityGroup, Entity.group_id == EntityGroup.id, isouter=True)
            .join(
                primary_alias,
                and_(
                    primary_alias.entity_id == Entity.id,
                    primary_alias.is_primary.is_(True),
                ),
                isouter=True,
            )
            .filter(func.lower(EntityGroup.name).in_(["gene ontology", "go"]))
            .order_by(Entity.id)
            .all()
        )

        input_entries: list[dict[str, str]] = []
        resolved_by_key: dict[str, dict[str, Any]] = {}
        entity_ids: list[int] = []

        for r in rows:
            entity_id = int(r.entity_id)
            display_alias = (
                _norm(r.primary_name)
                or _norm(r.go_id)
                or _norm(r.go_name)
                or f"ENTITY:{entity_id}"
            )
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
            .filter(func.lower(EntityGroup.name).in_(["gene ontology", "go"]))
            .all()
        )

        best_by_key: dict[str, dict[str, Any]] = {}
        for r in rows:
            key = str(r.input_key)
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
            current = best_by_key.get(key)
            if current is None or score < current["_score"]:
                best_by_key[key] = {**candidate, "_score": score}

        for k in list(best_by_key.keys()):
            best_by_key[k].pop("_score", None)

        entity_ids = sorted({v["entity_id"] for v in best_by_key.values()})
        return best_by_key, entity_ids

    def _fetch_go_core(self, entity_ids: list[int]) -> dict[int, dict[str, Any]]:
        if not entity_ids:
            return {}

        rows = (
            self.session.query(
                GOMaster.entity_id.label("entity_id"),
                GOMaster.id.label("go_master_id"),
                GOMaster.go_id.label("go_id"),
                GOMaster.name.label("go_name"),
                GOMaster.namespace.label("go_namespace"),
                GOMaster.etl_package_id.label("go_etl_package_id"),
                ETLDataSource.name.label("go_data_source"),
                ETLSourceSystem.name.label("go_source_system"),
            )
            .join(ETLDataSource, ETLDataSource.id == GOMaster.data_source_id, isouter=True)  # noqa: E501
            .join(
                ETLSourceSystem,
                ETLSourceSystem.id == ETLDataSource.source_system_id,
                isouter=True,
            )
            .filter(GOMaster.entity_id.in_(entity_ids))
            .order_by(GOMaster.entity_id, GOMaster.go_id)
            .all()
        )

        out: dict[int, dict[str, Any]] = {}
        for r in rows:
            eid = int(r.entity_id)
            if eid in out:
                continue
            out[eid] = {
                "go_master_id": int(r.go_master_id),
                "go_id": r.go_id,
                "go_name": r.go_name,
                "go_namespace": r.go_namespace,
                "go_source_system": r.go_source_system,
                "go_data_source": r.go_data_source,
                "go_etl_package_id": r.go_etl_package_id,
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

    def _fetch_go_relation_summary(
        self,
        go_master_ids: list[int],
        include_go_relation_summary: bool,
        include_go_relation_details: bool,
        max_go_terms_per_side: int,
    ) -> dict[int, dict[str, Any]]:
        if not include_go_relation_summary or not go_master_ids:
            return {}

        rows = (
            self.session.query(
                GORelation.parent_id,
                GORelation.child_id,
                GORelation.relation_type,
            )
            .filter(
                or_(
                    GORelation.parent_id.in_(go_master_ids),
                    GORelation.child_id.in_(go_master_ids),
                )
            )
            .all()
        )

        go_id_rows = (
            self.session.query(GOMaster.id, GOMaster.go_id)
            .filter(GOMaster.id.in_(go_master_ids))
            .all()
        )
        term_id_to_go_id = {int(gid): str(go_id) for gid, go_id in go_id_rows}

        parent_counts: dict[int, int] = defaultdict(int)
        child_counts: dict[int, int] = defaultdict(int)
        parent_types: dict[int, set[str]] = defaultdict(set)
        child_types: dict[int, set[str]] = defaultdict(set)
        parent_ids: dict[int, list[str]] = defaultdict(list)
        child_ids: dict[int, list[str]] = defaultdict(list)

        target_set = set(go_master_ids)
        for r in rows:
            p = int(r.parent_id)
            c = int(r.child_id)
            rel_type = _norm(r.relation_type) or "unknown"

            if c in target_set:
                parent_counts[c] += 1
                parent_types[c].add(rel_type)
                if include_go_relation_details and p in term_id_to_go_id:
                    go_val = term_id_to_go_id[p]
                    if go_val not in parent_ids[c] and len(parent_ids[c]) < max_go_terms_per_side:  # noqa: E501
                        parent_ids[c].append(go_val)

            if p in target_set:
                child_counts[p] += 1
                child_types[p].add(rel_type)
                if include_go_relation_details and c in term_id_to_go_id:
                    go_val = term_id_to_go_id[c]
                    if go_val not in child_ids[p] and len(child_ids[p]) < max_go_terms_per_side:  # noqa: E501
                        child_ids[p].append(go_val)

        out: dict[int, dict[str, Any]] = {}
        for gid in go_master_ids:
            out[gid] = {
                "go_parent_count": int(parent_counts.get(gid, 0)),
                "go_child_count": int(child_counts.get(gid, 0)),
                "go_parent_relation_types": sorted(parent_types.get(gid, set())),
                "go_child_relation_types": sorted(child_types.get(gid, set())),
                "go_parent_ids": parent_ids.get(gid, []) if include_go_relation_details else None,  # noqa: E501
                "go_child_ids": child_ids.get(gid, []) if include_go_relation_details else None,  # noqa: E501
            }
        return out

    def _fetch_entity_relationship_summary(
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

        group_name_map: dict[int, str] = {}
        if group_ids:
            group_rows = (
                self.session.query(EntityGroup.id, EntityGroup.name)
                .filter(EntityGroup.id.in_(sorted(group_ids)))
                .all()
            )
            group_name_map = {int(gid): str(name) for gid, name in group_rows}

        counts: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        target_set = set(entity_ids)
        for r in rows:
            e1 = int(r.entity_1_id)
            e2 = int(r.entity_2_id)
            g1 = int(r.entity_1_group_id) if r.entity_1_group_id is not None else None
            g2 = int(r.entity_2_group_id) if r.entity_2_group_id is not None else None

            if e1 in target_set:
                counts[e1][group_name_map.get(g2, "Unknown")] += 1
            if e2 in target_set and e2 != e1:
                counts[e2][group_name_map.get(g1, "Unknown")] += 1

        for eid in entity_ids:
            pairs = list(counts[eid].items())
            pairs.sort(key=lambda x: (-x[1], x[0]))
            by_group[eid] = pairs
            totals[eid] = int(sum(v for _, v in pairs))

        return by_group, totals

    def run(self):
        input_data_raw = self.param("input_data", required=True)

        emit_not_found_rows = _parse_bool(self.param("emit_not_found_rows", True), True)
        include_aliases = _parse_bool(self.param("include_aliases", True), True)
        include_go_relation_summary = _parse_bool(
            self.param("include_go_relation_summary", True), True
        )
        include_go_relation_details = _parse_bool(
            self.param("include_go_relation_details", False), False
        )
        max_go_terms_per_side = _parse_int(self.param("max_go_terms_per_side", 20), 20)
        include_relationships = _parse_bool(
            self.param("include_relationships", False), False
        )

        all_mode = self._is_all_input(input_data_raw)
        if all_mode:
            input_entries, resolved_by_key, entity_ids = self._resolve_all_go_entities()
            if not entity_ids:
                raise ValueError(
                    "No GO entities found in database for input_data='__ALL__'."
                )
        else:
            input_data = self.resolve_input_list(input_data_raw, param_name="input_data")
            input_entries = []
            for item in input_data:
                raw = _norm(item)
                if raw:
                    input_entries.append({"input_value": raw, "input_key": raw.lower()})

            if not input_entries:
                raise ValueError("input_data must contain at least one non-empty value.")

            resolved_by_key, entity_ids = self._resolve_input_entities(input_entries)

        go_core_by_entity = self._fetch_go_core(entity_ids)
        aliases_by_entity = self._fetch_aliases(entity_ids) if include_aliases else {}

        go_master_ids = sorted(
            {int(v["go_master_id"]) for v in go_core_by_entity.values() if v.get("go_master_id") is not None}  # noqa: E501
        )
        go_rel_summary_by_id = self._fetch_go_relation_summary(
            go_master_ids=go_master_ids,
            include_go_relation_summary=include_go_relation_summary,
            include_go_relation_details=include_go_relation_details,
            max_go_terms_per_side=max_go_terms_per_side,
        )
        rel_by_group, rel_totals = self._fetch_entity_relationship_summary(
            entity_ids=entity_ids,
            include_relationships=include_relationships,
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
                        "go_master_id": None,
                        "go_id": None,
                        "go_name": None,
                        "go_namespace": None,
                        "go_source_system": None,
                        "go_data_source": None,
                        "go_etl_package_id": None,
                        "go_parent_count": None,
                        "go_child_count": None,
                        "go_parent_relation_types": None,
                        "go_child_relation_types": None,
                        "go_parent_ids": None,
                        "go_child_ids": None,
                        "entity_relationships_by_group": None,
                        "total_entity_relationships": None,
                        "other_aliases": [] if include_aliases else None,
                        "status": "not_found",
                        "note": "Input not resolved to a GO entity.",
                    }
                )
                continue

            entity_id = int(resolved["entity_id"])
            core = go_core_by_entity.get(entity_id, {})
            go_master_id = core.get("go_master_id")
            go_rel = (
                go_rel_summary_by_id.get(int(go_master_id), {})
                if go_master_id is not None
                else {}
            )
            aliases = aliases_by_entity.get(entity_id, [])

            status = "ok"
            notes: list[str] = []
            if not core:
                status = "partial"
                notes.append("GO entity resolved but no GOMaster row found.")

            if include_aliases:
                canonical_values = {
                    _norm(core.get("go_id")),
                    _norm(resolved.get("matched_alias")),
                    _norm(resolved.get("primary_name")),
                }
                canonical_values.discard("")
                other_aliases = sorted(
                    {
                        _norm(a.alias_value)
                        for a in aliases
                        if _norm(a.alias_value) and _norm(a.alias_value) not in canonical_values
                    }
                )
            else:
                other_aliases = None

            if include_relationships:
                entity_rel = rel_by_group.get(entity_id, [])
                total_entity_rel = rel_totals.get(entity_id, 0)
            else:
                entity_rel = None
                total_entity_rel = None

            records.append(
                {
                    "input_value": input_value,
                    "input_matched_alias": resolved.get("matched_alias"),
                    "entity_id": entity_id,
                    "go_master_id": core.get("go_master_id"),
                    "go_id": core.get("go_id"),
                    "go_name": core.get("go_name"),
                    "go_namespace": core.get("go_namespace"),
                    "go_source_system": core.get("go_source_system"),
                    "go_data_source": core.get("go_data_source"),
                    "go_etl_package_id": core.get("go_etl_package_id"),
                    "go_parent_count": (
                        go_rel.get("go_parent_count")
                        if include_go_relation_summary
                        else None
                    ),
                    "go_child_count": (
                        go_rel.get("go_child_count")
                        if include_go_relation_summary
                        else None
                    ),
                    "go_parent_relation_types": (
                        go_rel.get("go_parent_relation_types")
                        if include_go_relation_summary
                        else None
                    ),
                    "go_child_relation_types": (
                        go_rel.get("go_child_relation_types")
                        if include_go_relation_summary
                        else None
                    ),
                    "go_parent_ids": (
                        go_rel.get("go_parent_ids")
                        if include_go_relation_summary and include_go_relation_details
                        else None
                    ),
                    "go_child_ids": (
                        go_rel.get("go_child_ids")
                        if include_go_relation_summary and include_go_relation_details
                        else None
                    ),
                    "entity_relationships_by_group": entity_rel,
                    "total_entity_relationships": total_entity_rel,
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
