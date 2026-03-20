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
    ProteinEntity,
    ProteinMaster,
    ProteinPfam,
    ProteinPfamLink,
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
    if t in {"code", "symbol", "preferred"}:
        return 1
    if t in {"name", "synonym"}:
        return 2
    if t == "isoform":
        return 3
    return 4


@dataclass
class AliasRow:
    alias_value: str
    alias_type: Optional[str]
    xref_source: Optional[str]
    is_primary: Optional[bool]


class ProteinMasterAnnotationReport(ReportBase):
    name = "protein_master_annotation"
    description = (
        "Compact protein annotation report with ProteinMaster metadata, canonical/"
        "isoform context, optional Pfam summary, and optional relationship summary."
    )

    columns = [
        "input_value",
        "input_matched_alias",
        "entity_id",
        "canonical_entity_id",
        "protein_master_id",
        "protein_id",
        "input_is_isoform",
        "input_isoform_accession",
        "isoform_count",
        "function",
        "location",
        "tissue_expression",
        "pseudogene_note",
        "protein_source_system",
        "protein_data_source",
        "protein_etl_package_id",
        "pfam_total_count",
        "pfam_count_by_type",
        "pfam_ids_by_type",
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
            "include_pfam_summary": True,
            "include_pfam_details": False,
            "max_pfam_ids_per_type": 20,
            "include_relationships": False,
            "include_aliases": True,
        }

    @staticmethod
    def _cast_nullable_int_columns(df: pd.DataFrame) -> pd.DataFrame:
        int_cols = [
            "entity_id",
            "canonical_entity_id",
            "protein_master_id",
            "protein_etl_package_id",
            "isoform_count",
            "pfam_total_count",
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

    def _resolve_all_protein_entities(
        self,
    ) -> tuple[list[dict[str, str]], dict[str, dict[str, Any]], list[int]]:
        primary_alias = aliased(EntityAlias)

        rows = (
            self.session.query(
                Entity.id.label("entity_id"),
                ProteinMaster.protein_id.label("protein_id"),
                ProteinEntity.isoform_accession.label("isoform_accession"),
                primary_alias.alias_value.label("primary_name"),
            )
            .join(ProteinEntity, ProteinEntity.entity_id == Entity.id)
            .join(ProteinMaster, ProteinMaster.id == ProteinEntity.protein_id, isouter=True)
            .join(EntityGroup, Entity.group_id == EntityGroup.id, isouter=True)
            .join(
                primary_alias,
                and_(
                    primary_alias.entity_id == Entity.id,
                    primary_alias.is_primary.is_(True),
                ),
                isouter=True,
            )
            .filter(func.lower(EntityGroup.name).in_(["protein", "proteins"]))
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
                or _norm(r.isoform_accession)
                or _norm(r.protein_id)
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
            .filter(func.lower(EntityGroup.name).in_(["protein", "proteins"]))
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

    def _fetch_input_protein_entities(
        self,
        entity_ids: list[int],
    ) -> tuple[dict[int, dict[str, Any]], list[int]]:
        if not entity_ids:
            return {}, []

        rows = (
            self.session.query(
                ProteinEntity.entity_id,
                ProteinEntity.protein_id,
                ProteinEntity.is_isoform,
                ProteinEntity.isoform_accession,
            )
            .filter(ProteinEntity.entity_id.in_(entity_ids))
            .all()
        )

        out: dict[int, dict[str, Any]] = {}
        protein_master_ids: set[int] = set()
        for r in rows:
            eid = int(r.entity_id)
            if eid in out:
                continue
            pid = int(r.protein_id)
            out[eid] = {
                "protein_master_id": pid,
                "input_is_isoform": bool(r.is_isoform),
                "input_isoform_accession": r.isoform_accession,
            }
            protein_master_ids.add(pid)

        return out, sorted(protein_master_ids)

    def _fetch_protein_master(
        self,
        protein_master_ids: list[int],
    ) -> dict[int, dict[str, Any]]:
        if not protein_master_ids:
            return {}

        rows = (
            self.session.query(
                ProteinMaster.id.label("protein_master_id"),
                ProteinMaster.protein_id.label("protein_id"),
                ProteinMaster.function.label("function"),
                ProteinMaster.location.label("location"),
                ProteinMaster.tissue_expression.label("tissue_expression"),
                ProteinMaster.pseudogene_note.label("pseudogene_note"),
                ProteinMaster.etl_package_id.label("protein_etl_package_id"),
                ETLDataSource.name.label("protein_data_source"),
                ETLSourceSystem.name.label("protein_source_system"),
            )
            .join(ETLDataSource, ETLDataSource.id == ProteinMaster.data_source_id, isouter=True)  # noqa: E501
            .join(
                ETLSourceSystem,
                ETLSourceSystem.id == ETLDataSource.source_system_id,
                isouter=True,
            )
            .filter(ProteinMaster.id.in_(protein_master_ids))
            .all()
        )

        out: dict[int, dict[str, Any]] = {}
        for r in rows:
            out[int(r.protein_master_id)] = {
                "protein_id": r.protein_id,
                "function": r.function,
                "location": r.location,
                "tissue_expression": r.tissue_expression,
                "pseudogene_note": r.pseudogene_note,
                "protein_etl_package_id": r.protein_etl_package_id,
                "protein_data_source": r.protein_data_source,
                "protein_source_system": r.protein_source_system,
            }
        return out

    def _fetch_canonical_entities(self, protein_master_ids: list[int]) -> dict[int, int]:
        if not protein_master_ids:
            return {}

        rows = (
            self.session.query(
                ProteinEntity.protein_id,
                ProteinEntity.entity_id,
            )
            .filter(ProteinEntity.protein_id.in_(protein_master_ids))
            .filter(ProteinEntity.is_isoform.is_(False))
            .order_by(ProteinEntity.protein_id, ProteinEntity.entity_id)
            .all()
        )

        out: dict[int, int] = {}
        for r in rows:
            pid = int(r.protein_id)
            if pid in out:
                continue
            out[pid] = int(r.entity_id)
        return out

    def _fetch_isoform_counts(self, protein_master_ids: list[int]) -> dict[int, int]:
        if not protein_master_ids:
            return {}

        rows = (
            self.session.query(
                ProteinEntity.protein_id,
                func.count(func.distinct(ProteinEntity.entity_id)).label("n_isoforms"),
            )
            .filter(ProteinEntity.protein_id.in_(protein_master_ids))
            .filter(ProteinEntity.is_isoform.is_(True))
            .group_by(ProteinEntity.protein_id)
            .all()
        )
        return {int(r.protein_id): int(r.n_isoforms or 0) for r in rows}

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

    def _fetch_pfam_summary(
        self,
        protein_master_ids: list[int],
        include_pfam_summary: bool,
        include_pfam_details: bool,
        max_pfam_ids_per_type: int,
    ) -> tuple[dict[int, int], dict[int, list[tuple[str, int]]], dict[int, dict[str, list[str]]]]:  # noqa: E501
        total_counts: dict[int, int] = {}
        counts_by_type: dict[int, list[tuple[str, int]]] = {}
        ids_by_type: dict[int, dict[str, list[str]]] = {}

        if not include_pfam_summary or not protein_master_ids:
            return total_counts, counts_by_type, ids_by_type

        rows = (
            self.session.query(
                ProteinPfamLink.protein_id.label("protein_master_id"),
                ProteinPfam.pfam_acc.label("pfam_acc"),
                ProteinPfam.pfam_id.label("pfam_id"),
                ProteinPfam.type.label("pfam_type"),
            )
            .join(ProteinPfam, ProteinPfam.id == ProteinPfamLink.pfam_pk_id)
            .filter(ProteinPfamLink.protein_id.in_(protein_master_ids))
            .all()
        )

        agg_counts: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        agg_ids: dict[int, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))

        for r in rows:
            pid = int(r.protein_master_id)
            ptype = _norm(r.pfam_type) or "Unknown"
            agg_counts[pid][ptype] += 1

            if include_pfam_details:
                label = _norm(r.pfam_id) or _norm(r.pfam_acc)
                if label and label not in agg_ids[pid][ptype]:
                    if len(agg_ids[pid][ptype]) < max_pfam_ids_per_type:
                        agg_ids[pid][ptype].append(label)

        for pid in protein_master_ids:
            by_type = list(agg_counts[pid].items())
            by_type.sort(key=lambda x: (-x[1], x[0]))
            counts_by_type[pid] = by_type
            total_counts[pid] = int(sum(v for _, v in by_type))
            if include_pfam_details:
                ids_by_type[pid] = dict(agg_ids[pid])

        return total_counts, counts_by_type, ids_by_type

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

        group_name_map: dict[int, str] = {}
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
                counts[e1][group_name_map.get(g2, "Unknown")] += 1
            if e2 in entity_id_set and e2 != e1:
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
        include_pfam_summary = _parse_bool(self.param("include_pfam_summary", True), True)
        include_pfam_details = _parse_bool(
            self.param("include_pfam_details", False), False
        )
        max_pfam_ids_per_type = _parse_int(self.param("max_pfam_ids_per_type", 20), 20)
        include_relationships = _parse_bool(
            self.param("include_relationships", False), False
        )
        include_aliases = _parse_bool(self.param("include_aliases", True), True)

        all_mode = self._is_all_input(input_data_raw)
        if all_mode:
            input_entries, resolved_by_key, input_entity_ids = (
                self._resolve_all_protein_entities()
            )
            if not input_entity_ids:
                raise ValueError(
                    "No protein entities found in database for input_data='__ALL__'."
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

            resolved_by_key, input_entity_ids = self._resolve_input_entities(input_entries)

        protein_link_by_entity, protein_master_ids = self._fetch_input_protein_entities(
            input_entity_ids
        )
        protein_master_by_id = self._fetch_protein_master(protein_master_ids)
        canonical_entity_by_protein = self._fetch_canonical_entities(protein_master_ids)
        isoform_counts_by_protein = self._fetch_isoform_counts(protein_master_ids)

        (
            pfam_total_by_protein,
            pfam_count_by_type_by_protein,
            pfam_ids_by_type_by_protein,
        ) = self._fetch_pfam_summary(
            protein_master_ids=protein_master_ids,
            include_pfam_summary=include_pfam_summary,
            include_pfam_details=include_pfam_details,
            max_pfam_ids_per_type=max_pfam_ids_per_type,
        )

        relationship_entity_ids: set[int] = set()
        alias_entity_ids: set[int] = set()
        for eid in input_entity_ids:
            link = protein_link_by_entity.get(eid)
            if not link:
                alias_entity_ids.add(eid)
                relationship_entity_ids.add(eid)
                continue
            pid = int(link["protein_master_id"])
            canonical_eid = canonical_entity_by_protein.get(pid)
            alias_entity_ids.add(int(canonical_eid or eid))
            relationship_entity_ids.add(int(canonical_eid or eid))

        aliases_by_entity = (
            self._fetch_aliases(sorted(alias_entity_ids)) if include_aliases else {}
        )
        rel_by_group, rel_totals = self._fetch_relationship_summary(
            entity_ids=sorted(relationship_entity_ids),
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
                        "canonical_entity_id": None,
                        "protein_master_id": None,
                        "protein_id": None,
                        "input_is_isoform": None,
                        "input_isoform_accession": None,
                        "isoform_count": None,
                        "function": None,
                        "location": None,
                        "tissue_expression": None,
                        "pseudogene_note": None,
                        "protein_source_system": None,
                        "protein_data_source": None,
                        "protein_etl_package_id": None,
                        "pfam_total_count": None,
                        "pfam_count_by_type": None,
                        "pfam_ids_by_type": None,
                        "entity_relationships_by_group": None,
                        "total_entity_relationships": None,
                        "other_aliases": [] if include_aliases else None,
                        "status": "not_found",
                        "note": "Input not resolved to a Protein entity.",
                    }
                )
                continue

            input_entity_id = int(resolved["entity_id"])
            link = protein_link_by_entity.get(input_entity_id)

            notes: list[str] = []
            status = "ok"
            if not link:
                status = "partial"
                notes.append("Protein entity resolved but no ProteinEntity link found.")

            protein_master_id = int(link["protein_master_id"]) if link else None
            protein_master = (
                protein_master_by_id.get(protein_master_id, {})
                if protein_master_id is not None
                else {}
            )
            if link and not protein_master:
                status = "partial"
                notes.append("ProteinEntity link found but no ProteinMaster row found.")

            canonical_entity_id = (
                canonical_entity_by_protein.get(protein_master_id)
                if protein_master_id is not None
                else None
            )

            if include_pfam_summary and protein_master_id is not None:
                pfam_total_count = pfam_total_by_protein.get(protein_master_id, 0)
                pfam_count_by_type = pfam_count_by_type_by_protein.get(protein_master_id, [])
                pfam_ids_by_type = (
                    pfam_ids_by_type_by_protein.get(protein_master_id, {})
                    if include_pfam_details
                    else None
                )
            else:
                pfam_total_count = None
                pfam_count_by_type = None
                pfam_ids_by_type = None

            rel_entity_id = int(canonical_entity_id or input_entity_id)
            if include_relationships:
                rel_list = rel_by_group.get(rel_entity_id, [])
                rel_total = rel_totals.get(rel_entity_id, 0)
            else:
                rel_list = None
                rel_total = None

            if include_aliases:
                alias_entity_id = int(canonical_entity_id or input_entity_id)
                aliases = aliases_by_entity.get(alias_entity_id, [])
                canonical_values = {
                    _norm(protein_master.get("protein_id")),
                    _norm(resolved.get("matched_alias")),
                    _norm(resolved.get("primary_name")),
                    _norm(link.get("input_isoform_accession") if link else None),
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

            records.append(
                {
                    "input_value": input_value,
                    "input_matched_alias": resolved.get("matched_alias"),
                    "entity_id": input_entity_id,
                    "canonical_entity_id": canonical_entity_id,
                    "protein_master_id": protein_master_id,
                    "protein_id": protein_master.get("protein_id"),
                    "input_is_isoform": link.get("input_is_isoform") if link else None,
                    "input_isoform_accession": (
                        link.get("input_isoform_accession") if link else None
                    ),
                    "isoform_count": (
                        isoform_counts_by_protein.get(protein_master_id, 0)
                        if protein_master_id is not None
                        else None
                    ),
                    "function": protein_master.get("function"),
                    "location": protein_master.get("location"),
                    "tissue_expression": protein_master.get("tissue_expression"),
                    "pseudogene_note": protein_master.get("pseudogene_note"),
                    "protein_source_system": protein_master.get("protein_source_system"),
                    "protein_data_source": protein_master.get("protein_data_source"),
                    "protein_etl_package_id": protein_master.get("protein_etl_package_id"),
                    "pfam_total_count": pfam_total_count,
                    "pfam_count_by_type": pfam_count_by_type,
                    "pfam_ids_by_type": pfam_ids_by_type,
                    "entity_relationships_by_group": rel_list,
                    "total_entity_relationships": rel_total,
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
