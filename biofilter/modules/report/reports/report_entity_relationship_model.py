from __future__ import annotations

from collections import OrderedDict
from typing import Any

import pandas as pd
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import (
    Entity,
    EntityAlias,
    EntityGroup,
    EntityRelationship,
    EntityRelationshipType,
)
from biofilter.modules.report.reports.base_report import ReportBase


class EntityRelationshipModelReport(ReportBase):
    """
    Relationship-focused modeling report.

    Resolves input terms through EntityAlias and returns relationship rows
    from entity_relationships where input entities appear either as entity_1
    or entity_2.
    """

    name = "entity_relationship_model"
    description = (
        "Returns relationship models from EntityRelationship using EntityAlias "
        "resolution for input terms, supporting input/output group filters and "
        "scope controls (between inputs or input-to-any)."
    )

    columns = [
        "input_original",
        "input_normalized",
        "input_matched_alias",
        "input_entity_id",
        "input_primary_name",
        "input_group_id",
        "input_group_name",
        "match_side",
        "relationship_id",
        "relationship_type_id",
        "relationship_type",
        "relationship_description",
        "direction",
        "entity_1_id",
        "entity_1_primary_name",
        "entity_1_group_name",
        "entity_2_id",
        "entity_2_primary_name",
        "entity_2_group_name",
        "related_entity_id",
        "related_primary_name",
        "related_group_name",
        "data_source_id",
        "etl_package_id",
        "observation",
    ]

    @classmethod
    def available_columns(cls) -> list[str]:
        return cls.columns

    @classmethod
    def example_input(cls):
        return {
            "input_data": ["TP53", "BRCA1"],
            "input_entity_groups": ["Gene"],
            "output_entity_groups": ["Pathway", "Protein"],
            "relationship_scope": "input_to_any",
        }

    @classmethod
    def explain(cls) -> str:
        return """\
🔗 Entity Relationship Model
============================

Purpose:
- Resolve input terms through entity aliases and return relationship rows from
  EntityRelationship where input entities participate on either side.

Key parameters:
- input_data (required): list[str] or file path with one alias per line
- input_entity_groups (optional): restrict input resolution to these groups
- output_entity_groups (optional): keep only related entities in these groups
- relationship_types (optional): filter by relationship type code
- relationship_scope:
  - input_to_any (default): input entity related to any entity
  - between_inputs: both sides must be in the input-resolved entity set
- deduplicate_pairs (optional):
  - defaults to True in between_inputs
  - defaults to False in input_to_any

Notes:
- Input matching is case-insensitive using alias_norm when present, else alias_value.
- Input entities are matched against both entity_1 and entity_2.
- Output includes primary aliases for input and related entities.
"""

    @staticmethod
    def _as_ci_set(value: Any) -> set[str]:
        if value is None:
            return set()
        if isinstance(value, (list, tuple, set)):
            seq = value
        else:
            seq = [value]
        out = set()
        for v in seq:
            s = str(v).strip().lower()
            if s:
                out.add(s)
        return out

    @staticmethod
    def _parse_scope(value: Any) -> str:
        scope = str(value or "input_to_any").strip().lower()
        if scope not in {"input_to_any", "between_inputs"}:
            raise ValueError(
                "relationship_scope must be 'input_to_any' or 'between_inputs'."
            )
        return scope

    def _resolve_input_entities(
        self,
        normalized_to_original: OrderedDict[str, str],
        input_group_filter: set[str],
    ) -> tuple[dict[int, dict[str, Any]], set[str]]:
        """
        Resolve input aliases to entity ids.

        Returns:
        - entity_hits_by_id: first hit metadata per entity id
        - found_input_keys: normalized input keys that matched at least one entity
        """
        input_keys = list(normalized_to_original.keys())
        input_key_expr = func.lower(
            func.coalesce(EntityAlias.alias_norm, EntityAlias.alias_value)
        )
        primary_alias = aliased(EntityAlias)

        q = (
            self.session.query(
                input_key_expr.label("input_key"),
                EntityAlias.alias_value.label("matched_alias"),
                Entity.id.label("entity_id"),
                Entity.group_id.label("group_id"),
                EntityGroup.name.label("group_name"),
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
            .filter(input_key_expr.in_(input_keys))
        )

        if input_group_filter:
            q = q.filter(func.lower(EntityGroup.name).in_(list(input_group_filter)))

        rows = q.all()

        entity_hits_by_id: dict[int, dict[str, Any]] = {}
        found_input_keys: set[str] = set()
        for row in rows:
            input_key = row.input_key
            entity_id = int(row.entity_id)
            found_input_keys.add(input_key)
            if entity_id in entity_hits_by_id:
                continue
            entity_hits_by_id[entity_id] = {
                "input_key": input_key,
                "input_original": normalized_to_original.get(input_key, input_key),
                "matched_alias": row.matched_alias,
                "entity_id": entity_id,
                "group_id": row.group_id,
                "group_name": row.group_name,
                "primary_name": row.primary_name,
            }

        return entity_hits_by_id, found_input_keys

    def _query_relationships(
        self,
        input_entity_ids: set[int],
        relationship_type_filter: set[str],
        scope: str,
    ):
        e1_group = aliased(EntityGroup)
        e2_group = aliased(EntityGroup)
        e1_primary = aliased(EntityAlias)
        e2_primary = aliased(EntityAlias)
        e1 = aliased(Entity)
        e2 = aliased(Entity)

        q = (
            self.session.query(
                EntityRelationship.id.label("relationship_id"),
                EntityRelationship.entity_1_id.label("entity_1_id"),
                EntityRelationship.entity_2_id.label("entity_2_id"),
                EntityRelationship.relationship_type_id.label("relationship_type_id"),
                EntityRelationship.data_source_id.label("data_source_id"),
                EntityRelationship.etl_package_id.label("etl_package_id"),
                EntityRelationshipType.code.label("relationship_type"),
                EntityRelationshipType.description.label("relationship_description"),
                e1_group.name.label("entity_1_group_name"),
                e2_group.name.label("entity_2_group_name"),
                e1_primary.alias_value.label("entity_1_primary_name"),
                e2_primary.alias_value.label("entity_2_primary_name"),
            )
            .join(
                EntityRelationshipType,
                EntityRelationshipType.id == EntityRelationship.relationship_type_id,
            )
            .join(e1, e1.id == EntityRelationship.entity_1_id)
            .join(e2, e2.id == EntityRelationship.entity_2_id)
            .join(e1_group, e1.group_id == e1_group.id, isouter=True)
            .join(e2_group, e2.group_id == e2_group.id, isouter=True)
            .join(
                e1_primary,
                and_(
                    e1_primary.entity_id == EntityRelationship.entity_1_id,
                    e1_primary.is_primary.is_(True),
                ),
                isouter=True,
            )
            .join(
                e2_primary,
                and_(
                    e2_primary.entity_id == EntityRelationship.entity_2_id,
                    e2_primary.is_primary.is_(True),
                ),
                isouter=True,
            )
            .filter(
                or_(
                    EntityRelationship.entity_1_id.in_(list(input_entity_ids)),
                    EntityRelationship.entity_2_id.in_(list(input_entity_ids)),
                )
            )
        )

        if scope == "between_inputs":
            q = q.filter(
                and_(
                    EntityRelationship.entity_1_id.in_(list(input_entity_ids)),
                    EntityRelationship.entity_2_id.in_(list(input_entity_ids)),
                )
            )

        if relationship_type_filter:
            q = q.filter(
                func.lower(EntityRelationshipType.code).in_(
                    list(relationship_type_filter)
                )
            )

        return q.all()

    def run(self):
        input_data_raw = self.param("input_data", required=True)
        input_data = self.resolve_input_list(input_data_raw, param_name="input_data")
        scope = self._parse_scope(self.param("relationship_scope", "input_to_any"))
        input_group_filter = self._as_ci_set(self.param("input_entity_groups"))
        output_group_filter = self._as_ci_set(self.param("output_entity_groups"))
        relationship_type_filter = self._as_ci_set(self.param("relationship_types"))

        deduplicate_pairs = self.param("deduplicate_pairs", default=None)
        if deduplicate_pairs is None:
            deduplicate_pairs = scope == "between_inputs"
        deduplicate_pairs = bool(deduplicate_pairs)

        normalized_to_original: OrderedDict[str, str] = OrderedDict()
        for item in input_data:
            value = str(item).strip()
            if not value:
                continue
            key = value.lower()
            if key not in normalized_to_original:
                normalized_to_original[key] = value

        if not normalized_to_original:
            raise ValueError("input_data must contain at least one non-empty value.")

        entity_hits_by_id, found_input_keys = self._resolve_input_entities(
            normalized_to_original=normalized_to_original,
            input_group_filter=input_group_filter,
        )

        if not entity_hits_by_id:
            not_found_rows = []
            for key, original in normalized_to_original.items():
                not_found_rows.append(
                    {
                        "input_original": original,
                        "input_normalized": key,
                        "input_matched_alias": None,
                        "input_entity_id": None,
                        "input_primary_name": None,
                        "input_group_id": None,
                        "input_group_name": None,
                        "match_side": None,
                        "relationship_id": None,
                        "relationship_type_id": None,
                        "relationship_type": None,
                        "relationship_description": None,
                        "direction": None,
                        "entity_1_id": None,
                        "entity_1_primary_name": None,
                        "entity_1_group_name": None,
                        "entity_2_id": None,
                        "entity_2_primary_name": None,
                        "entity_2_group_name": None,
                        "related_entity_id": None,
                        "related_primary_name": None,
                        "related_group_name": None,
                        "data_source_id": None,
                        "etl_package_id": None,
                        "observation": "not found",
                    }
                )
            df = pd.DataFrame(not_found_rows).reindex(columns=self.columns)
            self.results = df
            return df.reset_index(drop=True)

        input_entity_ids = set(entity_hits_by_id.keys())
        relationships = self._query_relationships(
            input_entity_ids=input_entity_ids,
            relationship_type_filter=relationship_type_filter,
            scope=scope,
        )

        rows: list[dict[str, Any]] = []
        seen_between_inputs: set[tuple[int, int, int, int]] = set()

        for rel in relationships:
            entity_1_id = int(rel.entity_1_id)
            entity_2_id = int(rel.entity_2_id)
            relationship_id = int(rel.relationship_id)

            # Expand both sides when input appears in either side.
            anchors: list[tuple[str, int, int]] = []
            if entity_1_id in input_entity_ids:
                anchors.append(("entity_1", entity_1_id, entity_2_id))
            if entity_2_id in input_entity_ids:
                anchors.append(("entity_2", entity_2_id, entity_1_id))

            for match_side, input_entity_id, related_entity_id in anchors:
                hit = entity_hits_by_id[input_entity_id]
                related_group_name = (
                    rel.entity_2_group_name if match_side == "entity_1" else rel.entity_1_group_name
                )
                related_primary_name = (
                    rel.entity_2_primary_name
                    if match_side == "entity_1"
                    else rel.entity_1_primary_name
                )
                direction = (
                    "input->related" if match_side == "entity_1" else "related->input"
                )

                if output_group_filter:
                    if not related_group_name or (
                        str(related_group_name).strip().lower() not in output_group_filter
                    ):
                        continue

                if scope == "between_inputs" and deduplicate_pairs:
                    pair_key = (
                        relationship_id,
                        min(input_entity_id, related_entity_id),
                        max(input_entity_id, related_entity_id),
                        int(rel.relationship_type_id),
                    )
                    if pair_key in seen_between_inputs:
                        continue
                    seen_between_inputs.add(pair_key)

                rows.append(
                    {
                        "input_original": hit["input_original"],
                        "input_normalized": hit["input_key"],
                        "input_matched_alias": hit["matched_alias"],
                        "input_entity_id": input_entity_id,
                        "input_primary_name": hit["primary_name"],
                        "input_group_id": hit["group_id"],
                        "input_group_name": hit["group_name"],
                        "match_side": match_side,
                        "relationship_id": relationship_id,
                        "relationship_type_id": int(rel.relationship_type_id),
                        "relationship_type": rel.relationship_type,
                        "relationship_description": rel.relationship_description,
                        "direction": direction,
                        "entity_1_id": entity_1_id,
                        "entity_1_primary_name": rel.entity_1_primary_name,
                        "entity_1_group_name": rel.entity_1_group_name,
                        "entity_2_id": entity_2_id,
                        "entity_2_primary_name": rel.entity_2_primary_name,
                        "entity_2_group_name": rel.entity_2_group_name,
                        "related_entity_id": related_entity_id,
                        "related_primary_name": related_primary_name,
                        "related_group_name": related_group_name,
                        "data_source_id": rel.data_source_id,
                        "etl_package_id": rel.etl_package_id,
                        "observation": "",
                    }
                )

        # Add not found terms after applying input group filter.
        not_found_rows = []
        for key, original in normalized_to_original.items():
            if key in found_input_keys:
                continue
            not_found_rows.append(
                {
                    "input_original": original,
                    "input_normalized": key,
                    "input_matched_alias": None,
                    "input_entity_id": None,
                    "input_primary_name": None,
                    "input_group_id": None,
                    "input_group_name": None,
                    "match_side": None,
                    "relationship_id": None,
                    "relationship_type_id": None,
                    "relationship_type": None,
                    "relationship_description": None,
                    "direction": None,
                    "entity_1_id": None,
                    "entity_1_primary_name": None,
                    "entity_1_group_name": None,
                    "entity_2_id": None,
                    "entity_2_primary_name": None,
                    "entity_2_group_name": None,
                    "related_entity_id": None,
                    "related_primary_name": None,
                    "related_group_name": None,
                    "data_source_id": None,
                    "etl_package_id": None,
                    "observation": "not found",
                }
            )

        if not rows and not_found_rows:
            df = pd.DataFrame(not_found_rows)
        else:
            df = pd.DataFrame(rows + not_found_rows)

        if not df.empty:
            df = df.sort_values(
                by=[
                    "input_original",
                    "relationship_type",
                    "related_group_name",
                    "related_primary_name",
                ],
                na_position="last",
            )

        df = df.reindex(columns=self.columns)
        self.results = df
        return df.reset_index(drop=True)
