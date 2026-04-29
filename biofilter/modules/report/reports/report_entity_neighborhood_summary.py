"""
Entity Neighborhood Summary Report.

Resolves a heterogeneous list of inputs (genes, diseases, proteins, etc.)
to entity ids and returns a 1-hop neighborhood summary, with neighbor
counts and primary names grouped by entity type.

Engine-agnostic: works on PostgreSQL and SQLite. Fuzzy matching uses
rapidfuzz client-side, no DB extension required.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pandas as pd
from sqlalchemy import and_, or_
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import (
    Entity,
    EntityAlias,
    EntityGroup,
    EntityRelationship,
)
from biofilter.modules.report.reports.base_report import ReportBase


VALID_MATCH_MODES = ("exact", "like", "fuzzy")


# Maps user-facing type hint -> internal EntityGroup.name
TYPE_HINT_TO_GROUP = {
    "gene": "Genes",
    "genes": "Genes",
    "disease": "Diseases",
    "diseases": "Diseases",
    "chemical": "Chemicals",
    "chemicals": "Chemicals",
    "pathway": "Pathways",
    "pathways": "Pathways",
    "protein": "Proteins",
    "proteins": "Proteins",
    "go": "GO Terms",
    "go_terms": "GO Terms",
    "goterms": "GO Terms",
}

# Inverse mapping for the "Entity Type" output column
GROUP_TO_ENTITY_TYPE = {
    "Genes": "gene",
    "Diseases": "disease",
    "Chemicals": "chemical",
    "Pathways": "pathway",
    "Proteins": "protein",
    "GO Terms": "go",
}


@dataclass(frozen=True)
class InputItem:
    type_hint: str | None
    word: str


class EntityNeighborhoodSummaryReport(ReportBase):
    name = "entity_neighborhood_summary"
    description = (
        "Resolve heterogeneous inputs (gene:, disease:, protein:, etc.) "
        "to entities and return a 1-hop neighborhood summary grouped by "
        "neighbor entity type. Engine-agnostic; fuzzy matching uses "
        "rapidfuzz client-side."
    )

    BASE_COLUMNS = [
        "Input Word",
        "Input Type Hint",
        "Resolver Mode",
        "Entity ID",
        "Entity Type",
        "Exact Match",
        "Matched Name",
        "Primary Alias",
        "Aliases Top",
        "Alias Count",
        "Degree Total (1-hop)",
        "Degree By Type (1-hop)",
        "Resolve Status",
        "Resolve Method",
        "Resolve Score",
    ]

    @classmethod
    def available_columns(cls) -> list[str]:
        # Per-type neighbor columns are appended dynamically at runtime.
        return cls.BASE_COLUMNS

    @classmethod
    def example_input(cls) -> dict[str, Any]:
        return {
            "items": [
                "gene:BRCA1",
                "disease:Alzheimer disease",
                "protein:P38398",
                "APOE",
            ],
            "match_mode": "exact",
            "aliases_top_n": 20,
            "neighbors_top_n_per_type": 50,
            "emit_not_found_rows": True,
        }

    # =========================================================================
    # Entry point
    # =========================================================================
    def run(self):
        # ---- Parse params ----
        raw_items = self.param("items") or self.param("input_data")
        if raw_items is None:
            raise ValueError(
                "Provide 'items' (or 'input_data') as a list of strings."
            )
        items = self._parse_items(raw_items)
        if not items:
            raise ValueError("No valid input items after parsing.")

        match_mode = self.param("match_mode", default="exact")
        if match_mode not in VALID_MATCH_MODES:
            raise ValueError(
                f"match_mode must be one of {VALID_MATCH_MODES}. "
                f"Got: {match_mode!r}"
            )
        similarity_threshold = float(
            self.param("similarity_threshold", default=80)
        )
        aliases_top_n = int(self.param("aliases_top_n", default=20))
        include_all_aliases = bool(
            self.param("include_all_aliases", default=False)
        )
        neighbors_top_n_per_type = int(
            self.param("neighbors_top_n_per_type", default=50)
        )
        emit_not_found_rows = bool(
            self.param("emit_not_found_rows", default=False)
        )

        # ---- Group map (used everywhere) ----
        group_name_to_id = dict(
            self.session.query(EntityGroup.name, EntityGroup.id).all()
        )
        all_group_names = sorted(group_name_to_id.keys())

        # ---- 1) Resolve inputs to entities ----
        resolution = self._resolve_inputs(
            items=items,
            match_mode=match_mode,
            similarity_threshold=similarity_threshold,
            group_name_to_id=group_name_to_id,
        )

        if not resolution and not emit_not_found_rows:
            self.logger.log("No entities resolved from inputs.", "WARNING")
            return self._empty_output(all_group_names)

        # ---- 2) Build initial rows (resolved + optionally not-found) ----
        rows: list[dict[str, Any]] = []
        for item in items:
            matches = resolution.get(self._key_of(item), [])
            if matches:
                for m in matches:
                    rows.append(self._row_from_match(item, m, match_mode))
            elif emit_not_found_rows:
                rows.append(self._row_not_found(item, match_mode))

        if not rows:
            return self._empty_output(all_group_names)

        # ---- 3) Enrich with primary alias, group name, alias list ----
        entity_ids = sorted({
            r["Entity ID"] for r in rows if r["Entity ID"] is not None
        })

        if entity_ids:
            meta = self._fetch_metadata(entity_ids)
            self._fill_metadata(
                rows, meta, aliases_top_n, include_all_aliases
            )

            # ---- 4) 1-hop neighborhood summary ----
            neighborhood = self._fetch_neighborhood(
                entity_ids,
                group_name_to_id=group_name_to_id,
                neighbors_top_n_per_type=neighbors_top_n_per_type,
            )
        else:
            neighborhood = {}

        # ---- 5) Build final dataframe with dynamic per-type columns ----
        return self._build_output(
            rows=rows,
            neighborhood=neighborhood,
            all_group_names=all_group_names,
        )

    # =========================================================================
    # Input parsing
    # =========================================================================
    def _parse_items(self, raw_items) -> list[InputItem]:
        if isinstance(raw_items, str):
            raw_items = [raw_items]
        if not isinstance(raw_items, (list, tuple)):
            raise ValueError("items must be a list of strings.")

        out: list[InputItem] = []
        for raw in raw_items:
            if isinstance(raw, dict):
                th = (raw.get("type_hint") or raw.get("type") or None)
                wd = str(raw.get("word") or raw.get("value") or "").strip()
                if wd:
                    out.append(InputItem(th, wd))
                continue

            if isinstance(raw, (list, tuple)) and len(raw) == 2:
                th, wd = raw
                wd = str(wd).strip()
                if wd:
                    out.append(InputItem(str(th).strip() or None, wd))
                continue

            s = str(raw).strip()
            if not s:
                continue
            if ":" in s:
                left, right = s.split(":", 1)
                th = left.strip().lower() or None
                wd = right.strip()
                if wd:
                    out.append(InputItem(th, wd))
            else:
                out.append(InputItem(None, s))
        return out

    @staticmethod
    def _key_of(item: InputItem) -> tuple:
        return (item.type_hint or "", item.word.lower())

    # =========================================================================
    # Resolution
    # =========================================================================
    def _resolve_inputs(
        self,
        *,
        items: list[InputItem],
        match_mode: str,
        similarity_threshold: float,
        group_name_to_id: dict,
    ) -> dict[tuple, list[dict]]:
        """
        Returns a dict keyed by (type_hint, lowercased word), value is a list
        of match dicts with keys: entity_id, primary_alias, group_name,
        method, score.
        """
        if not items:
            return {}

        # Bucket inputs by type_hint to scope queries by group when possible.
        buckets: dict[str | None, list[InputItem]] = {}
        for it in items:
            key = it.type_hint.lower() if it.type_hint else None
            buckets.setdefault(key, []).append(it)

        resolution: dict[tuple, list[dict]] = {}
        primary_alias = aliased(EntityAlias)

        for th, bucket in buckets.items():
            words = list({b.word for b in bucket})
            if not words:
                continue

            group_id = None
            if th and th in TYPE_HINT_TO_GROUP:
                group_name = TYPE_HINT_TO_GROUP[th]
                group_id = group_name_to_id.get(group_name)
                if group_id is None:
                    self.logger.log(
                        f"⚠️  Unknown group '{group_name}' for type "
                        f"hint '{th}'; resolving without group filter.",
                        "WARNING",
                    )

            if match_mode == "fuzzy":
                # Vectorized client-side fuzzy via rapidfuzz.
                matches = self._resolve_fuzzy(
                    words=words,
                    group_id=group_id,
                    threshold=similarity_threshold,
                    primary_alias=primary_alias,
                )
            else:
                matches = self._resolve_exact_or_like(
                    words=words,
                    group_id=group_id,
                    match_mode=match_mode,
                    primary_alias=primary_alias,
                )

            # Group matches back by input key
            for it in bucket:
                key = self._key_of(it)
                hits = matches.get(it.word.lower(), [])
                if hits:
                    resolution.setdefault(key, []).extend(hits)

        return resolution

    def _resolve_exact_or_like(
        self,
        *,
        words: list[str],
        group_id: int | None,
        match_mode: str,
        primary_alias,
    ) -> dict[str, list[dict]]:
        from sqlalchemy import func

        norm_words = [w.lower() for w in words]
        word_expr = func.lower(
            func.coalesce(EntityAlias.alias_norm, EntityAlias.alias_value)
        )

        q = (
            self.session.query(
                word_expr.label("matched_norm"),
                EntityAlias.alias_value.label("matched_alias"),
                Entity.id.label("entity_id"),
                primary_alias.alias_value.label("primary_alias"),
                EntityGroup.name.label("group_name"),
            )
            .join(Entity, Entity.id == EntityAlias.entity_id)
            .join(
                primary_alias,
                and_(
                    primary_alias.entity_id == Entity.id,
                    primary_alias.is_primary.is_(True),
                ),
            )
            .join(EntityGroup, EntityGroup.id == Entity.group_id)
        )
        if group_id is not None:
            q = q.filter(Entity.group_id == group_id)

        if match_mode == "exact":
            q = q.filter(word_expr.in_(norm_words))
        else:  # like
            like_filters = [word_expr.like(f"%{w}%") for w in norm_words]
            q = q.filter(or_(*like_filters))

        out: dict[str, list[dict]] = {}
        # Track entities already added per input word so multiple aliases
        # of the same entity collapse into a single output row.
        seen_per_word: dict[str, set[int]] = {}
        method = match_mode
        score = 1.0 if match_mode == "exact" else None

        for row in q.all():
            matched = (row.matched_norm or "").lower()
            # In "like" mode, find which input(s) contain or are contained
            input_words = (
                [matched]
                if match_mode == "exact"
                else [w for w in norm_words if w in matched or matched in w]
            )
            entity_id = int(row.entity_id)
            for w in input_words:
                seen = seen_per_word.setdefault(w, set())
                if entity_id in seen:
                    continue
                seen.add(entity_id)
                out.setdefault(w, []).append({
                    "entity_id": entity_id,
                    "matched_alias": row.matched_alias,
                    "primary_alias": row.primary_alias,
                    "group_name": row.group_name,
                    "method": method,
                    "score": score,
                })
        return out

    def _resolve_fuzzy(
        self,
        *,
        words: list[str],
        group_id: int | None,
        threshold: float,
        primary_alias,
    ) -> dict[str, list[dict]]:
        try:
            from rapidfuzz import fuzz, process
        except ImportError:
            raise ImportError(
                "rapidfuzz is required for fuzzy matching. "
                "Install it with: pip install rapidfuzz"
            )

        from sqlalchemy import func

        word_expr = func.lower(
            func.coalesce(EntityAlias.alias_norm, EntityAlias.alias_value)
        )

        # Pull all alias candidates for the (optional) group, with primary
        # alias and group name.
        q = (
            self.session.query(
                word_expr.label("matched_norm"),
                EntityAlias.alias_value.label("matched_alias"),
                Entity.id.label("entity_id"),
                primary_alias.alias_value.label("primary_alias"),
                EntityGroup.name.label("group_name"),
            )
            .join(Entity, Entity.id == EntityAlias.entity_id)
            .join(
                primary_alias,
                and_(
                    primary_alias.entity_id == Entity.id,
                    primary_alias.is_primary.is_(True),
                ),
            )
            .join(EntityGroup, EntityGroup.id == Entity.group_id)
            .distinct()
        )
        if group_id is not None:
            q = q.filter(Entity.group_id == group_id)

        all_rows = q.all()
        all_keys = [r.matched_norm for r in all_rows if r.matched_norm]
        # Many-to-one: same matched_norm can map to multiple entities.
        key_to_rows: dict[str, list] = {}
        for r in all_rows:
            if r.matched_norm:
                key_to_rows.setdefault(r.matched_norm, []).append(r)

        out: dict[str, list[dict]] = {}
        for word in words:
            results = process.extract(
                word.lower(),
                all_keys,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=threshold,
                limit=None,
            )
            best_per_entity: dict[int, dict] = {}
            for matched_key, score, _ in results:
                for r in key_to_rows.get(matched_key, []):
                    eid = int(r.entity_id)
                    prev = best_per_entity.get(eid)
                    if prev is None or score > prev["score"]:
                        best_per_entity[eid] = {
                            "entity_id": eid,
                            "matched_alias": r.matched_alias,
                            "primary_alias": r.primary_alias,
                            "group_name": r.group_name,
                            "method": "fuzzy",
                            "score": float(score),
                        }
            if best_per_entity:
                out[word.lower()] = list(best_per_entity.values())
        return out

    # =========================================================================
    # Row builders
    # =========================================================================
    @staticmethod
    def _is_exact_match(input_word: str, matched_alias: str | None) -> bool:
        if not matched_alias:
            return False
        return input_word.strip().lower() == matched_alias.strip().lower()

    def _row_from_match(
        self, item: InputItem, m: dict, match_mode: str
    ) -> dict:
        matched_name = m.get("matched_alias")
        return {
            "Input Word": item.word,
            "Input Type Hint": item.type_hint,
            "Resolver Mode": match_mode,
            "Entity ID": m["entity_id"],
            "Entity Type": GROUP_TO_ENTITY_TYPE.get(m["group_name"]),
            "Exact Match": self._is_exact_match(item.word, matched_name),
            "Matched Name": matched_name,
            "Primary Alias": m["primary_alias"],
            "Aliases Top": json.dumps([]),
            "Alias Count": 0,
            "Degree Total (1-hop)": 0,
            "Degree By Type (1-hop)": json.dumps({}),
            "Resolve Status": "resolved",
            "Resolve Method": m["method"],
            "Resolve Score": m["score"],
            "_group_name": m["group_name"],
        }

    def _row_not_found(self, item: InputItem, match_mode: str) -> dict:
        return {
            "Input Word": item.word,
            "Input Type Hint": item.type_hint,
            "Resolver Mode": match_mode,
            "Entity ID": None,
            "Entity Type": None,
            "Exact Match": False,
            "Matched Name": None,
            "Primary Alias": None,
            "Aliases Top": json.dumps([]),
            "Alias Count": 0,
            "Degree Total (1-hop)": 0,
            "Degree By Type (1-hop)": json.dumps({}),
            "Resolve Status": "not_found",
            "Resolve Method": match_mode,
            "Resolve Score": None,
            "_group_name": None,
        }

    # =========================================================================
    # Metadata enrichment
    # =========================================================================
    def _fetch_metadata(self, entity_ids: list[int]) -> dict:
        rows = (
            self.session.query(
                EntityAlias.entity_id,
                EntityAlias.alias_value,
                EntityAlias.is_primary,
                EntityAlias.alias_type,
            )
            .filter(EntityAlias.entity_id.in_(entity_ids))
            .all()
        )
        if not rows:
            return {"alias_count": {}, "aliases": {}}

        df = pd.DataFrame(
            rows,
            columns=["entity_id", "alias_value", "is_primary", "alias_type"],
        )
        alias_count = df.groupby("entity_id").size().to_dict()
        df["primary_rank"] = (~df["is_primary"].fillna(False).astype(bool)).astype(int)
        df["alias_type_str"] = df["alias_type"].fillna("").astype(str)
        df = df.sort_values(
            ["entity_id", "primary_rank", "alias_type_str", "alias_value"]
        )
        aliases = (
            df.groupby("entity_id")["alias_value"]
            .apply(lambda s: s.dropna().astype(str).tolist())
            .to_dict()
        )
        return {"alias_count": alias_count, "aliases": aliases}

    def _fill_metadata(
        self,
        rows: list[dict],
        meta: dict,
        aliases_top_n: int,
        include_all_aliases: bool,
    ) -> None:
        alias_count = meta.get("alias_count", {})
        aliases = meta.get("aliases", {})
        for r in rows:
            eid = r["Entity ID"]
            if eid is None:
                continue
            r["Alias Count"] = int(alias_count.get(eid, 0))
            full_list = aliases.get(eid, [])
            if not include_all_aliases:
                full_list = full_list[:aliases_top_n]
            r["Aliases Top"] = json.dumps(full_list)

    # =========================================================================
    # 1-hop neighborhood
    # =========================================================================
    def _fetch_neighborhood(
        self,
        entity_ids: list[int],
        *,
        group_name_to_id: dict,
        neighbors_top_n_per_type: int,
    ) -> dict[int, dict]:
        """
        Returns a dict keyed by entity_id with:
          - degree_total
          - degree_by_type: dict {group_name: count}
          - neighbors_by_group: dict {group_name: [primary_alias, ...]}
        """
        ER = EntityRelationship
        rows = (
            self.session.query(
                ER.entity_1_id,
                ER.entity_2_id,
            )
            .filter(
                or_(
                    ER.entity_1_id.in_(entity_ids),
                    ER.entity_2_id.in_(entity_ids),
                )
            )
            .all()
        )
        if not rows:
            return {eid: self._empty_neighborhood() for eid in entity_ids}

        edges = pd.DataFrame(rows, columns=["e1", "e2"])
        # Build undirected (entity, neighbor) pairs scoped to our inputs
        targets = set(entity_ids)
        left = edges[edges["e1"].isin(targets)].rename(
            columns={"e1": "entity_id", "e2": "neighbor_id"}
        )
        right = edges[edges["e2"].isin(targets)].rename(
            columns={"e2": "entity_id", "e1": "neighbor_id"}
        )
        nbr = pd.concat([left, right], ignore_index=True)
        nbr = nbr[nbr["entity_id"] != nbr["neighbor_id"]]
        nbr = nbr.drop_duplicates(subset=["entity_id", "neighbor_id"])

        if nbr.empty:
            return {eid: self._empty_neighborhood() for eid in entity_ids}

        # Resolve neighbor metadata (group + primary alias) in batch
        neighbor_ids = sorted(nbr["neighbor_id"].unique().tolist())
        primary_alias = aliased(EntityAlias)
        meta_rows = (
            self.session.query(
                Entity.id,
                EntityGroup.name,
                primary_alias.alias_value,
            )
            .join(EntityGroup, EntityGroup.id == Entity.group_id)
            .join(
                primary_alias,
                and_(
                    primary_alias.entity_id == Entity.id,
                    primary_alias.is_primary.is_(True),
                ),
            )
            .filter(Entity.id.in_(neighbor_ids))
            .all()
        )
        meta_df = pd.DataFrame(
            meta_rows,
            columns=["neighbor_id", "neighbor_group", "neighbor_name"],
        )
        nbr = nbr.merge(meta_df, on="neighbor_id", how="left")
        nbr = nbr.dropna(subset=["neighbor_group"])

        # Aggregate per entity_id and per group
        result: dict[int, dict] = {}
        grouped = nbr.groupby(["entity_id", "neighbor_group"])
        per_pair_lists = (
            grouped["neighbor_name"]
            .apply(lambda s: sorted(s.dropna().astype(str).unique().tolist()))
        )

        for (eid, group_name), names in per_pair_lists.items():
            entry = result.setdefault(int(eid), self._empty_neighborhood())
            count = len(names)
            entry["degree_total"] += count
            entry["degree_by_type"][group_name] = count
            entry["neighbors_by_group"][group_name] = names[
                :neighbors_top_n_per_type
            ]

        # Ensure every requested entity has an entry (even if no neighbors)
        for eid in entity_ids:
            result.setdefault(eid, self._empty_neighborhood())

        return result

    @staticmethod
    def _empty_neighborhood() -> dict:
        return {
            "degree_total": 0,
            "degree_by_type": {},
            "neighbors_by_group": {},
        }

    # =========================================================================
    # Output assembly
    # =========================================================================
    def _build_output(
        self,
        *,
        rows: list[dict],
        neighborhood: dict[int, dict],
        all_group_names: list[str],
    ) -> pd.DataFrame:
        # Apply neighborhood summary into each row + dynamic per-type cols
        for r in rows:
            eid = r["Entity ID"]
            entry = (
                neighborhood.get(eid, self._empty_neighborhood())
                if eid is not None
                else self._empty_neighborhood()
            )
            r["Degree Total (1-hop)"] = int(entry["degree_total"])
            r["Degree By Type (1-hop)"] = json.dumps(entry["degree_by_type"])

            for gname in all_group_names:
                r[gname] = json.dumps(
                    entry["neighbors_by_group"].get(gname, [])
                )

        df = pd.DataFrame(rows)
        # Drop helper column if present
        if "_group_name" in df.columns:
            df = df.drop(columns=["_group_name"])

        # Reorder: BASE_COLUMNS first, then per-type neighbor columns
        cols = list(self.BASE_COLUMNS) + list(all_group_names)
        df = df.reindex(columns=cols)
        self.results = df
        return df.reset_index(drop=True)

    def _empty_output(self, all_group_names: list[str]) -> pd.DataFrame:
        cols = list(self.BASE_COLUMNS) + list(all_group_names)
        df = pd.DataFrame(columns=cols)
        self.results = df
        return df

    def to_dataframe(self, data=None):
        return data if isinstance(data, pd.DataFrame) else pd.DataFrame(data or [])
