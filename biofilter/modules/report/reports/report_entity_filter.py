import pandas as pd
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import Entity, EntityAlias, EntityGroup
from biofilter.modules.report.reports.base_report import ReportBase

VALID_MATCH_MODES = ("exact", "like", "fuzzy")


class EntityFilterReport(ReportBase):
    name = "entity_filter"
    description = (
        "Validates input list of entity names and returns all matching entities, "
        "including conflict and status flags. "
        "Supports match_mode: 'exact' (default), 'like' (substring), 'fuzzy' (similarity)."
    )

    def run(self):
        input_data_raw = self.param("input_data", required=True)
        input_data = self.resolve_input_list(input_data_raw, param_name="input_data")
        match_mode = self.param("match_mode", default="exact")
        similarity_threshold = float(self.param("similarity_threshold", default=80))
        group_filter = self.param("group_filter", default=None)

        if match_mode not in VALID_MATCH_MODES:
            raise ValueError(
                f"match_mode must be one of {VALID_MATCH_MODES}. Got: {match_mode!r}"
            )

        # Normalize + preserve first original form
        normalized_to_original: dict[str, str] = {}
        for item in input_data:
            value = str(item).strip()
            if not value:
                continue
            key = value.lower()
            normalized_to_original.setdefault(key, value)

        if not normalized_to_original:
            raise ValueError("input_data must contain at least one non-empty value.")

        input_keys = list(normalized_to_original.keys())
        primary_alias = aliased(EntityAlias)

        input_key_expr = func.lower(
            func.coalesce(EntityAlias.alias_norm, EntityAlias.alias_value)
        )

        base_query = (
            self.session.query(
                input_key_expr.label("input_key"),
                EntityAlias.alias_value.label("input"),
                EntityAlias.is_primary.label("is_primary"),
                Entity.id.label("entity_id"),
                primary_alias.alias_value.label("primary_name"),
                Entity.group_id.label("group_id"),
                EntityGroup.name.label("group_name"),
                Entity.has_conflict.label("has_conflict"),
                Entity.is_active.label("is_active"),
                EntityAlias.data_source_id.label("data_source_id"),
            )
            .join(Entity, Entity.id == EntityAlias.entity_id)
            .join(
                primary_alias,
                and_(
                    primary_alias.entity_id == Entity.id,
                    primary_alias.is_primary.is_(True),
                ),
            )
            .join(EntityGroup, Entity.group_id == EntityGroup.id, isouter=True)
        )

        if group_filter:
            base_query = base_query.filter(
                func.lower(EntityGroup.name) == group_filter.lower()
            )

        # --- Match logic ---
        key_to_input_original: dict[str, str] = {}
        match_scores: dict[str, float] = {}

        if match_mode == "exact":
            matches = base_query.filter(input_key_expr.in_(input_keys)).all()
            key_to_input_original = normalized_to_original

        elif match_mode == "like":
            like_filters = [input_key_expr.like(f"%{k}%") for k in input_keys]
            matches = base_query.filter(or_(*like_filters)).all()
            for row in matches:
                alias_key = row.input_key or ""
                matched_input = self._best_like_match(alias_key, input_keys)
                if matched_input is not None:
                    key_to_input_original.setdefault(
                        alias_key, normalized_to_original[matched_input]
                    )

        elif match_mode == "fuzzy":
            matched_keys, key_to_input_original, match_scores = self._fuzzy_match_keys(
                input_key_expr=input_key_expr,
                input_keys=input_keys,
                normalized_to_original=normalized_to_original,
                threshold=similarity_threshold,
                group_filter=group_filter,
            )
            matches = (
                base_query.filter(input_key_expr.in_(matched_keys)).all()
                if matched_keys
                else []
            )

        # --- Build DataFrame ---
        columns = [
            "input_original",
            "input",
            "is_primary",
            "entity_id",
            "primary_name",
            "group_id",
            "group_name",
            "has_conflict",
            "is_active",
            "is_deactive",
            "data_source_id",
            "observation",
        ]
        if match_mode == "fuzzy":
            columns.append("similarity_score")

        if matches:
            df = pd.DataFrame(matches)
            df["input_original"] = df["input_key"].map(key_to_input_original)
            df["observation"] = ""
            dupes = df.duplicated(subset=["input_key"], keep=False)
            df.loc[dupes, "observation"] = "multiple matches"
            df["is_deactive"] = df["is_active"].apply(
                lambda x: None if pd.isna(x) else (not bool(x))
            )
            if match_mode == "fuzzy":
                df["similarity_score"] = df["input_key"].map(match_scores)
            found_input_originals = set(df["input_original"].dropna())
            df = df.drop(columns=["input_key"]).sort_values(
                by=["primary_name", "input"]
            )
        else:
            df = pd.DataFrame(columns=columns)
            found_input_originals = set()

        not_found_originals = set(normalized_to_original.values()) - found_input_originals

        if not_found_originals:
            missing_data: dict = {
                "input_original": list(not_found_originals),
                "input": list(not_found_originals),
                "is_primary": None,
                "entity_id": None,
                "primary_name": None,
                "group_id": None,
                "group_name": None,
                "has_conflict": None,
                "is_active": None,
                "is_deactive": None,
                "data_source_id": None,
                "observation": "not found",
            }
            if match_mode == "fuzzy":
                missing_data["similarity_score"] = None
            df = pd.concat([df, pd.DataFrame(missing_data)], ignore_index=True)

        df = df.reindex(columns=columns)
        self.results = df
        return df.reset_index(drop=True)

    @staticmethod
    def _best_like_match(alias_key: str, input_keys: list) -> str | None:
        """Return the longest input_key that is a substring of alias_key or vice-versa."""
        candidates = [k for k in input_keys if k in alias_key or alias_key in k]
        if not candidates:
            return None
        return max(candidates, key=len)

    def _fuzzy_match_keys(
        self,
        input_key_expr,
        input_keys: list,
        normalized_to_original: dict,
        threshold: float,
        group_filter: str | None,
    ) -> tuple[list, dict, dict]:
        """
        Fetch all alias keys from DB and use rapidfuzz to find matches above threshold.

        Returns:
            matched_keys: list of alias keys that matched
            key_to_input_original: alias_key -> original input string
            match_scores: alias_key -> best similarity score
        """
        try:
            from rapidfuzz import fuzz, process
        except ImportError:
            raise ImportError(
                "rapidfuzz is required for fuzzy matching. "
                "Install it with: pip install rapidfuzz"
            )

        # Lightweight query: only alias keys (with optional group filter)
        light_q = (
            self.session.query(input_key_expr.label("input_key"))
            .join(Entity, Entity.id == EntityAlias.entity_id)
            .join(EntityGroup, Entity.group_id == EntityGroup.id, isouter=True)
            .distinct()
        )
        if group_filter:
            light_q = light_q.filter(
                func.lower(EntityGroup.name) == group_filter.lower()
            )

        all_alias_keys = [row.input_key for row in light_q.all() if row.input_key]

        key_to_input_original: dict[str, str] = {}
        match_scores: dict[str, float] = {}

        for input_key in input_keys:
            results = process.extract(
                input_key,
                all_alias_keys,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=threshold,
                limit=None,
            )
            for alias_key, score, _ in results:
                if score > match_scores.get(alias_key, 0):
                    key_to_input_original[alias_key] = normalized_to_original[input_key]
                    match_scores[alias_key] = score

        return list(key_to_input_original.keys()), key_to_input_original, match_scores

    def to_dataframe(self, data=None):
        return (
            data if isinstance(data, pd.DataFrame) else pd.DataFrame(data or [])
        )
