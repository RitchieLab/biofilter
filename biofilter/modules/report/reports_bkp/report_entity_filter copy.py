import pandas as pd
from sqlalchemy import and_, func
from sqlalchemy.orm import aliased

from biofilter.modules.db.models import Entity, EntityAlias, EntityGroup
from biofilter.modules.report.reports.base_report import ReportBase


class EntityFilterReport(ReportBase):
    name = "entity_filter"
    description = "Validates input list of entity names and returns all matching entities, including conflict and status flags."  # noqa E501

    def run(self):
        input_data_raw = self.param("input_data", required=True)
        input_data = self.resolve_input_list(input_data_raw, param_name="input_data")

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

        matches = (
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
            .filter(input_key_expr.in_(input_keys))
            .all()
        )

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

        if matches:
            df = pd.DataFrame(matches)
            df["input_original"] = df["input_key"].map(normalized_to_original)
            df["observation"] = ""
            dupes = df.duplicated(subset=["input_key"], keep=False)
            df.loc[dupes, "observation"] = "multiple matches"
            df["is_deactive"] = df["is_active"].apply(
                lambda x: None if pd.isna(x) else (not bool(x))
            )
            found_input_keys = set(df["input_key"].dropna().tolist())
            df = df.drop(columns=["input_key"]).sort_values(
                by=["primary_name", "input"]
            )
        else:
            df = pd.DataFrame(columns=columns)
            found_input_keys = set()

        not_found = [
            normalized_to_original[k]
            for k in normalized_to_original.keys()
            if k not in found_input_keys
        ]

        if not_found:
            missing = pd.DataFrame(
                {
                    "input_original": not_found,
                    "input": not_found,
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
            )
            df = pd.concat([df, missing], ignore_index=True)

        # Keep predictable output contract
        df = df.reindex(columns=columns)
        self.results = df
        return df.reset_index(drop=True)

    def to_dataframe(self, data=None):
        return (
            data if isinstance(data, pd.DataFrame) else pd.DataFrame(data or [])
        )  # noqa E501
