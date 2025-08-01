from biofilter.report.reports.base_report import ReportBase
from biofilter.db.models import Entity, EntityName, EntityGroup  # DataSource
from sqlalchemy.orm import aliased
from sqlalchemy import func
import pandas as pd

" TODO: Add data-source as FK in Entity to relation run"


class EntityFilterReport(ReportBase):
    name = "entity_filter"
    description = "Validates input list of entity names and returns all matching entities, including conflict and status flags."  # noqa E501

    def run(self):
        input_data = self.params.get("input_data", [])
        if not input_data:
            raise ValueError("Missing required parameter: input_data")

        # Mapeia input lowercase para o valor original
        input_map = {x.lower(): x for x in input_data}
        input_lc = list(input_map.keys())

        # Aliases for primary name lookup
        PrimaryName = aliased(EntityName)

        # Query
        matches = (
            self.session.query(
                EntityName.name.label("input_original"),
                EntityName.name.label("input"),
                EntityName.is_primary.label("is_primary"),
                Entity.id.label("entity_id"),
                PrimaryName.name.label("primary_name"),
                Entity.group_id.label("group_id"),
                EntityGroup.name.label("group_name"),
                Entity.has_conflict,
                Entity.is_deactive,
                EntityName.data_source_id,
                # DataSource.name.label("data_source_name"),

            )
            .join(Entity, Entity.id == EntityName.entity_id)
            .join(PrimaryName, PrimaryName.entity_id == Entity.id)
            .join(EntityGroup, Entity.group_id == EntityGroup.id, isouter=True)
            .filter(PrimaryName.is_primary.is_(True))
            .filter(func.lower(EntityName.name).in_(input_lc))
            .all()
        )

        df = pd.DataFrame(matches)

        df["input_original"] = df["input"].str.lower().map(input_map)

        if not df.empty:
            # Adds notes for duplicate entries
            df["observation"] = ""
            dupes = df.duplicated(subset=["input"], keep=False)
            df.loc[dupes, "observation"] = "multiple matches (conflict)"

            # Order by Primary_name
            df = df.sort_values(by=["primary_name", "input"]).reset_index(
                drop=True
            )  # noqa E501
        else:
            # Dataframe create with all outcome reusults
            df = pd.DataFrame(
                columns=[
                    "input_original",
                    "input",
                    "is_primary",
                    "entity_id",
                    "primary_name",
                    "group_id",
                    "group_name",
                    "has_conflict",
                    "is_deactive",
                    "data_source_id",
                    # "data_source_name",
                    "observation",
                ]
            )

        found_inputs_lc = set(df["input"].str.lower().unique())
        not_found = [
            input_map[x] for x in input_map if x not in found_inputs_lc
        ]  # noqa E501

        if not_found:
            missing = pd.DataFrame({
                "input_original": not_found,
                "input": not_found,
                "name": None,
                "is_primary": None,
                "entity_id": None,
                "primary_name": None,
                "group_id": None,
                "group_name": None,
                "has_conflict": None,
                "is_deactive": None,
                "data_source_id": None,
                # "data_source_name": None,
                "observation": "not found"
            })
            df = pd.concat([df, missing], ignore_index=True)

        self.results = df
        return df

    def to_dataframe(self, data=None):
        return data if isinstance(data, pd.DataFrame) else pd.DataFrame(data or [])  # noqa E501
