from biofilter.report.reports.base_report import ReportBase
from biofilter.db.models import Entity, EntityName, EntityGroup
from sqlalchemy.orm import aliased
import pandas as pd


class EntityFilterReport(ReportBase):
    name = "entity_filter"
    description = "Validates input list of entity names and returns all matching entities, including conflict and status flags."

    def run(self):
        input_data = self.params.get("input_data", [])
        if not input_data:
            raise ValueError("Missing required parameter: input_data")

        # Aliases for primary name lookup
        PrimaryName = aliased(EntityName)

        # Query
        matches = (
            self.session.query(
                EntityName.name.label("input"),
                EntityName.is_primary.label("is_primary"),
                Entity.id.label("entity_id"),
                PrimaryName.name.label("primary_name"),
                Entity.group_id.label("group_id"),
                EntityGroup.name.label("group_name"),
                Entity.has_conflict,
                Entity.is_deactive,
                EntityName.data_source_id,
            )
            .join(Entity, Entity.id == EntityName.entity_id)
            .join(PrimaryName, PrimaryName.entity_id == Entity.id)
            .join(EntityGroup, Entity.group_id == EntityGroup.id, isouter=True)
            .filter(PrimaryName.is_primary.is_(True))
            .filter(EntityName.name.in_(input_data))
            .all()
        )

        df = pd.DataFrame(matches)

        if not df.empty:
            # Adiciona observações para entradas duplicadas
            df["observation"] = ""
            dupes = df.duplicated(subset=["input"], keep=False)
            df.loc[dupes, "observation"] = "multiple matches (conflict)"

            # Ordena pela primary_name
            df = df.sort_values(by=["primary_name", "input"]).reset_index(drop=True)
        else:
            # Criar DataFrame vazio com colunas corretas
            df = pd.DataFrame(columns=[
                "input", "is_primary", "entity_id", "primary_name", "group_id",
                "group_name", "has_conflict", "is_deactive", "data_source_id", "observation"
            ])

        # Adiciona os não encontrados
        found_inputs = set(df["input"].unique())
        not_found = [x for x in input_data if x not in found_inputs]
        if not_found:
            missing = pd.DataFrame({
                "input": not_found,
                "is_primary": None,
                "entity_id": None,
                "primary_name": None,
                "group_id": None,
                "group_name": None,
                "has_conflict": None,
                "is_deactive": None,
                "data_source_id": None,
                "observation": "not found"
            })
            df = pd.concat([df, missing], ignore_index=True)

        self.results = df
        return df

    def to_dataframe(self, data=None):
        return data if isinstance(data, pd.DataFrame) else pd.DataFrame(data or [])
