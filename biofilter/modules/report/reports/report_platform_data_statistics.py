from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import MetaData, Table, func

from biofilter.modules.db.models import (
    ETLDataSource,
    ETLPackage,
    ETLSourceSystem,
    Entity,
    EntityGroup,
    EntityRelationship,
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


class PlatformDataStatisticsReport(ReportBase):
    name = "platform_data_statistics"
    description = (
        "Platform-level statistics for dashboarding: entity counts by omic domain, "
        "variant counts by chromosome, relationship counts by group pair, and "
        "datasource latest load execution summaries."
    )

    columns = [
        "section",
        "metric",
        "dimension_1",
        "dimension_2",
        "value_number",
        "value_text",
        "as_of",
        "note",
    ]

    default_sections = [
        "entity_counts_by_group",
        "variant_counts_by_chromosome",
        "relationship_counts_by_group_pair",
        "datasource_latest_load",
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
            "sections": [
                "entity_counts_by_group",
                "variant_counts_by_chromosome",
                "relationship_counts_by_group_pair",
                "datasource_latest_load",
            ],
            "only_active_entities": True,
            "relationship_mode": "undirected",
            "include_totals": True,
        }

    @staticmethod
    def _cast_nullable_int_columns(df: pd.DataFrame) -> pd.DataFrame:
        if "value_number" in df.columns:
            df["value_number"] = pd.to_numeric(df["value_number"], errors="coerce").astype("Int64")
        return df

    def _table(self, table_name: str) -> Table:
        metadata = MetaData()
        return Table(table_name, metadata, autoload_with=self.db.engine)

    @staticmethod
    def _parse_sections(value: Any) -> list[str]:
        if value is None:
            return list(PlatformDataStatisticsReport.default_sections)

        if isinstance(value, (list, tuple, set)):
            items = [_norm(v) for v in value if _norm(v)]
        else:
            raw = _norm(value)
            if not raw:
                return list(PlatformDataStatisticsReport.default_sections)
            if "," in raw:
                items = [_norm(v) for v in raw.split(",") if _norm(v)]
            else:
                items = [raw]

        normalized = []
        for item in items:
            key = item.lower()
            if key not in normalized:
                normalized.append(key)

        valid = set(PlatformDataStatisticsReport.default_sections)
        invalid = [s for s in normalized if s not in valid]
        if invalid:
            raise ValueError(
                f"Invalid sections: {invalid}. Valid options: {sorted(valid)}"
            )
        return normalized

    @staticmethod
    def _parse_relationship_mode(value: Any) -> str:
        mode = _norm(value).lower() or "undirected"
        if mode not in {"undirected", "directed"}:
            return "undirected"
        return mode

    @staticmethod
    def _row(
        *,
        section: str,
        metric: str,
        as_of: str,
        dimension_1: str | None = None,
        dimension_2: str | None = None,
        value_number: int | None = None,
        value_text: str | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        return {
            "section": section,
            "metric": metric,
            "dimension_1": dimension_1,
            "dimension_2": dimension_2,
            "value_number": value_number,
            "value_text": value_text,
            "as_of": as_of,
            "note": note,
        }

    def _collect_entity_counts(
        self,
        *,
        as_of: str,
        only_active_entities: bool,
        include_totals: bool,
    ) -> list[dict[str, Any]]:
        section = "entity_counts_by_group"

        q = (
            self.session.query(
                EntityGroup.name.label("group_name"),
                func.count(Entity.id).label("n_entities"),
            )
            .join(Entity, Entity.group_id == EntityGroup.id)
        )

        if only_active_entities:
            # Treat NULL as active-like legacy rows; exclude explicit False.
            q = q.filter(Entity.is_active.isnot(False))

        rows = q.group_by(EntityGroup.name).order_by(EntityGroup.name).all()

        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                self._row(
                    section=section,
                    metric="entity_count",
                    dimension_1=str(r.group_name),
                    value_number=int(r.n_entities or 0),
                    as_of=as_of,
                )
            )

        if include_totals:
            q_total = self.session.query(func.count(Entity.id))
            if only_active_entities:
                q_total = q_total.filter(Entity.is_active.isnot(False))
            total = int(q_total.scalar() or 0)
            out.append(
                self._row(
                    section=section,
                    metric="entity_count_total",
                    dimension_1="ALL_GROUPS",
                    value_number=total,
                    as_of=as_of,
                )
            )

        return out

    def _collect_variant_counts(
        self,
        *,
        as_of: str,
        include_totals: bool,
    ) -> list[dict[str, Any]]:
        section = "variant_counts_by_chromosome"
        out: list[dict[str, Any]] = []

        try:
            variant_masters = self._table("variant_masters")
        except Exception:
            out.append(
                self._row(
                    section=section,
                    metric="variant_table_missing",
                    dimension_1="variant_masters",
                    value_number=0,
                    as_of=as_of,
                    note="Table variant_masters not available in this database.",
                )
            )
            return out

        rows = (
            self.session.query(
                variant_masters.c.chromosome.label("chromosome"),
                func.count().label("n_variants"),
            )
            .group_by(variant_masters.c.chromosome)
            .order_by(variant_masters.c.chromosome)
            .all()
        )

        for r in rows:
            out.append(
                self._row(
                    section=section,
                    metric="variant_count",
                    dimension_1=str(r.chromosome),
                    value_number=int(r.n_variants or 0),
                    as_of=as_of,
                )
            )

        if include_totals:
            total = int(self.session.query(func.count()).select_from(variant_masters).scalar() or 0)
            out.append(
                self._row(
                    section=section,
                    metric="variant_count_total",
                    dimension_1="ALL_CHROMOSOMES",
                    value_number=total,
                    as_of=as_of,
                )
            )

        return out

    def _collect_relationship_counts(
        self,
        *,
        as_of: str,
        relationship_mode: str,
        include_totals: bool,
    ) -> list[dict[str, Any]]:
        section = "relationship_counts_by_group_pair"

        group_rows = self.session.query(EntityGroup.id, EntityGroup.name).all()
        group_map = {int(gid): str(name) for gid, name in group_rows}

        directed_rows = (
            self.session.query(
                EntityRelationship.entity_1_group_id.label("g1"),
                EntityRelationship.entity_2_group_id.label("g2"),
                func.count(EntityRelationship.id).label("n_rels"),
            )
            .group_by(
                EntityRelationship.entity_1_group_id,
                EntityRelationship.entity_2_group_id,
            )
            .all()
        )

        out: list[dict[str, Any]] = []

        if relationship_mode == "directed":
            for r in directed_rows:
                g1 = group_map.get(int(r.g1), "Unknown") if r.g1 is not None else "Unknown"
                g2 = group_map.get(int(r.g2), "Unknown") if r.g2 is not None else "Unknown"
                out.append(
                    self._row(
                        section=section,
                        metric="relationship_count",
                        dimension_1=g1,
                        dimension_2=g2,
                        value_number=int(r.n_rels or 0),
                        as_of=as_of,
                    )
                )
        else:
            pair_counts: dict[tuple[str, str], int] = {}
            for r in directed_rows:
                g1 = group_map.get(int(r.g1), "Unknown") if r.g1 is not None else "Unknown"
                g2 = group_map.get(int(r.g2), "Unknown") if r.g2 is not None else "Unknown"
                pair = tuple(sorted([g1, g2]))
                pair_counts[pair] = pair_counts.get(pair, 0) + int(r.n_rels or 0)

            for pair in sorted(pair_counts.keys()):
                out.append(
                    self._row(
                        section=section,
                        metric="relationship_count",
                        dimension_1=pair[0],
                        dimension_2=pair[1],
                        value_number=int(pair_counts[pair]),
                        as_of=as_of,
                    )
                )

        if include_totals:
            total = int(self.session.query(func.count(EntityRelationship.id)).scalar() or 0)
            out.append(
                self._row(
                    section=section,
                    metric="relationship_count_total",
                    dimension_1=("ALL_GROUP_PAIRS_DIRECTED" if relationship_mode == "directed" else "ALL_GROUP_PAIRS_UNDIRECTED"),
                    value_number=total,
                    as_of=as_of,
                )
            )

        return out

    def _collect_datasource_latest_load(
        self,
        *,
        as_of: str,
        include_totals: bool,
    ) -> list[dict[str, Any]]:
        section = "datasource_latest_load"

        ds_rows = (
            self.session.query(
                ETLDataSource.id.label("data_source_id"),
                ETLDataSource.name.label("data_source"),
                ETLSourceSystem.name.label("source_system"),
            )
            .join(ETLSourceSystem, ETLSourceSystem.id == ETLDataSource.source_system_id)
            .order_by(ETLSourceSystem.name, ETLDataSource.name)
            .all()
        )

        load_rows = (
            self.session.query(
                ETLPackage.data_source_id.label("data_source_id"),
                ETLPackage.id.label("etl_package_id"),
                ETLPackage.load_end.label("load_end"),
                ETLPackage.load_status.label("load_status"),
                ETLPackage.load_rows.label("load_rows"),
                ETLPackage.created_at.label("created_at"),
            )
            .filter(func.lower(ETLPackage.operation_type) == "load")
            .all()
        )

        latest_by_ds: dict[int, dict[str, Any]] = {}
        for r in load_rows:
            ds_id = int(r.data_source_id)
            ts = r.load_end or r.created_at
            current = latest_by_ds.get(ds_id)
            if current is None:
                latest_by_ds[ds_id] = {
                    "etl_package_id": int(r.etl_package_id),
                    "load_end": r.load_end,
                    "load_status": r.load_status,
                    "load_rows": r.load_rows,
                    "sort_ts": ts,
                }
                continue

            current_ts = current.get("sort_ts")
            if (ts is not None and current_ts is None) or (ts is not None and current_ts is not None and ts > current_ts):
                latest_by_ds[ds_id] = {
                    "etl_package_id": int(r.etl_package_id),
                    "load_end": r.load_end,
                    "load_status": r.load_status,
                    "load_rows": r.load_rows,
                    "sort_ts": ts,
                }

        out: list[dict[str, Any]] = []
        as_of_dt = datetime.fromisoformat(as_of)
        with_load_count = 0

        for ds in ds_rows:
            ds_id = int(ds.data_source_id)
            ds_name = str(ds.data_source)
            ss_name = str(ds.source_system)
            latest = latest_by_ds.get(ds_id)

            if latest is None:
                out.append(
                    self._row(
                        section=section,
                        metric="latest_load_status",
                        dimension_1=ss_name,
                        dimension_2=ds_name,
                        value_text="not_loaded",
                        as_of=as_of,
                        note="No load package found for this data source.",
                    )
                )
                continue

            with_load_count += 1
            load_end = latest.get("load_end")
            load_end_txt = load_end.isoformat() if load_end is not None else None
            load_status = _norm(latest.get("load_status")) or "unknown"
            load_rows = latest.get("load_rows")
            pkg_id = latest.get("etl_package_id")

            if load_end is not None:
                age_days = int((as_of_dt - load_end.replace(tzinfo=timezone.utc)).total_seconds() // 86400) if load_end.tzinfo is None else int((as_of_dt - load_end).total_seconds() // 86400)  # noqa: E501
            else:
                age_days = None

            out.append(
                self._row(
                    section=section,
                    metric="latest_load_package_id",
                    dimension_1=ss_name,
                    dimension_2=ds_name,
                    value_number=int(pkg_id) if pkg_id is not None else None,
                    as_of=as_of,
                )
            )
            out.append(
                self._row(
                    section=section,
                    metric="latest_load_status",
                    dimension_1=ss_name,
                    dimension_2=ds_name,
                    value_text=load_status,
                    as_of=as_of,
                )
            )
            out.append(
                self._row(
                    section=section,
                    metric="latest_load_end",
                    dimension_1=ss_name,
                    dimension_2=ds_name,
                    value_text=load_end_txt,
                    as_of=as_of,
                )
            )
            out.append(
                self._row(
                    section=section,
                    metric="latest_load_rows",
                    dimension_1=ss_name,
                    dimension_2=ds_name,
                    value_number=int(load_rows) if load_rows is not None else None,
                    as_of=as_of,
                )
            )
            out.append(
                self._row(
                    section=section,
                    metric="latest_load_age_days",
                    dimension_1=ss_name,
                    dimension_2=ds_name,
                    value_number=age_days,
                    as_of=as_of,
                )
            )

        if include_totals:
            out.append(
                self._row(
                    section=section,
                    metric="data_source_count_total",
                    dimension_1="ALL_DATASOURCES",
                    value_number=len(ds_rows),
                    as_of=as_of,
                )
            )
            out.append(
                self._row(
                    section=section,
                    metric="data_source_with_load_count_total",
                    dimension_1="ALL_DATASOURCES",
                    value_number=with_load_count,
                    as_of=as_of,
                )
            )

        return out

    def run(self):
        sections = self._parse_sections(self.param("sections", None))
        only_active_entities = _parse_bool(self.param("only_active_entities", True), True)
        relationship_mode = self._parse_relationship_mode(self.param("relationship_mode", "undirected"))
        include_totals = _parse_bool(self.param("include_totals", True), True)

        as_of = datetime.now(timezone.utc).isoformat()
        records: list[dict[str, Any]] = []

        if "entity_counts_by_group" in sections:
            records.extend(
                self._collect_entity_counts(
                    as_of=as_of,
                    only_active_entities=only_active_entities,
                    include_totals=include_totals,
                )
            )

        if "variant_counts_by_chromosome" in sections:
            records.extend(
                self._collect_variant_counts(
                    as_of=as_of,
                    include_totals=include_totals,
                )
            )

        if "relationship_counts_by_group_pair" in sections:
            records.extend(
                self._collect_relationship_counts(
                    as_of=as_of,
                    relationship_mode=relationship_mode,
                    include_totals=include_totals,
                )
            )

        if "datasource_latest_load" in sections:
            records.extend(
                self._collect_datasource_latest_load(
                    as_of=as_of,
                    include_totals=include_totals,
                )
            )

        out = pd.DataFrame(records)
        out = out.reindex(columns=self.columns)
        out = self._cast_nullable_int_columns(out)
        out = out.sort_values(
            by=["section", "metric", "dimension_1", "dimension_2"],
            na_position="last",
        )
        self.results = out
        return out.reset_index(drop=True)
