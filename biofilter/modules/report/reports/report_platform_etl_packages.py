"""
Build log: every ETL package that went into this bundle.

Migrated from `etl_packages` (ADR-004 §6, §2.12). Where
`platform_etl_status` summarises one row per data source,
this is the unaggregated record — one row per package, which is one
stage of one run.
"""

from __future__ import annotations

from typing import Any, Sequence

import pyarrow as pa

from biofilter.modules.report.reports.base_report import ReportBase


class PlatformETLPackagesReport(ReportBase):
    name = "platform_etl_packages"
    description = (
        "One row per ETL package: which source, which stage, when it ran, how "
        "many rows it moved, and the hash it carried forward. The raw record "
        "behind platform_etl_status."
    )

    requires = ("etl_data_sources", "etl_packages", "etl_source_systems")

    COLUMNS = (
        "package_id",
        "source_system",
        "data_source",
        "data_type",
        "operation_type",
        "status",
        "version_tag",
        "created_at",
        "extract_status",
        "extract_start",
        "extract_end",
        "extract_rows",
        "extract_hash",
        "transform_status",
        "transform_start",
        "transform_end",
        "transform_rows",
        "transform_hash",
        "load_status",
        "load_start",
        "load_end",
        "load_rows",
        "load_hash",
        "note",
        "stats",
    )

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.COLUMNS

    @classmethod
    def example_input(cls):
        return {
            "only_active": False,
            "source_system": None,
            "data_sources": None,
            "operation_type": None,
        }

    # ------------------------------------------------------------------
    @staticmethod
    def _as_list(value: Any) -> list[str] | None:
        if value is None:
            return None
        values = value if isinstance(value, (list, tuple, set)) else [value]
        cleaned = [str(v).strip().lower() for v in values if str(v).strip()]
        return cleaned or None

    @staticmethod
    def _sql_list(values: list[str]) -> str:
        return ", ".join("'" + v.replace("'", "''") + "'" for v in values)

    @staticmethod
    def _parse_bool(value: Any, default: bool) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
        return default

    def run(self) -> pa.Table:
        only_active = self._parse_bool(self.param("only_active"), False)
        systems = self._as_list(self.param("source_system"))
        sources = self._as_list(self.param("data_sources"))
        operations = self._as_list(self.param("operation_type"))

        filters = []
        if only_active:
            filters.append("ds.active AND ss.active")
        if systems:
            filters.append(f"lower(ss.name) IN ({self._sql_list(systems)})")
        if sources:
            filters.append(f"lower(ds.name) IN ({self._sql_list(sources)})")
        if operations:
            filters.append(f"lower(p.operation_type) IN ({self._sql_list(operations)})")
        where = ("WHERE " + " AND ".join(filters)) if filters else ""

        return self.sql(
            f"""
            SELECT
                p.id                AS package_id,
                ss.name             AS source_system,
                ds.name             AS data_source,
                ds.data_type,
                p.operation_type,
                p.status,
                p.version_tag,
                p.created_at,
                p.extract_status,
                p.extract_start,
                p.extract_end,
                p.extract_rows,
                p.extract_hash,
                p.transform_status,
                p.transform_start,
                p.transform_end,
                p.transform_rows,
                p.transform_hash,
                p.load_status,
                p.load_start,
                p.load_end,
                p.load_rows,
                p.load_hash,
                p.note,
                CAST(p.stats AS VARCHAR) AS stats
            FROM etl_packages p
            LEFT JOIN etl_data_sources ds ON ds.id = p.data_source_id
            LEFT JOIN etl_source_systems ss ON ss.id = ds.source_system_id
            {where}
            ORDER BY p.id
            """
        )
