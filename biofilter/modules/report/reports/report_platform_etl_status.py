"""
Build status: what ran to produce this bundle, and whether it holds up.

Migrated from `etl_status` (ADR-004 §6, §2.12). A platform report — it
describes the bundle, not the biology in it.

The rewrite changed what "ok" means, because the relational version said
`False` for 51 of this bundle's 68 sources and none of them was broken.
Three different situations were collapsed into one word; `pipeline_state`
now tells them apart. See the guide.
"""

from __future__ import annotations

from typing import Any, Sequence

import pyarrow as pa

from biofilter.modules.report.reports.base_report import ReportBase

#: An extract that produced usable output. `up-to-date` means the source
#: had not changed, which is a success.
GOOD_EXTRACT = ("completed", "up-to-date")
GOOD_STEP = ("completed",)

#: Sources whose branch writes parquet directly and has no load stage
#: (ADR-003 §2.3). Identified by data type, which is how the build splits
#: the branches.
VARIANT_DATA_TYPE = "Variant"


class PlatformETLStatusReport(ReportBase):
    name = "platform_etl_status"
    description = (
        "One row per data source: the latest good extract, transform and load, "
        "whether each stage ran on the previous one's output, and whether "
        "anything is known to be wrong."
    )

    requires = ("etl_data_sources", "etl_packages", "etl_source_systems")

    COLUMNS = (
        "source_system",
        "data_source",
        "data_type",
        "branch",
        "data_source_active",
        "dtp_version",
        "schema_version",
        "format",
        "extract_package_id",
        "extract_status",
        "extract_end",
        "extract_hash",
        "transform_package_id",
        "transform_status",
        "transform_end",
        "transform_aligned",
        "load_package_id",
        "load_status",
        "load_end",
        "load_aligned",
        "pipeline_state",
        "pipeline_ok",
        "latest_error",
    )

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.COLUMNS

    @classmethod
    def example_input(cls):
        return {"only_active": False, "source_system": None, "data_sources": None}

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

        filters = []
        if only_active:
            filters.append("ds.active AND ss.active")
        if systems:
            filters.append(f"lower(ss.name) IN ({self._sql_list(systems)})")
        if sources:
            filters.append(f"lower(ds.name) IN ({self._sql_list(sources)})")
        where = ("WHERE " + " AND ".join(filters)) if filters else ""

        good_extract = self._sql_list(list(GOOD_EXTRACT))
        good_step = self._sql_list(list(GOOD_STEP))

        return self.sql(
            f"""
            WITH sources AS (
                SELECT
                    ds.id AS data_source_id,
                    ss.name AS source_system,
                    ds.name AS data_source,
                    ds.data_type,
                    CASE WHEN ds.data_type = '{VARIANT_DATA_TYPE}'
                         THEN 'variant' ELSE 'core' END AS branch,
                    ds.active AS data_source_active,
                    ds.dtp_version,
                    ds.schema_version,
                    ds.format
                FROM etl_data_sources ds
                LEFT JOIN etl_source_systems ss ON ss.id = ds.source_system_id
                {where}
            ),
            -- Each stage is its own package row, and the hash of the
            -- extract's output is carried forward into the transform's and
            -- then the load's. "Aligned" means a stage ran on the previous
            -- one's output, not that two unrelated digests happen to match.
            latest_extract AS (
                SELECT p.data_source_id, p.id AS package_id, p.extract_status,
                       p.extract_end, p.extract_hash
                FROM etl_packages p
                WHERE p.operation_type = 'extract'
                  AND p.extract_status IN ({good_extract})
                QUALIFY row_number() OVER (
                    PARTITION BY p.data_source_id ORDER BY p.id DESC
                ) = 1
            ),
            latest_transform AS (
                SELECT p.data_source_id, p.id AS package_id, p.transform_status,
                       p.transform_end, p.transform_hash
                FROM etl_packages p
                WHERE p.operation_type = 'transform'
                  AND p.transform_status IN ({good_step})
                QUALIFY row_number() OVER (
                    PARTITION BY p.data_source_id ORDER BY p.id DESC
                ) = 1
            ),
            latest_load AS (
                SELECT p.data_source_id, p.id AS package_id, p.load_status,
                       p.load_end, p.load_hash
                FROM etl_packages p
                WHERE p.operation_type = 'load'
                  AND p.load_status IN ({good_step})
                QUALIFY row_number() OVER (
                    PARTITION BY p.data_source_id ORDER BY p.id DESC
                ) = 1
            ),
            -- The most recent failure in a source's history, whether or
            -- not it was later retried successfully. Composed rather than
            -- read straight from `note`, because every failed package in
            -- this bundle has a null note: "it failed and said nothing"
            -- is still worth surfacing, and reporting null hid it.
            latest_error AS (
                SELECT
                    p.data_source_id,
                    concat_ws(' ',
                        p.operation_type || ' failed',
                        '(package ' || CAST(p.id AS VARCHAR) || ')',
                        nullif(trim(coalesce(p.note, '')), '')
                    ) AS note
                FROM etl_packages p
                WHERE lower(coalesce(p.status, '')) LIKE '%fail%'
                   OR lower(coalesce(p.extract_status, '')) LIKE '%fail%'
                   OR lower(coalesce(p.transform_status, '')) LIKE '%fail%'
                   OR lower(coalesce(p.load_status, '')) LIKE '%fail%'
                QUALIFY row_number() OVER (
                    PARTITION BY p.data_source_id ORDER BY p.id DESC
                ) = 1
            ),
            judged AS (
                SELECT
                    s.*,
                    e.package_id AS extract_package_id,
                    e.extract_status,
                    e.extract_end,
                    e.extract_hash,
                    t.package_id AS transform_package_id,
                    t.transform_status,
                    t.transform_end,
                    t.transform_hash,
                    l.package_id AS load_package_id,
                    l.load_status,
                    l.load_end,
                    l.load_hash,
                    err.note AS latest_error,
                    -- Null, not false, when there is nothing to compare.
                    -- The relational version returned false, which reads
                    -- as "this is wrong" rather than "this is unproven".
                    CASE
                        WHEN t.package_id IS NULL THEN NULL
                        WHEN e.extract_hash IS NULL OR t.transform_hash IS NULL
                             THEN NULL
                        ELSE t.transform_hash = e.extract_hash
                    END AS transform_aligned,
                    CASE
                        WHEN s.branch = 'variant' THEN NULL
                        WHEN l.package_id IS NULL THEN NULL
                        WHEN t.transform_hash IS NULL OR l.load_hash IS NULL
                             THEN NULL
                        ELSE l.load_hash = t.transform_hash
                    END AS load_aligned
                FROM sources s
                LEFT JOIN latest_extract   e   ON e.data_source_id = s.data_source_id
                LEFT JOIN latest_transform t   ON t.data_source_id = s.data_source_id
                LEFT JOIN latest_load      l   ON l.data_source_id = s.data_source_id
                LEFT JOIN latest_error     err ON err.data_source_id = s.data_source_id
            ),
            stated AS (
                SELECT
                    *,
                    CASE
                        WHEN extract_package_id IS NULL
                         AND transform_package_id IS NULL
                         AND load_package_id IS NULL      THEN 'never_run'
                        WHEN extract_package_id IS NULL
                          OR transform_package_id IS NULL THEN 'incomplete'
                        -- The variant branch writes parquet straight from
                        -- transform; there is no load stage to miss.
                        WHEN branch = 'core'
                         AND load_package_id IS NULL      THEN 'incomplete'
                        WHEN transform_aligned = false
                          OR load_aligned = false         THEN 'misaligned'
                        WHEN transform_aligned IS NULL
                          OR (branch = 'core' AND load_aligned IS NULL)
                                                          THEN 'unverifiable'
                        ELSE 'ok'
                    END AS pipeline_state
                FROM judged
            )
            SELECT
                source_system,
                data_source,
                data_type,
                branch,
                data_source_active,
                dtp_version,
                schema_version,
                format,
                extract_package_id,
                extract_status,
                extract_end,
                extract_hash,
                transform_package_id,
                transform_status,
                transform_end,
                transform_aligned,
                load_package_id,
                load_status,
                load_end,
                load_aligned,
                pipeline_state,
                -- "Nothing is known to be wrong", which is not the same as
                -- "everything is proven right" — `pipeline_state` carries
                -- that distinction.
                pipeline_state IN ('ok', 'unverifiable') AS pipeline_ok,
                latest_error
            FROM stated
            ORDER BY source_system, data_source
            """
        )
