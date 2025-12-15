import pandas as pd
from sqlalchemy import func
from biofilter.report.reports.base_report import ReportBase
from biofilter.db.models import ETLPackage, ETLDataSource, ETLSourceSystem


class ETLStatusReport(ReportBase):
    name = "etl_status"
    description = (
        "Shows the latest successful (good) ETL packages per DataSource "
        "for extract/transform/load, highlighting stale steps when hashes "
        "do not align with the most recent extract."
    )

    @classmethod
    def explain(cls) -> str:
        return """\
📦 ETL Status (Latest Good)

This report summarizes ETL execution health per DataSource by selecting:
- The most recent GOOD extract package (completed or up-to-date)
- The most recent GOOD transform package
- The most recent GOOD load package

If the latest extract is newer but transform/load are missing or not aligned
(by hash), the report still shows the last good transform/load and flags them
as stale (not aligned with latest extract).
"""

    def run(self) -> pd.DataFrame:
        # Optional filters (strings or lists)
        source_system = self.params.get("source_system")  # e.g. "NCBI" or ["NCBI","EBI"]
        data_sources = self.params.get("data_sources")    # e.g. "dbsnp_chr1" or ["hgnc","mondo"]
        only_active = self.params.get("only_active", True)

        # ----------------------------
        # 1) Pull packages + metadata
        # ----------------------------
        q = (
            self.session.query(
                ETLSourceSystem.id.label("source_system_id"),
                ETLSourceSystem.name.label("source_system"),
                ETLDataSource.id.label("data_source_id"),
                ETLDataSource.name.label("data_source"),
                ETLDataSource.data_type.label("data_type"),
                ETLDataSource.format.label("format"),
                ETLDataSource.dtp_version.label("dtp_version"),
                ETLDataSource.schema_version.label("schema_version"),
                ETLDataSource.active.label("data_source_active"),
                ETLSourceSystem.active.label("source_system_active"),
                ETLPackage.id.label("etl_package_id"),
                ETLPackage.created_at,
                ETLPackage.status,
                ETLPackage.operation_type,
                ETLPackage.note,
                ETLPackage.extract_status,
                ETLPackage.extract_start,
                ETLPackage.extract_end,
                ETLPackage.extract_hash,
                ETLPackage.transform_status,
                ETLPackage.transform_start,
                ETLPackage.transform_end,
                ETLPackage.transform_hash,
                ETLPackage.load_status,
                ETLPackage.load_start,
                ETLPackage.load_end,
                ETLPackage.load_hash,
                ETLPackage.stats,
            )
            .join(ETLDataSource, ETLDataSource.id == ETLPackage.data_source_id)
            .join(ETLSourceSystem, ETLSourceSystem.id == ETLDataSource.source_system_id)
        )

        if only_active:
            q = q.filter(ETLDataSource.active.is_(True), ETLSourceSystem.active.is_(True))

        # case-insensitive filters
        q = self._filter_ci(q, ETLSourceSystem.name, source_system)
        q = self._filter_ci(q, ETLDataSource.name, data_sources)

        rows = q.all()
        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        # ----------------------------
        # 2) Define what "GOOD" means
        # ----------------------------
        GOOD_EXTRACT = {"completed", "up-to-date"}
        GOOD_STEP = {"completed"}  # transform/load typically "completed" only

        def is_good_extract(r):
            return (r["operation_type"] == "extract") and (r["extract_status"] in GOOD_EXTRACT)

        def is_good_transform(r):
            return (r["operation_type"] == "transform") and (r["transform_status"] in GOOD_STEP)

        def is_good_load(r):
            return (r["operation_type"] == "load") and (r["load_status"] in GOOD_STEP)

        # Ensure sorting (most recent first)
        df = df.sort_values(["data_source_id", "created_at", "etl_package_id"], ascending=[True, False, False])

        # ----------------------------
        # 3) Pick latest good per step
        # ----------------------------
        out_rows = []
        for ds_id, g in df.groupby("data_source_id", sort=False):
            g = g.copy()

            # Latest GOOD extract
            g_extract = g[g.apply(is_good_extract, axis=1)]
            latest_extract = g_extract.iloc[0] if len(g_extract) else None

            # Latest GOOD transform
            g_transform = g[g.apply(is_good_transform, axis=1)]
            latest_transform = g_transform.iloc[0] if len(g_transform) else None

            # Latest GOOD load
            g_load = g[g.apply(is_good_load, axis=1)]
            latest_load = g_load.iloc[0] if len(g_load) else None

            # Alignment checks (hash-based)
            extract_hash = latest_extract["extract_hash"] if latest_extract is not None else None
            transform_hash = latest_transform["transform_hash"] if latest_transform is not None else None
            load_hash = latest_load["load_hash"] if latest_load is not None else None

            transform_aligned = (extract_hash is not None) and (transform_hash == extract_hash)
            load_aligned = (transform_hash is not None) and (load_hash == transform_hash)

            # Base metadata (same for all rows in g)
            base = g.iloc[0]

            out_rows.append(
                {
                    "source_system_id": base["source_system_id"],
                    "source_system": base["source_system"],
                    "data_source_id": base["data_source_id"],
                    "data_source": base["data_source"],
                    "data_type": base["data_type"],
                    "format": base["format"],
                    "dtp_version": base["dtp_version"],
                    "schema_version": base["schema_version"],

                    # Latest good EXTRACT
                    "extract_package_id": None if latest_extract is None else int(latest_extract["etl_package_id"]),
                    "extract_status": None if latest_extract is None else latest_extract["extract_status"],
                    "extract_end": None if latest_extract is None else latest_extract["extract_end"],
                    "extract_hash": extract_hash,

                    # Latest good TRANSFORM (may be stale vs extract)
                    "transform_package_id": None if latest_transform is None else int(latest_transform["etl_package_id"]),
                    "transform_status": None if latest_transform is None else latest_transform["transform_status"],
                    "transform_end": None if latest_transform is None else latest_transform["transform_end"],
                    "transform_hash": transform_hash,
                    "transform_aligned_with_latest_extract": transform_aligned,

                    # Latest good LOAD (may be stale vs transform)
                    "load_package_id": None if latest_load is None else int(latest_load["etl_package_id"]),
                    "load_status": None if latest_load is None else latest_load["load_status"],
                    "load_end": None if latest_load is None else latest_load["load_end"],
                    "load_hash": load_hash,
                    "load_aligned_with_latest_transform": load_aligned,

                    # Convenience: overall “good pipeline?”
                    "pipeline_ok": bool(latest_extract is not None and transform_aligned and load_aligned),

                    # Optional: show last error (if any) from newest FAILED package
                    "latest_error": self._get_latest_error_message(g),
                }
            )

        out = pd.DataFrame(out_rows)

        # Optional nice ordering
        # cols = [
        #     "source_system", "data_source", "data_source_id", "data_type", "format",
        #     "dtp_version", "schema_version",
        #     "extract_package_id", "extract_status", "extract_end", "extract_hash",
        #     "transform_package_id", "transform_status", "transform_end", "transform_hash",
        #     "transform_aligned_with_latest_extract",
        #     "load_package_id", "load_status", "load_end", "load_hash",
        #     "load_aligned_with_latest_transform",
        #     "pipeline_ok",
        #     "latest_error",
        # ]
        # Optional nice ordering
        cols = [
            "source_system", "data_source", "data_type",
            "pipeline_ok",
            "extract_package_id", "extract_status", "extract_end",
            "transform_package_id", "transform_status", "transform_end", 
            "transform_aligned_with_latest_extract",
            "load_package_id", "load_status", "load_end",
            "load_aligned_with_latest_transform",
            "latest_error",
        ]
        cols = [c for c in cols if c in out.columns]
        return out[cols].sort_values(["source_system", "data_source"])

    def _get_latest_error_message(self, g: pd.DataFrame) -> str | None:
        """
        Try to surface the most recent failure message, if present.
        Assumes stats may contain JSON with {"error": "...", "step": "..."}.
        """
        failed = g[g["status"] == "failed"]
        if failed.empty:
            return None

        newest = failed.iloc[0]
        stats = newest.get("stats")
        if not stats:
            return None

        # stats might already be dict-like or a JSON string
        try:
            if isinstance(stats, dict):
                return stats.get("error") or str(stats)
            # common case: JSON stored as string
            import json
            parsed = json.loads(stats)
            return parsed.get("error") or str(parsed)
        except Exception:
            return str(stats)
