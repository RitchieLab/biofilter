# biofilter/report/reports/report_etl_packages_details.py

import pandas as pd
from sqlalchemy import select
from biofilter.report.reports.base_report import ReportBase
from biofilter.db.models import (
    ETLSourceSystem,
    ETLDataSource,
    ETLPackage,
)


class ETLPackagesReport(ReportBase):
    """
    Detailed audit report of ETL packages.

    This report exposes the raw state of all ETLPackage records, joined with
    ETLDataSource and ETLSourceSystem metadata. It is intended for debugging,
    monitoring, and validating ETL execution behavior.
    """

    name = "etl_packages"
    description = (
        "Shows detailed ETL package execution records, including extract / "
        "transform / load statuses, timestamps, row counts, hashes, and stats."
    )

    # -----------------------------
    # Helpers / Schema contract
    # -----------------------------
    @classmethod
    def available_columns(cls) -> list[str]:
        """
        Internal column keys that can be requested via output_columns=[...].
        (These are stable keys; display names can be changed later.)
        """
        return [
            # input / match
            "...",

            # provenance
            "data_source",
            "source_system",
        ]
       
    @classmethod
    def explain(cls) -> str:
        return """\
📦 ETL Packages – Detailed Audit Report
======================================

This report provides a **raw, non-aggregated** view of the ETL execution state.

Each row corresponds to **one ETLPackage record**, which may represent:
- one ETL stage (extract / transform / load), or
- one full execution attempt, depending on how the ETL was triggered.

The report joins:
- ETLSourceSystem (e.g. NCBI, Ensembl, UniProt)
- ETLDataSource (e.g. dbSNP_chr1, ensembl, hgnc)
- ETLPackage (execution metadata)

This report is intentionally *not consolidated*.
It is designed for:
- Debugging failed or stuck jobs
- Auditing execution history
- Understanding how many packages were created per data source
- Verifying status transitions across ETL stages

Recommended usage:
- Use this report to identify inconsistencies
- Fix ETL status logic
- Only then create consolidated / dashboard-style reports
"""

    def run(self) -> pd.DataFrame:
        # Optional filters (strings or lists)
        source_system = self.params.get("source_system")   # "NCBI" or ["NCBI","EBI"]
        data_sources  = self.params.get("data_sources")    # "dbsnp_chr1" or ["hgnc","mondo"]
        only_active   = self.params.get("only_active", True)

        try:
            stmt = (
                select(
                    ETLPackage.id.label("package_id"),
                    ETLPackage.created_at,

                    ETLSourceSystem.name.label("source_system"),
                    ETLDataSource.name.label("data_source"),

                    ETLPackage.status,
                    ETLPackage.operation_type,
                    ETLPackage.version_tag,
                    ETLPackage.note,
                    ETLPackage.stats.label("log"),

                    # Extract
                    ETLPackage.extract_status,
                    ETLPackage.extract_start,
                    ETLPackage.extract_end,
                    ETLPackage.extract_rows,
                    ETLPackage.extract_hash,

                    # Transform
                    ETLPackage.transform_status,
                    ETLPackage.transform_start,
                    ETLPackage.transform_end,
                    ETLPackage.transform_rows,
                    ETLPackage.transform_hash,

                    # Load
                    ETLPackage.load_status,
                    ETLPackage.load_start,
                    ETLPackage.load_end,
                    ETLPackage.load_rows,
                    ETLPackage.load_hash,
                )
                .select_from(ETLPackage)
                .join(ETLDataSource, ETLPackage.data_source_id == ETLDataSource.id)
                .join(ETLSourceSystem, ETLDataSource.source_system_id == ETLSourceSystem.id)
            )

            # -------------------
            # Apply filters
            # -------------------
            if only_active:
                stmt = stmt.where(
                    ETLDataSource.active.is_(True),
                    ETLSourceSystem.active.is_(True),
                )

            # case-insensitive filters
            stmt = self._filter_ci(stmt, ETLSourceSystem.name, source_system)
            stmt = self._filter_ci(stmt, ETLDataSource.name, data_sources)

            # Ordering (after filters is fine too)
            stmt = stmt.order_by(ETLPackage.created_at.desc(), ETLPackage.id.desc())

            df = pd.read_sql(stmt, self.session.bind)

            # # Optional: derive simple duration metrics (minutes)
            # for stage in ["extract", "transform", "load"]:
            #     start_col = f"{stage}_start"
            #     end_col = f"{stage}_end"
            #     duration_col = f"{stage}_minutes"

            #     if start_col in df.columns and end_col in df.columns:
            #         df[duration_col] = (
            #             (df[end_col] - df[start_col])
            #             .dt.total_seconds()
            #             .div(60)
            #         )
            # Optional: derive simple duration metrics (minutes)
            now = pd.Timestamp.now()

            for stage in ["extract", "transform", "load"]:
                start_col = f"{stage}_start"
                end_col = f"{stage}_end"
                duration_col = f"{stage}_minutes"

                if start_col in df.columns and end_col in df.columns:

                    # Ensure datetime dtype (safe even if already datetime)
                    df[start_col] = pd.to_datetime(df[start_col], errors="coerce")
                    df[end_col] = pd.to_datetime(df[end_col], errors="coerce")

                    # Use end if present, otherwise "now" ONLY if start exists
                    effective_end = df[end_col].where(
                        df[end_col].notna(),
                        now
                    )

                    df[duration_col] = (
                        (effective_end - df[start_col])
                        .dt.total_seconds()
                        .div(60)
                    )

                    # If start is NULL → duration must be NULL
                    df.loc[df[start_col].isna(), duration_col] = None

            return df

        except Exception as e:
            self.logger.log(f"Error generating ETL package details report: {e}", "ERROR")
            return pd.DataFrame()
