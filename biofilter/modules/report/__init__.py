"""
Parquet-native report layer (ADR-004).

Reports read a bundle through DuckDB and return Arrow. The relational
report layer is frozen at `biofilter.modules.report_legacy` until its
last report has moved here.
"""

from biofilter.modules.report.bundle import (
    Bundle,
    BundleError,
    BundleIncomplete,
    BundleNotFound,
    BundleVersionError,
)
from biofilter.modules.report.report_manager import ReportInfo, ReportManager
from biofilter.modules.report.result import Artifact, ReportResult, make_provenance

__all__ = [
    "Artifact",
    "Bundle",
    "BundleError",
    "BundleIncomplete",
    "BundleNotFound",
    "BundleVersionError",
    "ReportInfo",
    "ReportManager",
    "ReportResult",
    "make_provenance",
]
