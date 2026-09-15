"""
Parquet-native report layer (ADR-004).

Reports read a bundle through DuckDB and return Arrow. There is no
relational path: `biofilter.modules.report_legacy` holds the reports
still to be rewritten, as reference material that nothing imports.
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
