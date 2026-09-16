"""
Parquet-native report layer (ADR-004).

Reports read a bundle through DuckDB and return Arrow. There is no
relational path: every report was rewritten here, and the frozen
`report_legacy` module was deleted once the last one moved (§2.11).
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
