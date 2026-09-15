"""
Facade over the report module.

Reports read a bundle. There is no relational path: `biofilter.modules
.report_legacy` is reference material for the reports still to be
rewritten, not code that runs, so nothing here imports it.

What remains of it here is a count — how many reports are still waiting
— read from the directory listing rather than by importing anything.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from biofilter.core.components.base_component import BaseComponent
from biofilter.modules.report.report_manager import ReportManager
from biofilter.utils.bundle_path import PARQUET_URI_SCHEME

#: Where the reports still to be rewritten live. Read as filenames, never
#: imported: several no longer run at all, since they select columns the
#: bundles stopped carrying.
_PENDING_DIR = (
    Path(__file__).resolve().parents[2] / "modules" / "report_legacy" / "reports"
)

_NO_BUNDLE = (
    "Reports read a bundle. Pass --bundle <path> on the CLI, or "
    "Biofilter(bundle='<path>') in Python."
)


class ReportComponent(BaseComponent):
    """
    Usage:
        bf.report.list()
        bf.report.explain("annotation_master_gene")
        bf.report.run("annotation_master_gene", input_data=["TP53"])
    """

    def __init__(self, core):
        super().__init__(core)
        self._manager: Optional[ReportManager] = None
        self._bundle = None

    # ------------------------------------------------------------------
    def _get_manager(self) -> ReportManager:
        """
        Discovery works with no bundle; running does not.

        Listing reports and reading their guides are questions about the
        installed package, so they are answered without opening anything.
        """
        if self._manager is None:
            self._manager = ReportManager(logger=self.core.logger)
        if self._manager.bundle is None:
            self._manager.bundle = self._open_bundle()
        return self._manager

    def _bundle_root(self) -> Optional[Path]:
        uri = getattr(self.core, "db_uri", None)
        if isinstance(uri, str) and uri.startswith(PARQUET_URI_SCHEME):
            return Path("/" + uri[len(PARQUET_URI_SCHEME):].lstrip("/")).resolve()

        db = getattr(self.core, "db", None)
        root = getattr(db, "_bundle_root", None)
        return Path(root) if root else None

    def _open_bundle(self):
        if self._bundle is not None:
            return self._bundle
        root = self._bundle_root()
        if root is None:
            return None
        from biofilter.modules.report.bundle import Bundle

        self._bundle = Bundle.open(root)
        return self._bundle

    # ------------------------------------------------------------------
    # Migration status
    # ------------------------------------------------------------------
    @staticmethod
    def pending_migration() -> list[str]:
        """
        Reports not yet rewritten, by name, from the directory listing.

        No import: the point of keeping them is to read them, and some
        would not import anyway.
        """
        if not _PENDING_DIR.is_dir():
            return []
        return sorted(
            path.stem[len("report_"):]
            for path in _PENDING_DIR.glob("report_*.py")
            if path.stem != "report_template"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def list(self, verbose: bool = True) -> list[dict[str, Any]]:
        return self._get_manager().list_reports()

    def explain(self, identifier: str):
        return self._get_manager().explain(identifier)

    def example_input(self, identifier: str):
        return self._get_manager().example_input(identifier)

    def available_columns(self, identifier: str, print_output: bool = True):
        return self._get_manager().available_columns(identifier)

    def get_report_class(self, identifier: str):
        return self._get_manager().get_class(identifier)

    def run(self, identifier: str, **kwargs):
        """Run a report. Returns a `ReportResult`."""
        manager = self._get_manager()
        if manager.bundle is None:
            raise ValueError(_NO_BUNDLE)
        return manager.run(identifier, **kwargs)

    def run_example(self, identifier: str, **kwargs):
        manager = self._get_manager()
        if manager.bundle is None:
            raise ValueError(_NO_BUNDLE)
        return manager.run_example(identifier, **kwargs)

    def refresh(self) -> None:
        self._get_manager().refresh()

    def close(self) -> None:
        if self._bundle is not None:
            self._bundle.close()
            self._bundle = None
            if self._manager is not None:
                self._manager.bundle = None
