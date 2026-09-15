"""
Route a report to the module that owns it.

Two report layers are live at once during the ADR-004 migration: the
parquet-native module, and the frozen relational one. A report is
served by the native module when the native module has it, and by the
legacy module otherwise. Which one answered is reported, not hidden —
`report list` shows it, so what is left to migrate stays visible in
normal use rather than requiring someone to read the tree.

The legacy side goes away when it is empty, and so does this routing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from biofilter.core.components.base_component import BaseComponent
from biofilter.modules.report.report_manager import ReportManager as NativeManager
from biofilter.modules.report_legacy.report_manager import ReportManager as LegacyManager  # noqa: E501

NATIVE = "native"
LEGACY = "legacy"

PARQUET_SCHEME = "parquet://"


class ReportComponent(BaseComponent):
    """
    Facade over both report managers.

    Usage:
        bf.report.list()
        bf.report.run("template", input_data=[...])
        bf.report.explain("template")
    """

    def __init__(self, core):
        super().__init__(core)
        self._native: Optional[NativeManager] = None
        self._legacy: Optional[LegacyManager] = None
        self._bundle = None

    # ------------------------------------------------------------------
    # Managers
    # ------------------------------------------------------------------
    def _native_manager(self) -> NativeManager:
        """
        Discovery works with no bundle; running does not.

        The bundle is attached lazily so `list` and `explain` answer
        without one, and `run` fails with a sentence about --bundle
        rather than about a missing connection.
        """
        if self._native is None:
            self._native = NativeManager(logger=self.core.logger)
        if self._native.bundle is None:
            self._native.bundle = self._open_bundle()
        return self._native

    def _legacy_manager(self) -> LegacyManager:
        if self._legacy is None:
            db = self.core.require_db()
            self._legacy = LegacyManager(
                session_factory=db.get_session,
                db=db,
                logger=self.core.logger,
            )
        return self._legacy

    def _bundle_root(self) -> Optional[Path]:
        """The bundle directory, when this session is pointed at one."""
        uri = getattr(self.core, "db_uri", None)
        if isinstance(uri, str) and uri.startswith(PARQUET_SCHEME):
            return Path("/" + uri[len(PARQUET_SCHEME):].lstrip("/")).resolve()

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
    # Routing
    # ------------------------------------------------------------------
    def engine_for(self, identifier: str) -> str:
        """Which module owns this report. Native wins a name collision."""
        try:
            self._native_manager().resolve(identifier)
            return NATIVE
        except ValueError:
            return LEGACY

    def _manager_for(self, identifier: str):
        return (
            self._native_manager()
            if self.engine_for(identifier) == NATIVE
            else self._legacy_manager()
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def list(self, verbose: bool = True) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for row in self._native_manager().list_reports():
            rows.append({**row, "engine": NATIVE})

        native_names = {r["name"] for r in rows}
        try:
            legacy_rows = self._legacy_manager().list_reports()
        except Exception:  # noqa: BLE001 — no database is not a listing error
            legacy_rows = []
        for row in legacy_rows:
            if row.get("name") not in native_names:
                rows.append({**row, "engine": LEGACY})

        rows.sort(key=lambda r: str(r.get("name", "")).lower())
        return rows

    def explain(self, identifier: str):
        return self._manager_for(identifier).explain(identifier)

    def example_input(self, identifier: str):
        return self._manager_for(identifier).example_input(identifier)

    def available_columns(self, identifier: str, print_output: bool = True):
        return self._manager_for(identifier).available_columns(identifier)

    def get_report_class(self, identifier: str):
        return self._manager_for(identifier).get_class(identifier)

    def run(self, identifier: str, **kwargs):
        """
        Run a report.

        Returns a `ReportResult` from the native module and a DataFrame
        from the legacy one. Callers that only want rows can use
        `.to_pandas()` on the former; the CLI keeps the provenance the
        result carries (ADR-004 §2.7).
        """
        return self._manager_for(identifier).run(identifier, **kwargs)

    def run_example(self, identifier: str, **kwargs):
        return self._manager_for(identifier).run_example(identifier, **kwargs)

    def refresh(self) -> None:
        self._native_manager().refresh()
        try:
            self._legacy_manager().refresh()
        except Exception:  # noqa: BLE001
            pass

    def close(self) -> None:
        if self._bundle is not None:
            self._bundle.close()
            self._bundle = None
            if self._native is not None:
                self._native.bundle = None
