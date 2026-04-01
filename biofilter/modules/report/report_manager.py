from __future__ import annotations

import importlib
import pkgutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Type

from sqlalchemy.orm import Session

import biofilter.modules.report.reports as reports_pkg
from biofilter.modules.db.database import Database
from biofilter.modules.report.reports.base_report import ReportBase
from biofilter.utils.logger import Logger


@dataclass(frozen=True)
class ReportInfo:
    module: str  # e.g. "report_gene_to_snp"
    name: str  # friendly (ReportBase.name)
    description: str


class ReportManager:
    """
    Discover, load, and run reports.

    - Reports live in: biofilter.modules.report.reports
    - Module name must start with: report_
    - Each module defines exactly one ReportBase subclass
    """

    def __init__(
        self, session_factory: Callable[[], Any], db: Database, logger: Logger
    ):
        # session_factory should be db.get_session (contextmanager)
        self._session_factory = session_factory
        self.db = db
        self.logger = logger

        self._class_cache: Dict[str, Type[ReportBase]] = {}
        self._index_cache: Optional[List[ReportInfo]] = None
        self._guides_dir = Path(__file__).resolve().parent / "reports_explain"
        self._legacy_guides_dir = Path(__file__).resolve().parent

    # ----------------------------
    # Discovery
    # ----------------------------
    def iter_modules(self) -> Iterable[str]:
        for _, module_name, _ in pkgutil.iter_modules(reports_pkg.__path__):
            if module_name.startswith("report_"):
                yield module_name

    def refresh(self) -> None:
        self._class_cache.clear()
        self._index_cache = None

    def index(self) -> List[ReportInfo]:
        """Return cached list of reports (module, name, description)."""
        if self._index_cache is None:
            items: List[ReportInfo] = []
            for module_name in self.iter_modules():
                cls = self._load_class(module_name)
                items.append(
                    ReportInfo(
                        module=module_name,
                        name=getattr(cls, "name", module_name),
                        description=getattr(cls, "description", "") or "",
                    )
                )
            items.sort(key=lambda x: x.name.lower())
            self._index_cache = items
        return list(self._index_cache)

    def list_reports(self) -> List[dict]:
        return [
            {"module": i.module, "name": i.name, "description": i.description}
            for i in self.index()
        ]

    # ----------------------------
    # Loading
    # ----------------------------
    def _load_class(self, module_name: str) -> Type[ReportBase]:
        if module_name in self._class_cache:
            return self._class_cache[module_name]

        try:
            module = importlib.import_module(
                f"biofilter.modules.report.reports.{module_name}"
            )
        except Exception as e:
            self.logger.log(
                f"Failed to import report module '{module_name}': {e}", "ERROR"
            )
            raise

        candidates: List[Type[ReportBase]] = []
        for attr in dir(module):
            obj = getattr(module, attr)
            if (
                isinstance(obj, type)
                and issubclass(obj, ReportBase)
                and obj is not ReportBase
            ):
                candidates.append(obj)

        if not candidates:
            raise ImportError(
                f"No ReportBase subclass found in module '{module_name}'."
            )
        if len(candidates) > 1:
            names = ", ".join([c.__name__ for c in candidates])
            raise ImportError(
                f"Multiple ReportBase subclasses found in '{module_name}': {names}. "
                f"Keep exactly one report class per module."
            )

        cls = candidates[0]
        self._class_cache[module_name] = cls
        return cls

    def resolve(self, identifier: str) -> str:
        """
        Resolve identifier to module_name.
        Accepts:
          - module name: report_xxx
          - friendly name: cls.name
          - class name: cls.__name__ (optional convenience)
        """
        ident = (identifier or "").strip()
        if not ident:
            raise ValueError("Report identifier cannot be empty.")

        # Ensure index built
        idx = self.index()
        modules = {info.module for info in idx}

        # Explicit module name path (report_xxx)
        if ident.startswith("report_"):
            if ident in modules:
                return ident
            available = [i.name for i in idx]
            raise ValueError(
                f"Report not found: '{identifier}'. Available reports: {available}"
            )

        # friendly name match
        for info in idx:
            if info.name == ident or info.name.lower() == ident.lower():
                return info.module

        # class name match (extra convenience)
        for info in idx:
            cls = self._load_class(info.module)
            if cls.__name__ == ident or cls.__name__.lower() == ident.lower():
                return info.module

        available = [i.name for i in idx]
        raise ValueError(
            f"Report not found: '{identifier}'. Available reports: {available}"
        )

    def get_class(self, identifier: str) -> Type[ReportBase]:
        return self._load_class(self.resolve(identifier))

    @staticmethod
    def _normalize_report_slug(value: str) -> str:
        slug = str(value or "").strip().lower()
        slug = slug.replace("-", "_").replace(" ", "_")
        while "__" in slug:
            slug = slug.replace("__", "_")
        if slug.startswith("report_"):
            slug = slug[len("report_") :]
        return slug.strip("_")

    def _find_explain_guide(self, module_name: str) -> Optional[Path]:
        direct_path = self._guides_dir / f"{module_name}.md"
        if direct_path.exists():
            return direct_path

        info = next((i for i in self.index() if i.module == module_name), None)
        candidate_values = [module_name]
        if info is not None:
            candidate_values.insert(0, info.name)

        candidate_slugs = []
        for value in candidate_values:
            slug = self._normalize_report_slug(value)
            if slug and slug not in candidate_slugs:
                candidate_slugs.append(slug)

        for slug in candidate_slugs:
            modern_path = self._guides_dir / f"report_{slug}.md"
            if modern_path.exists():
                return modern_path

            legacy_matches = sorted(
                self._legacy_guides_dir.glob(f"ag_*_report_{slug}.md")
            )
            if legacy_matches:
                return legacy_matches[0]
        return None

    def _load_explain_guide(self, module_name: str) -> Optional[str]:
        path = self._find_explain_guide(module_name)
        if path is None:
            return None
        try:
            return path.read_text(encoding="utf-8")
        except Exception as e:
            self.logger.log(
                f"Could not read report guide '{path.name}' for '{module_name}': {e}",
                "WARNING",
            )
            return None

    # ----------------------------
    # Introspection helpers (no session needed)
    # ----------------------------
    def explain(self, identifier: str) -> str:
        module_name = self.resolve(identifier)
        guide_text = self._load_explain_guide(module_name)
        if guide_text:
            return guide_text
        return self._load_class(module_name).explain()

    def example_input(self, identifier: str):
        return self.get_class(identifier).example_input()

    def available_columns(self, identifier: str):
        return self.get_class(identifier).available_columns()

    # ----------------------------
    # Instantiate / Run (session per call)
    # ----------------------------
    def get(self, identifier: str, session: Session, **kwargs) -> ReportBase:
        cls = self.get_class(identifier)
        try:
            return cls(session=session, db=self.db, logger=self.logger, **kwargs)
        except TypeError:
            self.logger.log(
                f"Report '{cls.__name__}' does not accept db=... yet. Falling back to session-only.",
                "WARNING",
            )
            return cls(session=session, logger=self.logger, **kwargs)

    def run(self, identifier: str, **kwargs):
        start_time = time.perf_counter()
        report_name = identifier
        self.logger.log(
            (
                f"Starting report '{identifier}'. Execution may take some time. "
                "If the process is terminated, execution will be interrupted."
            ),
            "INFO",
        )

        with self._session_factory() as session:
            try:
                report = self.get(identifier, session=session, **kwargs)
                report_name = getattr(report, "name", identifier)
                result = report.run()

                elapsed_seconds = time.perf_counter() - start_time
                elapsed_minutes = elapsed_seconds / 60.0
                self.logger.log(
                    (
                        f"Report '{report_name}' completed in {elapsed_minutes:.2f} "
                        f"minutes ({elapsed_seconds:.2f} seconds)."
                    ),
                    "INFO",
                )
                return result
            except KeyboardInterrupt:
                elapsed_seconds = time.perf_counter() - start_time
                elapsed_minutes = elapsed_seconds / 60.0
                self.logger.log(
                    (
                        f"Report '{report_name}' was interrupted after "
                        f"{elapsed_minutes:.2f} minutes."
                    ),
                    "WARNING",
                )
                try:
                    session.rollback()
                except Exception:
                    pass
                raise
            except Exception as e:
                elapsed_seconds = time.perf_counter() - start_time
                elapsed_minutes = elapsed_seconds / 60.0
                self.logger.log(
                    (
                        f"Report '{report_name}' failed after "
                        f"{elapsed_minutes:.2f} minutes: {e}"
                    ),
                    "ERROR",
                )
                try:
                    session.rollback()
                except Exception:
                    pass
                raise
            finally:
                # keep postgres clean (no idle in tx)
                try:
                    session.rollback()
                except Exception:
                    pass

    def run_example(self, identifier: str, **kwargs):
        cls = self.get_class(identifier)
        kwargs.setdefault("input_data", cls.example_input())
        return self.run(identifier, **kwargs)
