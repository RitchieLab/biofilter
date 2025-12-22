# report_manager.py
from __future__ import annotations

import importlib
import pkgutil
from dataclasses import dataclass
from typing import Dict, List, Optional, Type, Any

from sqlalchemy.orm import Session

import biofilter.report.reports as reports_pkg
from biofilter.report.reports.base_report import ReportBase
from biofilter.utils.logger import Logger


@dataclass(frozen=True)
class ReportInfo:
    module: str
    name: str
    description: str


class ReportManager:
    """
    Discovers, loads, lists, and runs Biofilter3R reports.

    Reports must live in `biofilter.report.reports` and their module must start
    with `report_`. Each module must expose exactly one subclass of ReportBase.
    """

    def __init__(self, session: Session, logger: Logger):
        self.session = session
        self.logger = logger
        self._class_cache: Dict[str, Type[ReportBase]] = {}
        self._index_cache: Optional[List[ReportInfo]] = None

    # ----------------------------
    # Discovery / Indexing
    # ----------------------------
    def _iter_report_modules(self):
        for _, module_name, _ in pkgutil.iter_modules(reports_pkg.__path__):
            if module_name.startswith("report_"):
                yield module_name

    def _build_index(self) -> List[ReportInfo]:
        """
        Build a cached index of available reports:
        - module name
        - report friendly name (ReportBase.name)
        - description (ReportBase.description)
        """
        items: List[ReportInfo] = []
        for module_name in self._iter_report_modules():
            cls = self._load_report_class(module_name)
            items.append(
                ReportInfo(
                    module=module_name,
                    name=getattr(cls, "name", module_name),
                    description=getattr(cls, "description", ""),
                )
            )
        # Sort by friendly name for nicer output
        items.sort(key=lambda x: x.name.lower())
        return items

    def refresh(self) -> None:
        """
        Clears caches and forces re-discovery of reports.
        Useful during development / notebooks.
        """
        self._class_cache.clear()
        self._index_cache = None

    # ----------------------------
    # Loading
    # ----------------------------
    def _load_report_class(self, module_name: str) -> Type[ReportBase]:
        """
        Import module and find the ReportBase subclass.
        Uses cache for performance.
        """
        if module_name in self._class_cache:
            return self._class_cache[module_name]

        try:
            module = importlib.import_module(f"biofilter.report.reports.{module_name}")
        except Exception as e:
            self.logger.log(f"Failed to import report module '{module_name}': {e}", "ERROR")
            raise

        report_classes: List[Type[ReportBase]] = []
        for attr in dir(module):
            obj = getattr(module, attr)
            if isinstance(obj, type) and issubclass(obj, ReportBase) and obj is not ReportBase:
                report_classes.append(obj)

        if not report_classes:
            raise ImportError(f"No ReportBase subclass found in module '{module_name}'.")

        if len(report_classes) > 1:
            names = ", ".join([c.__name__ for c in report_classes])
            raise ImportError(
                f"Multiple ReportBase subclasses found in module '{module_name}': {names}. "
                f"Keep exactly one report class per module."
            )

        cls = report_classes[0]
        self._class_cache[module_name] = cls
        return cls

    def _resolve_module_name(self, identifier: str) -> str:
        """
        Resolve an identifier to a module name.
        Accepts:
          - module name: 'report_gene_disease_links'
          - friendly name: ReportBase.name (e.g., 'GeneDiseaseLinks')
        """
        # If it already looks like a module, accept it
        if identifier.startswith("report_"):
            return identifier

        # Build index if needed
        if self._index_cache is None:
            self._index_cache = self._build_index()

        # Match by friendly name (exact match)
        for info in self._index_cache:
            if info.name == identifier:
                return info.module

        # Try case-insensitive match
        for info in self._index_cache:
            if info.name.lower() == identifier.lower():
                return info.module

        available = [i.name for i in self._index_cache] if self._index_cache else []
        raise ValueError(f"Report not found: '{identifier}'. Available reports: {available}")

    # ----------------------------
    # Public API
    # ----------------------------
    def list(self, verbose: bool = True) -> List[dict]:
        """
        List all available reports.

        Returns a list of dicts with:
        - module
        - name
        - description
        """
        if self._index_cache is None:
            self._index_cache = self._build_index()

        rows = [
            {"module": i.module, "name": i.name, "description": i.description}
            for i in self._index_cache
        ]

        if verbose:
            print("\n📄 Available Reports")
            print("====================\n")
            for n, r in enumerate(rows, start=1):
                print(f"{n}. {r['name']}")
                if r["description"]:
                    print(f"   {r['description']}")
                print(f"   module: {r['module']}\n")
            return None

        return rows

    def get_report_class(self, identifier: str) -> Type[ReportBase]:
        """
        Return the report class for a given identifier (module name or friendly name).
        """
        module_name = self._resolve_module_name(identifier)
        return self._load_report_class(module_name)

    def get_report(self, identifier: str, **kwargs) -> ReportBase:
        """
        Instantiate and return a report object.
        """
        cls = self.get_report_class(identifier)
        return cls(session=self.session, logger=self.logger, **kwargs)

    def run(self, identifier: str, **kwargs):
        """
        Run a report and return the resulting DataFrame.
        """
        try:
            report = self.get_report(identifier, **kwargs)
            return report.run()
        # except Exception as e:
        #     self.logger.log(f"Report '{identifier}' failed: {e}", "ERROR")
        #     raise
        # 3.2.0: Avoit idle in transation in Postgres
        except Exception as e:
            self.logger.log(f"Report '{identifier}' failed: {e}", "ERROR")
            # Ensure we don't keep an aborted transaction open
            try:
                self.session.rollback()
            except Exception:
                pass
            raise
        finally:
            # Ensure no "idle in transaction" after SELECTs
            try:
                self.session.rollback()
            except Exception:
                pass


    def explain(self, identifier: str, print_output: bool = True) -> str:
        """
        Return (and optionally print) the report explanation.
        """
        cls = self.get_report_class(identifier)
        text = cls.explain()
        if print_output:
            print(text)
        else:
            return text
        
    def example_input(self, identifier: str, print_output: bool = True) -> str:
        """
        Return 
        """
        cls = self.get_report_class(identifier)
        text = cls.example_input()
        if print_output:
            print(text)
        else:
            return text
    
    def available_columns(self, identifier: str, print_output: bool = True) -> str:
        """
        Return 
        """
        cls = self.get_report_class(identifier)
        text = cls.available_columns()
        if print_output:
            print(text)
        else:
            return text

    def run_example(self, identifier: str, **kwargs):
        """
        Run a report using its example_input() as input_data (if not provided).
        """
        cls = self.get_report_class(identifier)
        kwargs.setdefault("input_data", cls.example_input())
        return self.run(identifier, **kwargs)
