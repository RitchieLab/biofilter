"""
Discover reports, and run one against a bundle.

Discovery does not touch a bundle. Listing what reports exist, or
reading a report's guide, is a question about the installed package —
the old manager opened a database connection to answer it, which meant
`report list` failed when no database was reachable.
"""

from __future__ import annotations

import importlib
import pkgutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Type

import pyarrow as pa

import biofilter.modules.report.reports as reports_pkg
from biofilter.modules.report.bundle import Bundle
from biofilter.modules.report.reports.base_report import ReportBase
from biofilter.modules.report.result import ReportResult, make_provenance

REPORT_PREFIX = "report_"


@dataclass(frozen=True)
class ReportInfo:
    module: str
    name: str
    description: str
    requires: tuple[str, ...] = ()


class ReportManager:
    """
    Find reports in `biofilter.modules.report.reports` and run them.

    One `ReportBase` subclass per module, module name prefixed
    `report_`. A report is discovered by being there; nothing has to be
    registered.
    """

    def __init__(self, bundle: Optional[Bundle] = None, logger: Any = None) -> None:
        self.bundle = bundle
        self.logger = logger or self._default_logger()
        self._class_cache: dict[str, Type[ReportBase]] = {}
        self._index_cache: Optional[list[ReportInfo]] = None
        self._guides_dir = Path(__file__).resolve().parent / "reports_explain"

    # ------------------------------------------------------------------
    # Discovery — no bundle needed
    # ------------------------------------------------------------------
    def iter_modules(self) -> Iterable[str]:
        for _, module_name, _ in pkgutil.iter_modules(reports_pkg.__path__):
            if module_name.startswith(REPORT_PREFIX):
                yield module_name

    def index(self) -> list[ReportInfo]:
        if self._index_cache is None:
            items = []
            for module_name in self.iter_modules():
                cls = self._load_class(module_name)
                items.append(
                    ReportInfo(
                        module=module_name,
                        name=getattr(cls, "name", module_name),
                        description=getattr(cls, "description", "") or "",
                        requires=tuple(getattr(cls, "requires", ())),
                    )
                )
            items.sort(key=lambda i: i.name.lower())
            self._index_cache = items
        return list(self._index_cache)

    def list_reports(self) -> list[dict[str, Any]]:
        return [
            {
                "module": i.module,
                "name": i.name,
                "description": i.description,
                "requires": list(i.requires),
            }
            for i in self.index()
        ]

    def refresh(self) -> None:
        self._class_cache.clear()
        self._index_cache = None

    def resolve(self, identifier: str) -> str:
        ident = (identifier or "").strip()
        if not ident:
            raise ValueError("Report identifier cannot be empty.")

        index = self.index()
        if ident.startswith(REPORT_PREFIX):
            if any(i.module == ident for i in index):
                return ident
        for info in index:
            if info.name.lower() == ident.lower():
                return info.module
        for info in index:
            if self._load_class(info.module).__name__.lower() == ident.lower():
                return info.module

        raise ValueError(
            f"Report not found: '{identifier}'. Available: "
            f"{[i.name for i in index]}"
        )

    def get_class(self, identifier: str) -> Type[ReportBase]:
        return self._load_class(self.resolve(identifier))

    def explain(self, identifier: str) -> str:
        module_name = self.resolve(identifier)
        guide = self._guides_dir / f"{module_name}.md"
        if guide.is_file():
            return guide.read_text(encoding="utf-8")
        return self._load_class(module_name).explain()

    def available_columns(self, identifier: str):
        return self.get_class(identifier).available_columns()

    def example_input(self, identifier: str):
        return self.get_class(identifier).example_input()

    def _load_class(self, module_name: str) -> Type[ReportBase]:
        if module_name in self._class_cache:
            return self._class_cache[module_name]

        module = importlib.import_module(
            f"{reports_pkg.__name__}.{module_name}"
        )
        # Defined here, not merely imported here. Reports share base
        # classes (see _annotation.py), and an imported base is not a
        # second report in the module.
        candidates = [
            obj
            for attr in dir(module)
            if isinstance(obj := getattr(module, attr), type)
            and issubclass(obj, ReportBase)
            and obj is not ReportBase
            and obj.__module__ == module.__name__
        ]
        if not candidates:
            raise ImportError(f"No ReportBase subclass in '{module_name}'.")
        if len(candidates) > 1:
            names = ", ".join(c.__name__ for c in candidates)
            raise ImportError(
                f"Multiple ReportBase subclasses in '{module_name}': {names}. "
                f"Keep exactly one report class per module."
            )
        self._class_cache[module_name] = candidates[0]
        return candidates[0]

    # ------------------------------------------------------------------
    # Running — bundle required
    # ------------------------------------------------------------------
    def run(self, identifier: str, **params: Any) -> ReportResult:
        bundle = self.bundle
        if bundle is None:
            raise ValueError(
                "This report needs a bundle. Pass --bundle, or construct "
                "ReportManager(bundle=Bundle.open(path))."
            )

        cls = self.get_class(identifier)
        report_name = getattr(cls, "name", identifier)

        required = tuple(getattr(cls, "requires", ()))
        if required:
            bundle.require(*required)

        started = time.perf_counter()
        report = cls(bundle=bundle, logger=self.logger, **params)
        try:
            produced = report.run()
            extra = dict(report.provenance_extra)
        finally:
            report.close()

        if isinstance(produced, ReportResult):
            result = produced
        elif isinstance(produced, pa.Table):
            result = ReportResult(table=produced, artifacts=list(report.artifacts))
        else:
            raise TypeError(
                f"Report '{report_name}' returned {type(produced).__name__}; "
                f"expected a pyarrow.Table or a ReportResult."
            )

        result.provenance = make_provenance(
            report=report_name,
            bundle_id=bundle.bundle_id,
            bundle_root=str(bundle.root),
            params=params,
            biofilter_version=bundle.biofilter_version,
            rows=result.num_rows,
            coverage=self._coverage(cls, bundle),
        )
        # What the report decided, not only what it was told.
        result.provenance.update(extra)

        elapsed = time.perf_counter() - started
        self.logger.log(
            f"Report '{report_name}' produced {result.num_rows:,} rows "
            f"in {elapsed:.2f}s from bundle {bundle.bundle_id}.",
            "INFO",
        )
        return result

    @staticmethod
    def _coverage(cls: Type[ReportBase], bundle: Bundle) -> dict[str, Any]:
        """
        What the bundle did not have, recorded alongside the result.

        A report that declares optional tables works without them, and
        the columns they would have filled come back null — which is
        indistinguishable from a null answer unless someone writes down
        that the source was absent. Same for chromosomes: a bundle built
        for one of them returns honest zeros everywhere else.
        """
        optional = tuple(getattr(cls, "optional", ()))
        absent = [name for name in optional if not bundle.has(name)]

        coverage: dict[str, Any] = {"optional_tables_absent": absent}
        if any(
            name.startswith("variant_")
            for name in tuple(getattr(cls, "requires", ())) + optional
        ):
            chromosomes = bundle.chromosomes()
            if chromosomes is not None:
                coverage["chromosomes"] = chromosomes
        return coverage

    def run_example(self, identifier: str, **params: Any) -> ReportResult:
        cls = self.get_class(identifier)
        params.setdefault("input_data", cls.example_input())
        return self.run(identifier, **params)

    def _default_logger(self):
        from biofilter.utils.logger import Logger

        return Logger()
