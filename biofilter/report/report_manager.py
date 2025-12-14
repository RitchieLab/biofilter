# report_manager.py
import pkgutil
import importlib
from sqlalchemy.orm import Session
from biofilter.utils.logger import Logger
import biofilter.report.reports as reports_pkg
from biofilter.report.reports.base_report import ReportBase


class ReportManager:
    def __init__(self, session: Session, logger: Logger):
        self.session = session
        self.logger = logger
        # self._cache: dict[str, type[ReportBase]] = {}

    def _load_report_class(self, name: str):
        # if module_name in self._cache:
        #     return self._cache[module_name]
        
        module = importlib.import_module(f"biofilter.report.reports.{name}")
        for attr in dir(module):
            obj = getattr(module, attr)
            if (
                isinstance(obj, type)
                and issubclass(obj, ReportBase)
                and obj != ReportBase
            ):
                return obj
        raise ImportError(f"No valid report class found in {name}")
    
    def _resolve_report_module(self, identifier: str) -> str:
        # 1) If already a module name
        if identifier.startswith("report_"):
            return identifier

        # 2) Otherwise, match by report_class.name
        for _, mod_name, _ in pkgutil.iter_modules(reports_pkg.__path__):
            if mod_name.startswith("report_"):
                cls = self._load_report_class(mod_name)
                if cls.name == identifier:
                    return mod_name

        raise ValueError(f"Report not found: {identifier}")

    # ----------------------------------
    # LIST ALL REPORTS
    # ----------------------------------
    def list_reports(self, verbose: bool = True):
        """
        Lists all available reports in the system.

        Parameters:
            verbose (bool): If True, prints a friendly formatted table.

        Returns:
            List[Dict]: List of report metadata with name and description.
        """
        reports = []
        for _, name, _ in pkgutil.iter_modules(reports_pkg.__path__):
            if name.startswith("report_"):
                report_class = self._load_report_class(name)
                reports.append(
                    {
                        "name": report_class.name,
                        "description": getattr(report_class, "description", ""),
                    }
                )
        if verbose:
            print("\n📄 Available Reports:")
            print("=====================\n")
            n = 0
            for r in reports:
                n += 1
                print(f"{n}. {r['name']}: ")
                print(f"   {r['description']}\n")
                # print(f" • {r['name']:<20} → {r['description']}")
            # print()
            # return True
            return reports

        return reports

    # ----------------------------------
    # RUN ONE REPORT
    # ----------------------------------
    def run_report(self, name: str, **kwargs):

        module_name = self._resolve_report_module(name)
        report_class = self._load_report_class(module_name)
        # report_class = self._load_report_class(name)

        report = report_class(session=self.session, logger=self.logger, **kwargs)
        result_df = report.run()
        return result_df

    def explain(self, name: str, **kwargs):
        report_class = self._load_report_class(name)
        report = report_class(session=self.session, logger=self.logger, **kwargs)
        result = report.explain()
        print(result)

    def run_example_report(self, name: str, **kwargs):
        report_class = self._load_report_class(name)
        kwargs.setdefault("input_data", report_class.example_input())  # Use Example
        report = report_class(session=self.session, logger=self.logger, **kwargs)
        result_df = report.run()
        return result_df
