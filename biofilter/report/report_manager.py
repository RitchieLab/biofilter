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

    def _load_report_class(self, name: str):
        module = importlib.import_module(f"biofilter.report.reports.{name}")
        for attr in dir(module):
            obj = getattr(module, attr)
            if isinstance(obj, type) and issubclass(obj, ReportBase) and obj != ReportBase:
                return obj
        raise ImportError(f"No valid report class found in {name}")

    def list_reports(self):
        """List all available report classes under report/reports"""
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
        return reports

    def run_report(self, name: str, as_dataframe: bool = True, **kwargs):
        report_class = self._load_report_class(name)
        report = report_class(session=self.session, logger=self.logger, **kwargs)
        result = report.run()
        # return report.to_dataframe(result) if as_dataframe else result
        return result
