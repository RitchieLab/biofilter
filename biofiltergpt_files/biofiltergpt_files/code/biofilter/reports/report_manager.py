# biofilter/reports/report_manager.py

import pkgutil
import importlib
from sqlalchemy.orm import Session
from biofilter.utils.logger import Logger
from biofilter.reports import queries
from biofilter.reports.base import QryBase


class ReportManager:
    def __init__(self, session: Session):
        self.session = session
        self.logger = Logger()

    def list_reports(self):
        reports = []
        for _, name, _ in pkgutil.iter_modules(queries.__path__):
            if name.startswith("qry_"):
                report_class = self.load_report_class(name)
                reports.append(
                    {
                        "name": report_class.name,
                        "description": getattr(report_class, "description", ""),
                    }
                )
        return reports

    def load_report_class(self, name: str):
        module = importlib.import_module(f"biofilter.reports.queries.{name}")
        for attr in dir(module):
            obj = getattr(module, attr)
            if isinstance(obj, type) and issubclass(obj, QryBase) and obj != QryBase:
                return obj
        raise ImportError(f"No valid report class found in {name}")

    def run_report(self, name: str, as_dataframe: bool = True, **kwargs):
        report_class = self.load_report_class(name)
        # logger = Logger(name=name)
        report = report_class(session=self.session, logger=self.session, **kwargs)
        result = report.run()
        return result
        # return report.to_dataframe(result) if as_dataframe else result


"""
from biofilter import Biofilter

bf = Biofilter("sqlite:///biofilter.db")
print(bf.list_reports())  # retorna nome e descrição
df = bf.run_report("qry_template")
"""
