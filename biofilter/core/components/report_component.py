from __future__ import annotations
from biofilter.core.components.base_component import BaseComponent
from biofilter.modules.report.report_manager import ReportManager

class ReportComponent(BaseComponent):
    """
    Facade over ReportManager.

    Usage:
        bf.reports.list()
        bf.reports.run("gene_to_snp", input_data=...)
        bf.reports.explain("gene_to_snp")
    """

    def __init__(self, core):
        super().__init__(core)
        self._manager = None

    def _get_manager(self) -> ReportManager:
        if self._manager is None:
            db = self.core.require_db()

            # session_factory: returns a context manager / session
            # If db.get_session is contextmanager, we can pass it directly.
            self._manager = ReportManager(
                session_factory=db.get_session,
                db=db,
                logger=self.core.logger,
            )
        return self._manager

    # --- Public API (thin wrappers) ---
    def refresh(self) -> None:
        self._get_manager().refresh()

    def list(self, verbose: bool = True):
        return self._get_manager().list(verbose=verbose)

    def run(self, identifier: str, **kwargs):
        return self._get_manager().run(identifier, **kwargs)

    def run_example(self, identifier: str, **kwargs):
        return self._get_manager().run_example(identifier, **kwargs)

    def explain(self, identifier: str, print_output: bool = True):
        return self._get_manager().explain(identifier, print_output=print_output)

    def example_input(self, identifier: str, print_output: bool = True):
        return self._get_manager().example_input(identifier, print_output=print_output)

    def available_columns(self, identifier: str, print_output: bool = True):
        return self._get_manager().available_columns(identifier, print_output=print_output)

    def get_report_class(self, identifier: str):
        return self._get_manager().get_report_class(identifier)
