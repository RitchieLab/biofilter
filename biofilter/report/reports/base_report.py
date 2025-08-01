from sqlalchemy.orm import Session


class ReportBase:
    name: str = "unnamed_report"
    description: str = "No description provided"

    def __init__(self, session: Session = None, logger=None, **kwargs):
        self.session = session
        # self.logger = logger or self.default_logger()
        self.logger = logger
        self.params = kwargs

    def default_logger(self):
        from biofilter.utils.logger import Logger

        return Logger(name=self.name)

    def run(self):
        raise NotImplementedError("Subclasses must implement `run()`.")
