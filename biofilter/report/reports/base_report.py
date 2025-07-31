from sqlalchemy.orm import Session
# import pandas as pd


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

    # def to_dataframe(self, result):
    #     if isinstance(result, pd.DataFrame):
    #         return result
    #     elif hasattr(result, "__iter__"):
    #         return pd.DataFrame([dict(row) for row in result])
    #     else:
    #         return pd.DataFrame([{"result": result}])
