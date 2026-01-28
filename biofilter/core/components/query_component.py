from __future__ import annotations

import pandas as pd
from biofilter.core.components.base_component import BaseComponent
from biofilter.modules.query import Query


class QueryComponent(BaseComponent):
    """
    Query entry point (lazy-loaded).
    """

    def get(self) -> Query:
        db = self.require_db()
        if self.core._query is None:
            self.core._query = Query(db.get_session())
        return self.core._query

    def run_sql(self, sql: str, *, raise_on_error: bool = False) -> pd.DataFrame:
        return self.get().run_sql(sql, raise_on_error=raise_on_error)
