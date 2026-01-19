from __future__ import annotations

from biofilter.core.components.base_component import BaseComponent
from biofilter.modules.query import SchemaExplorer
from biofilter.core.components.query_component import QueryComponent


class SchemaComponent(BaseComponent):
    """
    Schema explorer entry point (lazy-loaded).
    """

    def __init__(self, core, query_component: QueryComponent):
        super().__init__(core)
        self._query_component = query_component

    def get(self) -> SchemaExplorer:
        if self.core._schema is None:
            self.core._schema = SchemaExplorer(self._query_component.get())
        return self.core._schema
