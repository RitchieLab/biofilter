"""
Starting point for a new report. Copy, rename, replace the query.

What this shows, and what every report should do:

- name what it reads in `requires`, so a bundle missing a source fails
  with one sentence instead of a query error
- name what it reads *when present* in `optional`, so the provenance can
  record which sources were absent — a column that is null because the
  source was never built looks exactly like one whose answer is null
- turn the user's input into a relation and JOIN it, never into a list
  of SQL literals
- express the whole question as one statement and let DuckDB plan it,
  rather than issuing a query per input and merging in Python
- return Arrow; the manager attaches provenance and the CLI writes it
"""

from __future__ import annotations

from typing import Sequence

import pyarrow as pa

from biofilter.modules.report.reports.base_report import ReportBase


class TemplateReport(ReportBase):
    name = "template"
    description = "Example report: resolve gene symbols against the bundle."

    requires = ("gene_masters",)

    COLUMNS = ("input_value", "entity_id", "symbol", "found")

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return cls.COLUMNS

    @classmethod
    def example_input(cls):
        return ["TP53", "BRCA1", "NOT_A_GENE"]

    def run(self) -> pa.Table:
        values = self.resolve_input_list(self.param("input_data", required=True))
        self.register_input(values, name="input_genes", column="input_value")

        # One statement. The input is a relation on the left of a LEFT
        # JOIN, so unmatched inputs stay in the result with found=false —
        # a report should say what it could not resolve, not drop it.
        return self.sql(
            """
            SELECT
                i.input_value,
                g.entity_id,
                g.symbol,
                g.entity_id IS NOT NULL AS found
            FROM input_genes i
            LEFT JOIN gene_masters g
                   ON lower(g.symbol) = i.input_value_norm
            ORDER BY i.input_value
            """
        )
