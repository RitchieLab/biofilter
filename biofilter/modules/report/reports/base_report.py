"""
The contract a parquet-native report implements.

A report writes SQL and returns a table. It does not assemble rows in
Python, and it does not interpolate user input into a query — input
arrives as a registered relation and is joined (ADR-004 §2.5), which is
both what keeps injection out and what turns a ten-thousand-value filter
into a hash join instead of a literal list.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import duckdb
import pyarrow as pa

from biofilter.modules.report.bundle import Bundle
from biofilter.modules.report.result import Artifact, ReportResult


#: What DuckDB says when a name genuinely is not in the data, as opposed
#: to the many other things a binder error can mean.
_MISSING_PHRASES = (
    "does not have a column named",
    "not found in from clause",
    "table with name",
    "does not exist",
)


def _is_missing_from_bundle(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(phrase in message for phrase in _MISSING_PHRASES)


class BundleSchemaMismatch(RuntimeError):
    """The bundle does not carry something the report asked for."""

    def _render_traceback_(self) -> list[str]:
        """IPython prints the message; the frames add nothing."""
        return [f"BundleSchemaMismatch: {self}"]


def _jsonable_context(context: dict[str, Any]) -> dict[str, Any]:
    """Keep a warning's context printable: it ends up in JSON."""
    out: dict[str, Any] = {}
    for key, value in context.items():
        if isinstance(value, (str, int, float, bool, type(None))):
            out[key] = value
        elif isinstance(value, (list, tuple, set)):
            out[key] = [str(v) for v in value]
        else:
            out[key] = str(value)
    return out


class ReportBase:
    #: Friendly name; how the report is asked for on the CLI.
    name: str = "unnamed_report"
    description: str = "No description provided"

    #: Bundle tables this report reads. Checked before `run()`, so a
    #: bundle built without a source says so in one line rather than
    #: failing somewhere inside the third query.
    requires: tuple[str, ...] = ()

    #: Tables the report uses when the bundle has them, and does without
    #: when it does not. Their absence is not an error — it is a silent
    #: hole in the result, so the manager records which ones were missing
    #: in the provenance rather than leaving a reader to wonder why a
    #: column is all null.
    optional: tuple[str, ...] = ()

    def __init__(
        self,
        bundle: Bundle,
        logger: Any = None,
        **params: Any,
    ) -> None:
        if bundle is None:
            raise ValueError("A report needs a Bundle (bundle=...).")
        self.bundle = bundle
        self.params = params
        self.logger = logger or self._default_logger()
        self.artifacts: list[Artifact] = []
        #: Decisions the report made that the parameters do not show —
        #: a default that was taken, a mode that was chosen. Merged into
        #: the result's provenance, because "which of two mechanisms
        #: produced this" is not recoverable from the rows.
        self.provenance_extra: dict[str, Any] = {}
        #: Things that went wrong or nearly did, in order. A report that
        #: silently copes with a problem leaves nothing behind; this is
        #: where coping gets written down, and it travels in the result's
        #: provenance rather than only into a log file nobody kept.
        self.run_warnings: list[dict[str, Any]] = []
        #: Tables beyond the main one, by name (ADR-004 §2.6).
        self.extra_tables: dict[str, pa.Table] = {}
        # One cursor per execution. Views are shared; temp tables and
        # registered relations are not, so two reports in one process
        # cannot collide.
        self.con = bundle.cursor()

    # ------------------------------------------------------------------
    # To implement
    # ------------------------------------------------------------------
    def run(self) -> pa.Table | ReportResult:
        """
        Produce the result.

        Return an Arrow table; the manager wraps it with provenance.
        Return a `ReportResult` instead only when the report has
        artifacts of its own to attach.
        """
        raise NotImplementedError("Subclasses must implement run().")

    @classmethod
    def available_columns(cls) -> Sequence[str]:
        return ()

    @classmethod
    def example_input(cls) -> Any:
        return None

    @classmethod
    def explain(cls) -> str:
        return "No explanation provided."

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------
    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> pa.Table:
        """
        Run a query on this report's cursor and return Arrow.

        `params` are bound by DuckDB, never formatted into the string.
        For a list of values use `register_input()` and join against it —
        a scalar placeholder is for a scalar.
        """
        try:
            cur = self.con.execute(query, list(params) if params else None)
        except duckdb.BinderException as exc:
            if not _is_missing_from_bundle(exc):
                # Not every binder error is the bundle's fault. A bad
                # ORDER BY or an ambiguous name is the report's own bug,
                # and blaming the data sends the reader somewhere useless.
                raise
            raise BundleSchemaMismatch(self._schema_message(exc)) from exc
        return cur.to_arrow_table()

    def _schema_message(self, exc: Exception) -> str:
        """
        Say which bundle is missing what, instead of a bare binder error.

        A report asking for a column an older bundle never carried fails
        with `Binder Error: ... does not have a column named X`, which
        names neither the report nor the bundle. The usual cause is a
        bundle built before the column existed, and that is worth saying
        outright rather than leaving to be deduced.
        """
        return (
            f"Report '{self.name}' asked this bundle for something it does not "
            f"carry.\n"
            f"  bundle: {self.bundle.root}\n"
            f"  built:  {self.bundle.created_at} by Biofilter "
            f"{self.bundle.biofilter_version}\n"
            f"  {exc}\n"
            f"  A bundle built before a column existed will fail here. "
            f"`biofilter db verify --in <bundle> --schema` compares it against "
            f"this install."
        )

    def stream(self, query: str, params: Optional[Sequence[Any]] = None):
        """
        The same query as a streaming `RecordBatchReader`.

        For results too large to hold: the caller consumes batches and
        never materialises the whole table.
        """
        return self.con.execute(query, list(params) if params else None).fetch_record_batch()

    def register(self, name: str, table: pa.Table) -> str:
        """Expose an Arrow table to SQL on this cursor only."""
        self.con.register(name, table)
        return name

    def register_input(
        self,
        values: Iterable[Any],
        *,
        name: str = "input_values",
        column: str = "value",
        normalise: bool = True,
    ) -> str:
        """
        Register the user's input list as a relation to join against.

        This is the single most important helper here. It is what keeps
        a 700,000-value input a hash join rather than a SQL literal the
        size of a phone book, and it is why no user value is ever
        concatenated into a query.

        `normalise` adds a lowercased, trimmed column (`<column>_norm`)
        so a case-insensitive match is a plain equality join — a
        `lower()` wrapped around a bundle column defeats the parquet
        statistics and turns a 97 ms scan into 535 ms.
        """
        cleaned = [str(v).strip() for v in values if v is not None and str(v).strip()]
        arrays: dict[str, pa.Array] = {column: pa.array(cleaned, type=pa.string())}
        if normalise:
            arrays[f"{column}_norm"] = pa.array(
                [v.lower() for v in cleaned], type=pa.string()
            )
        self.con.register(name, pa.table(arrays))
        return name

    # ------------------------------------------------------------------
    # Parameters and input
    # ------------------------------------------------------------------
    def param(self, key: str, default: Any = None, required: bool = False) -> Any:
        if key in self.params:
            return self.params[key]
        if required:
            raise ValueError(f"Missing required parameter: '{key}'")
        return default

    def resolve_input_list(
        self, input_data: Any, param_name: str = "input_data"
    ) -> list[str]:
        """
        Turn `--input` or `--input-file` into a list of strings.

        Accepts a list, or a path to a file with one value per line.
        """
        if isinstance(input_data, (list, tuple, set)):
            return [str(v).strip() for v in input_data if str(v).strip()]

        if isinstance(input_data, (str, Path)):
            path = Path(str(input_data)).expanduser()
            if path.is_file():
                with path.open(encoding="utf-8") as fh:
                    return [line.strip() for line in fh if line.strip()]
            # A bare string is one value, not a missing file.
            text = str(input_data).strip()
            if text:
                return [text]

        raise ValueError(
            f"{param_name} must be a list of values or a path to a text file."
        )

    def warn(self, message: str, **context: Any) -> None:
        """
        Record something the reader needs to know, and say it out loud.

        Both, deliberately. The log reaches whoever is watching the run;
        the provenance reaches whoever opens the result next month, who
        is usually a different person and never has the log.
        """
        entry = {"message": message, **_jsonable_context(context)}
        self.run_warnings.append(entry)
        self.logger.log(f"⚠️  {self.name}: {message}", "WARNING")

    def emit(self, name: str, table: pa.Table) -> None:
        """
        Attach a second table to the result.

        For an answer that is genuinely two shapes — the rows, and what
        was rejected; the bins, and what went into them. Writing the
        second one out as a CSV instead makes it a file the result only
        names, which is how it stops being checked.
        """
        if name == "result":
            raise ValueError(
                "'result' is the main table's name. Return the main table "
                "from run(); emit() is for the ones beside it."
            )
        self.extra_tables[name] = table

    def chromosome_filter(
        self, source: str, column: str = "chromosome", *, alias: str = ""
    ) -> str:
        """
        A WHERE clause restricting a bundle table to the chromosomes
        `source` actually touches, or empty when it touches none.

        The variant tables are partitioned by chromosome, so naming them
        lets a scan skip whole files. Without it a two-variant question
        reads every chromosome the bundle has, and what that costs grows
        with the bundle rather than with the question — which is how a
        report that was instant on one chromosome takes a minute on six.

        `source` is a registered relation or a table already narrowed to
        the input; the chromosomes are read from it now and written into
        the SQL as literals, so the optimiser can prune on them.
        """
        try:
            rows = self.con.execute(
                f"SELECT DISTINCT {column} FROM {source} "
                f"WHERE {column} IS NOT NULL ORDER BY 1"
            ).fetchall()
        except duckdb.Error:
            return ""
        present = [int(r[0]) for r in rows if r[0] is not None]
        if not present:
            return ""
        prefix = f"{alias}." if alias else ""
        return f"WHERE {prefix}{column} IN ({', '.join(str(c) for c in present)})"

    def note_provenance(self, key: str, value: Any) -> None:
        """
        Record something about how this result was produced.

        Parameters are already recorded, but only as passed — a default
        taken silently leaves no trace, and for a report with two
        mutually exclusive mechanisms that is the one thing a reader
        most needs.
        """
        self.provenance_extra[key] = value

    def add_artifact(
        self, name: str, path: str | Path, kind: str = "log", description: str = ""
    ) -> Artifact:
        """Record an extra file this run produced (ADR-004 §2.6)."""
        artifact = Artifact(
            name=name, path=Path(path), kind=kind, description=description
        )
        self.artifacts.append(artifact)
        return artifact

    # ------------------------------------------------------------------
    def _default_logger(self):
        from biofilter.utils.logger import Logger

        return Logger()

    def close(self) -> None:
        try:
            self.con.close()
        except Exception:  # noqa: BLE001
            pass
