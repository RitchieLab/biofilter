"""
What a report returns.

A report hands back a table plus the provenance needed to know which
bundle produced it, and — when it has any — the extra files it wrote
along the way. `artifacts` is empty for every report today; it is
declared now so the day a report needs to emit rejected rows or an
inconsistency log, the contract does not have to change under the
reports already migrated (ADR-004 §2.6).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

#: Suffix of the file written next to a CSV export. CSV has nowhere
#: honest to put metadata, so provenance travels beside it.
PROVENANCE_SUFFIX = ".provenance.json"


@dataclass(frozen=True)
class Artifact:
    """A file a report produced that is not the result itself."""

    name: str
    path: Path
    kind: str = "log"
    description: str = ""


def make_provenance(
    *,
    report: str,
    bundle_id: Optional[str],
    bundle_root: Optional[str] = None,
    params: Optional[dict[str, Any]] = None,
    biofilter_version: Optional[str] = None,
    rows: Optional[int] = None,
    coverage: Optional[dict[str, Any]] = None,
    version_mismatch: Optional[str] = None,
) -> dict[str, Any]:
    """
    Build the provenance record that travels with a result.

    `bundle_id` is the load-bearing field: entity and variant ids are
    scoped to one bundle (ADR-003 §2.5), so a result carrying them is
    only interpretable next to the build that produced them. It is a
    build identity, not a content hash — it answers "which build", not
    "has this been altered".

    `coverage` records what the bundle was missing: optional tables the
    report would have used, and which chromosomes its variants span. Both
    show up in the result as nulls and absences that look like answers.

    `version_mismatch` is set when the bundle was built by a different
    Biofilter release than the one that read it. The read is allowed; the
    record is what lets the result be recognised later as one produced
    across a version boundary.
    """
    return {
        "report": report,
        "bundle_id": bundle_id,
        "bundle_root": bundle_root,
        "biofilter_version": biofilter_version,
        "params": _jsonable(params or {}),
        "rows": rows,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        # What the bundle did not have. A column that is null because the
        # source was never built looks exactly like one that is null
        # because the answer is null, and only this tells them apart.
        "coverage": coverage or {},
        # None when the bundle and this install are the same release.
        "version_mismatch": version_mismatch,
    }


@dataclass
class ReportResult:
    """The result of one report execution."""

    table: pa.Table
    provenance: dict[str, Any] = field(default_factory=dict)
    artifacts: list[Artifact] = field(default_factory=list)

    @property
    def num_rows(self) -> int:
        return self.table.num_rows

    @property
    def columns(self) -> list[str]:
        return list(self.table.column_names)

    def to_pandas(self):
        """
        A DataFrame, for notebooks, with provenance on `.attrs`.

        The attribute survives in memory and through a notebook session.
        It does not survive a CSV round-trip, which is why `write()`
        puts provenance in a file of its own.
        """
        df = self.table.to_pandas()
        try:
            df.attrs.update(self.provenance)
        except Exception:  # noqa: BLE001 — never fail a result over metadata
            pass
        return df

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------
    def write(
        self,
        path: str | Path,
        *,
        fmt: Optional[str] = None,
        provenance_sidecar: bool = True,
    ) -> list[Path]:
        """
        Write the result, and the provenance that explains it.

        CSV is the default because it is what gets opened. Parquet is
        available for results big enough to want it, and carries the
        provenance in its own key-value metadata as well — but the
        sidecar is written either way, so there is one place to look
        regardless of format.
        """
        path = Path(path).expanduser()
        fmt = (fmt or _infer_format(path)).lower()
        path.parent.mkdir(parents=True, exist_ok=True)

        if fmt == "parquet":
            pq.write_table(self._table_with_metadata(), path)
        elif fmt == "csv":
            pacsv.write_csv(flatten_for_csv(self.table), path)
        else:
            raise ValueError(f"Unsupported output format: {fmt!r} (csv, parquet)")

        written = [path]
        if provenance_sidecar:
            written.append(self.write_provenance(path))
        return written

    def write_provenance(self, result_path: str | Path) -> Path:
        side = Path(str(result_path) + PROVENANCE_SUFFIX)
        payload = dict(self.provenance)
        payload["result_file"] = Path(result_path).name
        if self.artifacts:
            payload["artifacts"] = [
                {
                    "name": a.name,
                    "file": a.path.name,
                    "kind": a.kind,
                    "description": a.description,
                }
                for a in self.artifacts
            ]
        side.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        return side

    def _table_with_metadata(self) -> pa.Table:
        """Provenance as parquet key-value metadata, one JSON blob."""
        existing = self.table.schema.metadata or {}
        merged = dict(existing)
        merged[b"biofilter_provenance"] = json.dumps(self.provenance).encode()
        return self.table.replace_schema_metadata(merged)


def flatten_for_csv(table: pa.Table) -> pa.Table:
    """
    Render nested columns as JSON so CSV can hold them.

    A gene's groups and its relationship counts per related group are
    genuinely lists, and Arrow and parquet keep them that way. CSV has no
    nested type at all — pyarrow refuses to write one — so the choice is
    how to spell a list in one cell.

    JSON, because it is unambiguous and reversible: `["A","B"]` reads as
    a list to a person and parses back for a program. A separator-joined
    string does neither once a value contains the separator.
    """
    columns = []
    changed = False
    for column in table.columns:
        if pa.types.is_nested(column.type):
            rendered = [
                None if v is None else json.dumps(v, separators=(",", ":"))
                for v in column.to_pylist()
            ]
            columns.append(pa.array(rendered, type=pa.string()))
            changed = True
        else:
            columns.append(column)
    if not changed:
        return table
    return pa.table(columns, names=table.column_names)


def _infer_format(path: Path) -> str:
    return "parquet" if path.suffix.lower() in {".parquet", ".pq"} else "csv"


def _jsonable(value: Any) -> Any:
    """Params come from a CLI or a notebook; keep only what serialises."""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Path):
        return str(value)
    return repr(value)
