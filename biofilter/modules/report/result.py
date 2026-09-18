"""
What a report returns.

A report hands back its main table, any further tables it produced, the
provenance needed to know which bundle made them, and the extra files it
wrote along the way.

Two verbs, because they answer different questions:

- `write()` **exports**. CSV for reading elsewhere, parquet for size.
  It flattens nested columns so a spreadsheet can hold them, which is
  lossy on purpose.
- `save()` / `load()` **round-trip**. A directory holding every table as
  parquet plus a manifest, losing nothing. It is written in the same
  shape as a bundle, so a saved result can also be opened with
  `Bundle.open` and queried in DuckDB — which is most of what reusing
  one means.
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

#: What the main table is called inside a saved result. Named rather than
#: positional so the directory reads the same whether a report produced
#: one table or five.
PRIMARY_TABLE = "result"

#: The manifest a saved result declares. Deliberately the same version a
#: bundle declares: a saved result is shaped like one, and `Bundle.open`
#: reads it without knowing the difference.
RESULT_MANIFEST_VERSION = 2
RESULT_MANIFEST = "manifest.json"


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
    #: Further tables, by name. A report whose answer is genuinely two
    #: shapes says so here rather than flattening them into one wide
    #: table or writing the second one out as a file.
    extra_tables: dict[str, pa.Table] = field(default_factory=dict)

    @property
    def tables(self) -> dict[str, pa.Table]:
        """Every table this result carries, main one first."""
        return {PRIMARY_TABLE: self.table, **self.extra_tables}

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

    # ------------------------------------------------------------------
    # Round-trip
    # ------------------------------------------------------------------
    def save(self, directory: str | Path, *, overwrite: bool = False) -> Path:
        """
        Write every table and the provenance, losing nothing.

        A directory, not a file, because a result can carry more than one
        table and because its artifacts are files that belong beside it.
        The layout is a bundle's — `manifest.json` and `tables/` — so the
        same reader opens both and a saved result can be queried rather
        than only reloaded.
        """
        root = Path(directory).expanduser()
        if root.exists() and any(root.iterdir()) and not overwrite:
            raise FileExistsError(
                f"{root} already holds something. Pass overwrite=True to "
                f"replace it, or choose a directory of its own — a result "
                f"written over another leaves the manifest describing files "
                f"that are no longer all there."
            )
        tables_dir = root / "tables"
        tables_dir.mkdir(parents=True, exist_ok=True)

        entries = []
        for name, table in self.tables.items():
            target = tables_dir / f"{name}.parquet"
            pq.write_table(table, target)
            entries.append(
                {
                    "name": name,
                    "table": name,
                    "file": str(target.relative_to(root)),
                    "rows": table.num_rows,
                    "bytes": target.stat().st_size,
                    "branch": "result",
                }
            )

        manifest = {
            "manifest_version": RESULT_MANIFEST_VERSION,
            "kind": "report_result",
            "biofilter_version": self.provenance.get("biofilter_version"),
            "created_at": datetime.now(timezone.utc).isoformat(),
            # The bundle this came from, not an identity of its own: the
            # ids in these rows mean nothing except next to that build.
            "bundle_id": self.provenance.get("bundle_id"),
            "primary_table": PRIMARY_TABLE,
            "provenance": _jsonable(self.provenance),
            "tables": entries,
            "artifacts": [
                {
                    "name": a.name,
                    "file": a.path.name,
                    "kind": a.kind,
                    "description": a.description,
                }
                for a in self.artifacts
            ],
        }
        (root / RESULT_MANIFEST).write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
        return root

    @classmethod
    def load(cls, directory: str | Path) -> "ReportResult":
        """
        Read back a result `save()` wrote.

        The result is self-contained: it does not need the bundle that
        produced it, and does not go looking. Whether that bundle is
        still on disk is recorded under `provenance["source_bundle"]`,
        because a result outliving its bundle is the normal case and the
        reason for saving one at all.
        """
        root = Path(directory).expanduser()
        manifest_path = root / RESULT_MANIFEST
        if not manifest_path.is_file():
            raise FileNotFoundError(
                f"{root} is not a saved result: no {RESULT_MANIFEST}. "
                f"`ReportResult.save()` writes a directory, not a file."
            )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        primary_name = manifest.get("primary_table", PRIMARY_TABLE)
        tables: dict[str, pa.Table] = {}
        for entry in manifest.get("tables") or []:
            path = root / entry["file"]
            if not path.is_file():
                raise FileNotFoundError(
                    f"{manifest_path} declares {entry['file']}, which is not "
                    f"there. A saved result is the directory, not the "
                    f"manifest alone."
                )
            tables[entry.get("table") or entry["name"]] = pq.read_table(path)

        if primary_name not in tables:
            raise ValueError(
                f"{manifest_path} names {primary_name!r} as the main table "
                f"and does not declare it. Tables present: {sorted(tables)}."
            )

        provenance = dict(manifest.get("provenance") or {})
        provenance["source_bundle"] = cls._source_bundle_status(provenance)

        return cls(
            table=tables.pop(primary_name),
            provenance=provenance,
            artifacts=[
                Artifact(
                    name=a["name"],
                    path=root / a["file"],
                    kind=a.get("kind", "log"),
                    description=a.get("description", ""),
                )
                for a in (manifest.get("artifacts") or [])
            ],
            extra_tables=tables,
        )

    @staticmethod
    def _source_bundle_status(provenance: dict[str, Any]) -> dict[str, Any]:
        """
        Whether the bundle behind these rows is still where it was.

        Not a check that has to pass — the rows are valid either way, and
        `bundle_id` names the build whether or not it is still on disk.
        It is here so a reader asking "can I go back to the source" gets
        an answer instead of finding out by opening a missing path.
        """
        root = provenance.get("bundle_root")
        present = bool(root) and (Path(root) / "manifest.json").is_file()
        return {
            "bundle_id": provenance.get("bundle_id"),
            "bundle_root": root,
            "still_present": present,
            "means": (
                "The bundle that produced this is where it was, so the ids "
                "in these rows can be looked up again."
                if present
                else "The bundle that produced this is not at that path any "
                "more. The rows are unchanged and still belong to the build "
                "`bundle_id` names; what cannot be done without it is "
                "resolving those ids to anything else."
            ),
        }

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
