"""
Open a parquet bundle for reading.

A bundle is a directory, not a database, and this module treats it as
one: there is no engine, no session and no connection pool — just a
DuckDB connection with one view per logical table, built from what
`manifest.json` declares.

Reading the manifest rather than scanning the directory is the whole
point (ADR-004 §2.3). The manifest already states which files make up
`variant_masters`; inferring it from filenames is what produced the
defect where an empty parent file shadowed 177 million rows, twice, in
two independently written code paths.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

import duckdb

#: Manifest layouts this release can read. A bundle declaring anything
#: else is refused rather than guessed at (ADR-004 §2.9).
SUPPORTED_MANIFEST_VERSIONS = frozenset({2})

MANIFEST_FILENAME = "manifest.json"
BUILD_RECORD_FILENAME = "build_record.json"


class BundleError(Exception):
    """Base for every reason a bundle cannot be opened."""


class BundleNotFound(BundleError):
    """The path is not a bundle: no manifest, or nothing there."""


class BundleVersionError(BundleError):
    """
    The bundle is newer than this install understands.

    Upgrading Biofilter is a package install; migrating data is not
    something a reader should attempt. So this fails and says so.
    """


class BundleIncomplete(BundleError):
    """A file the manifest declares is missing, or is the wrong size."""


@dataclass(frozen=True)
class BundleTable:
    """One logical table, and the files that make it up."""

    name: str
    files: tuple[Path, ...]
    rows: int
    branch: str = "core"

    @property
    def partitioned(self) -> bool:
        return len(self.files) > 1


@dataclass
class Bundle:
    """
    A read-only parquet bundle, open for querying.

    Use `Bundle.open()`; the constructor takes already-validated state.
    """

    root: Path
    manifest: dict[str, Any]
    tables: dict[str, BundleTable]
    con: duckdb.DuckDBPyConnection = field(repr=False)

    # ------------------------------------------------------------------
    # Opening
    # ------------------------------------------------------------------
    @classmethod
    def open(
        cls,
        path: str | Path,
        *,
        threads: Optional[int] = None,
        memory_limit: Optional[str] = None,
        verify: bool = True,
    ) -> "Bundle":
        """
        Validate a bundle directory and register its tables as views.

        `verify` runs the cheap tier: every declared file is present and
        the size the manifest recorded. That is 100-odd `stat()` calls,
        and the size comes free in the same syscall as the existence
        check, so it is on by default. Checksums are a separate,
        explicit operation (`db verify --hashes`) — this never reads
        21 GB to answer "can I open it".
        """
        root = Path(path).expanduser().resolve()
        manifest = cls._read_manifest(root)
        cls._check_version(root, manifest)

        tables = cls._resolve_tables(root, manifest)
        if verify:
            cls._verify_files(root, manifest)

        con = duckdb.connect()
        if threads:
            con.execute(f"SET threads = {int(threads)}")
        if memory_limit:
            con.execute("SET memory_limit = ?", [memory_limit])
        # Results are assembled by joins, never by input order; letting
        # DuckDB drop that guarantee is a straight memory saving on the
        # large scans.
        con.execute("SET preserve_insertion_order = false")

        bundle = cls(root=root, manifest=manifest, tables=tables, con=con)
        bundle._register_views()
        return bundle

    @staticmethod
    def _read_manifest(root: Path) -> dict[str, Any]:
        if not root.is_dir():
            raise BundleNotFound(f"Not a directory: {root}")

        manifest_path = root / MANIFEST_FILENAME
        if not manifest_path.is_file():
            raise BundleNotFound(
                f"No {MANIFEST_FILENAME} in {root}. Point --bundle at the "
                f"bundle directory, not at its tables/ subdirectory."
            )
        try:
            return json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            raise BundleNotFound(f"{manifest_path} is not valid JSON: {e}") from e

    @staticmethod
    def _check_version(root: Path, manifest: dict[str, Any]) -> None:
        version = manifest.get("manifest_version")
        if version in SUPPORTED_MANIFEST_VERSIONS:
            return
        raise BundleVersionError(
            f"{root.name} declares manifest_version {version!r}; this "
            f"Biofilter reads {sorted(SUPPORTED_MANIFEST_VERSIONS)}. "
            f"Update Biofilter to read this bundle — the bundle itself is "
            f"fine and needs no migration."
        )

    @staticmethod
    def _resolve_tables(
        root: Path, manifest: dict[str, Any]
    ) -> dict[str, BundleTable]:
        """
        Group the manifest's file entries into logical tables.

        A partitioned table declares one entry per file, each naming the
        table it belongs to in `table`; a single-file table names only
        itself. So the logical name is `table` when present and `name`
        otherwise — no filename parsing, and no ordering to depend on.
        """
        grouped: dict[str, list[dict[str, Any]]] = {}
        for entry in manifest.get("tables") or []:
            logical = entry.get("table") or entry.get("name")
            if not logical or not entry.get("file"):
                continue
            grouped.setdefault(logical, []).append(entry)

        tables: dict[str, BundleTable] = {}
        for logical, entries in grouped.items():
            tables[logical] = BundleTable(
                name=logical,
                files=tuple(root / e["file"] for e in entries),
                rows=sum(int(e.get("rows") or 0) for e in entries),
                branch=entries[0].get("branch", "core"),
            )
        return tables

    @staticmethod
    def _verify_files(root: Path, manifest: dict[str, Any]) -> None:
        problems: list[str] = []
        for entry in manifest.get("tables") or []:
            rel = entry.get("file")
            if not rel:
                continue
            path = root / rel
            if not path.is_file():
                problems.append(f"{entry.get('name')}: missing {rel}")
                continue
            declared = entry.get("bytes")
            if declared is not None and path.stat().st_size != declared:
                problems.append(
                    f"{entry.get('name')}: size mismatch "
                    f"(manifest {declared}, found {path.stat().st_size})"
                )
        if problems:
            raise BundleIncomplete(
                f"{root.name} does not match its manifest:\n  "
                + "\n  ".join(problems[:20])
                + (f"\n  ... and {len(problems) - 20} more" if len(problems) > 20 else "")
            )

    def _register_views(self) -> None:
        """
        One view per logical table, over the exact files the manifest
        names. Views are catalog objects, so every cursor sees them.
        """
        for table in self.tables.values():
            paths = ", ".join(_sql_string(str(p)) for p in sorted(table.files))
            self.con.execute(
                f'CREATE OR REPLACE VIEW {_sql_ident(table.name)} AS '
                f"SELECT * FROM read_parquet([{paths}], union_by_name = true)"
            )

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------
    @property
    def bundle_id(self) -> Optional[str]:
        return self.manifest.get("bundle_id")

    @property
    def biofilter_version(self) -> Optional[str]:
        return self.manifest.get("biofilter_version")

    @property
    def created_at(self) -> Optional[str]:
        return self.manifest.get("created_at")

    def build_record(self) -> Optional[dict[str, Any]]:
        """
        The per-step provenance behind `bundle_id`: which DTP, which
        version, which source URL, which extract hash. Read on demand —
        it is large and most reports never need it.
        """
        path = self.root / BUILD_RECORD_FILENAME
        if not path.is_file():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------
    def cursor(self) -> duckdb.DuckDBPyConnection:
        """
        A connection of its own over the same database.

        The bundle's views are catalog objects and stay visible; temp
        tables and registered relations do not leak between cursors.
        That is what gives one report execution its own scratch space —
        something the SQLAlchemy layer could not provide, because a
        parquet bundle forced every session onto one shared connection.
        """
        return self.con.cursor()

    def has(self, table: str) -> bool:
        return table in self.tables

    def require(self, *names: str) -> None:
        """
        Fail with the missing names, before running anything.

        A report that needs `variant_gtex` against a bundle built
        without it should say so in one line, not fail mid-query.
        """
        missing = [n for n in names if n not in self.tables]
        if missing:
            raise BundleIncomplete(
                f"{self.root.name} does not carry: {', '.join(sorted(missing))}. "
                f"It has {len(self.tables)} tables; this report needs all of "
                f"{', '.join(names)}."
            )

    def close(self) -> None:
        try:
            self.con.close()
        except Exception:  # noqa: BLE001 — closing must not raise
            pass

    def __enter__(self) -> "Bundle":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def _sql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
