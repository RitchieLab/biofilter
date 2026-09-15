"""
Turning a bundle path into the URI the engine expects.

Its own module because both entry points need it — `--bundle` on the
CLI and `Biofilter(bundle=...)` in Python — and neither should import
the other. Two copies would drift.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

#: How the engine addresses a bundle. The scheme exists only because
#: Biofilter once spoke to several backends.
PARQUET_URI_SCHEME = "parquet://"


def bundle_to_uri(path: str | Path | None) -> Optional[str]:
    """
    `/data/bundles/20260914` → `parquet:///data/bundles/20260914`.

    A bundle is a folder someone was handed; the URI is an
    implementation detail, and keeping the translation here means nobody
    has to memorise it — including the triple slash, which trips people
    up every time.

    Returns None for an empty path, so callers can fall through to
    whatever other source of a URI they have.
    """
    if not path:
        return None
    return f"{PARQUET_URI_SCHEME}{Path(str(path)).expanduser().resolve()}"


class BundlePathError(ValueError):
    """The path given for a bundle is not one, with a reason."""


def _looks_like_bundle(path: Path) -> bool:
    return (path / "manifest.json").is_file()


def check_bundle_path(uri: str) -> None:
    """
    Fail early, and about the path the caller actually wrote.

    Without this the failure surfaces from deep in the engine as
    "Database not found at duckdb:///:memory:" — naming the translated
    URI, which the caller never typed and cannot act on.

    Raises BundlePathError with the path and what is wrong with it; does
    nothing for a URI that is not a bundle, or for a bundle that is fine.
    """
    if not uri or not uri.startswith(PARQUET_URI_SCHEME):
        return

    path = Path("/" + uri[len(PARQUET_URI_SCHEME):].lstrip("/"))

    if _looks_like_bundle(path):
        return

    if not path.exists():
        hint = ""
        parent = path.parent
        if parent.is_dir():
            siblings = sorted(
                child.name
                for child in parent.iterdir()
                if child.is_dir() and _looks_like_bundle(child)
            )
            if siblings:
                listed = "\n    ".join(siblings[:8])
                more = (
                    f"\n    ... and {len(siblings) - 8} more"
                    if len(siblings) > 8
                    else ""
                )
                hint = f"\n\n  Bundles in {parent}:\n    {listed}{more}"
        raise BundlePathError(f"No such directory: {path}{hint}")

    if not path.is_dir():
        raise BundlePathError(f"Not a directory: {path}")

    # The usual mistake: tables/ holds the parquet, but manifest.json —
    # the bundle id, the plan, the table map — is one level up.
    if path.name == "tables" and _looks_like_bundle(path.parent):
        raise BundlePathError(
            f"{path} is a bundle's tables/ directory.\n"
            f"  Point at the bundle itself: {path.parent}"
        )

    inner = path / "tables"
    detail = (
        "it has a tables/ directory but no manifest.json, so it is an "
        "incomplete or interrupted build"
        if inner.is_dir()
        else "there is no manifest.json in it"
    )
    raise BundlePathError(f"{path} is not a bundle — {detail}.")
