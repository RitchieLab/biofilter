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

    def _render_traceback_(self) -> list[str]:
        """
        What IPython prints instead of a traceback.

        The frames are noise for this error. Nothing in the call stack
        between `Biofilter(...)` and here tells a reader anything the
        message does not already say, and in a notebook the traceback
        buries the one line that matters. IPython calls this hook when an
        exception defines it; a plain Python run still gets the full
        traceback, which is what you want when debugging the library
        rather than using it.
        """
        return [f"BundlePathError: {self}"]


def _looks_like_bundle(path: Path) -> bool:
    # Tolerant on purpose: the sibling scan below walks whatever
    # directory the caller's path sits in, which can be the filesystem
    # root, and macOS has entries there that raise on stat.
    try:
        return (path / "manifest.json").is_file()
    except OSError:
        return False


def _siblings_hint(path: Path, limit: int = 8) -> str:
    """
    The bundles sitting next to the one that was not found.

    A courtesy, not part of the error: if the parent cannot be listed —
    it may not exist, and on macOS the filesystem root holds entries that
    raise on stat — the hint is simply omitted rather than replacing the
    error the caller needs to see.
    """
    try:
        siblings = sorted(
            child.name
            for child in path.parent.iterdir()
            if child.is_dir() and _looks_like_bundle(child)
        )
    except OSError:
        return ""

    if not siblings:
        return ""

    listed = "\n    ".join(siblings[:limit])
    more = (
        f"\n    ... and {len(siblings) - limit} more"
        if len(siblings) > limit
        else ""
    )
    return f"\n\n  Bundles in {path.parent}:\n    {listed}{more}"


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
        raise BundlePathError(f"No such directory: {path}{_siblings_hint(path)}")

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
