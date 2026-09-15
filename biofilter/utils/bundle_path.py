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
