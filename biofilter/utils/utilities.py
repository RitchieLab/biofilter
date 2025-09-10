# utils/utilities.py
from __future__ import annotations

import math
import re
from typing import Any, List, Optional

try:
    import numpy as np  # optional, but handy for ndarray checks
except Exception:  # pragma: no cover
    np = None  # type: ignore

__all__ = [
    "string_normalization",
    "as_list",
]

# ---------- TEXT NORMATIZATION ----------

_LATIN1_TRANS = str.maketrans(
    "ÁÀÂÄÃáàâäãÉÈÊËéèêëÍÌÎÏíìîïÓÒÔÖÕóòôöõÚÙÛÜúùûüÑñÇç",
    "AAAAAaaaaaEEEEeeeeIIIIiiiiOOOOOoooooUUUUuuuuNnCc",
)
_WS_RE = re.compile(r"\s+")


def string_normalization(s: Optional[str]) -> str:
    """
    Normalize a string for matching/search:
    - Trim outer spaces
    - Lowercase
    - Replace Latin-1 accented characters
    - Collapse multiple spaces to a single space
    - Handle common ligatures
    """
    if not s:
        return ""
    s = (
        s.replace("ß", "ss")
        .replace("Æ", "ae")
        .replace("æ", "ae")
        .replace("Œ", "oe")
        .replace("œ", "oe")
    )
    s = s.translate(_LATIN1_TRANS).lower()
    s = _WS_RE.sub(" ", s.strip())
    return s


# ---------- Generic ETL helpers ----------


def _is_nullish(v: Any) -> bool:
    """Return True for None, NaN (float/NumPy), or empty-string after strip."""
    if v is None:
        return True
    # float NaN
    if isinstance(v, float) and math.isnan(v):
        return True
    # numpy scalar NaN
    if np is not None and isinstance(v, (np.floating,)):  # type: ignore[attr-defined]  # noqa E501
        return bool(np.isnan(v))  # type: ignore[arg-type]
    # empty string
    try:
        return str(v).strip() == ""
    except Exception:
        return False


def as_list(value: Any) -> List[str]:
    """
    Convert a value (str, list/tuple/set, np.ndarray, None, float NaN) into
    a clean list of non-empty, stripped strings.
    """
    if _is_nullish(value):
        return []

    # Iterable containers
    if isinstance(value, (list, tuple, set)):
        return [str(v).strip() for v in value if not _is_nullish(v)]

    # NumPy ndarray
    if np is not None and isinstance(value, np.ndarray):  # type: ignore[attr-defined]  # noqa E501
        return [str(v).strip() for v in value.tolist() if not _is_nullish(v)]

    # Fallback: single item
    return [str(value).strip()]
