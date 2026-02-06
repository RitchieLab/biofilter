# biofilter/modules/kdc/utils.py
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_json(data: Any) -> str:
    # Stable hashing: sorted keys + no whitespace
    dumped = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return sha256_text(dumped)


def is_partition_dir(name: str) -> bool:
    # Parquet-style partition folder: key=value
    return "=" in name and not name.startswith("_")


def parse_partitioning_from_path(root: Path, base: Path) -> list[str]:
    """
    Infer partition keys by scanning relative path segments below `base`.
    Example: base/.../assembly=GRCh38/chromosome=1/bucket=002/part-*.parquet
    returns ["chromosome", "bucket"]
    """
    keys: list[str] = []
    # We cannot inspect all files cheaply; infer from directory structure.
    # Collect unique keys seen in first few partition-like dirs.
    rel = base.relative_to(root)
    for part in rel.parts:
        if "=" in part:
            key = part.split("=", 1)[0]
            if key not in keys:
                keys.append(key)
    return keys


def derive_path_pattern(base_dir: Path) -> str:
    """
    Conservative default: a pattern that matches any Parquet beneath base_dir.
    The scanner may tighten this if it detects partition dirs.
    """
    return "**/*.parquet"
