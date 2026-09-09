"""
Shared helpers for the gnomAD v4 DTPs.

`dtp_variant_gnomad_joint` and `dtp_variant_gnomad_vep` are deliberately
separate data sources — they read different files and produce different
parquets — but they resolve chromosomes and field selection the same way.
That common part lives here so neither DTP has to import the other.

See ADR-003 (notebooks/Andre/ADR/0003-parquet-native-build-pipeline.md).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

GNOMAD_BASE = "https://storage.googleapis.com/gcp-public-data--gnomad/release"

CONFIG_DIR = Path(__file__).resolve().parent / "config"


def chromosome_from_datasource_name(name: str) -> Optional[str]:
    """
    Derive the chromosome token from a data source named
    `gnomad_joint_chr21` / `gnomad_vep_chrx`.

    Returns the token as gnomAD spells it in the file name (uppercase for
    X and Y), or None when the name does not carry a usable one.
    """
    token = (name or "").strip().lower()
    marker = "chr"
    idx = token.rfind(marker)
    if idx == -1:
        return None

    chrom = token[idx + len(marker):]
    if not chrom:
        return None
    if chrom in ("x", "y"):
        return chrom.upper()
    if chrom.isdigit() and 1 <= int(chrom) <= 22:
        return str(int(chrom))
    return None


def load_field_config(dtp_name: str, override: Optional[str] = None) -> dict:
    """
    Read the JSON field-selection config shipped next to the DTP.

    The config is an *include*-list: only entries flagged `load: true`
    reach the parquet. gnomAD ships 664 INFO fields on the joint callset
    and 46 VEP subfields, so an exclude-list would silently adopt whatever
    a future release adds; this way a new field surfaces as `load: false`
    and widening the schema stays a decision someone makes.

    `override` points at a user-supplied JSON of the same shape, so a
    bundle can be built with a different selection without editing the
    packaged default.
    """
    path = Path(override) if override else CONFIG_DIR / f"{dtp_name}.json"
    if not path.is_file():
        raise FileNotFoundError(f"Field config not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def selected_fields(config: dict, key: str) -> List[str]:
    """Names flagged `load: true` under `key`, in config order."""
    return [f["name"] for f in config.get(key, []) if f.get("load")]


def build_filters(config: dict) -> Dict[str, Any]:
    """
    Normalise the config's filter block into plain bounds.

    Bounds are inclusive; `null` disables one. They are applied at build
    time, so whatever they exclude is absent from the resulting bundle for
    good — ADR-003 §2.1 makes bundles immutable, so there is no later pass
    that can add the rows back.
    """
    raw = config.get("filters", {}) or {}
    return {
        "min_ac": raw.get("min_ac"),
        "max_ac": raw.get("max_ac"),
        "min_af": raw.get("min_af"),
        "max_af": raw.get("max_af"),
        "ac_field": raw.get("ac_field", "AC_joint"),
        "af_field": raw.get("af_field", "AF_joint"),
    }


def passes_filters(ac, af, filters: Dict[str, Any]) -> bool:
    """
    True when a variant clears the configured AC/AF bounds.

    A missing value fails a bound that is set: an absent frequency is not
    evidence that the variant qualifies, so it is excluded rather than
    waved through.
    """
    checks = (
        (filters.get("min_ac"), ac, lambda b, v: v >= b),
        (filters.get("max_ac"), ac, lambda b, v: v <= b),
        (filters.get("min_af"), af, lambda b, v: v >= b),
        (filters.get("max_af"), af, lambda b, v: v <= b),
    )
    for bound, value, ok in checks:
        if bound is None:
            continue
        if value is None or not ok(bound, value):
            return False
    return True


def describe_filters(filters: Dict[str, Any]) -> str:
    """One-line human summary, for the ETL log and the parquet metadata."""
    parts = []
    for label, lo, hi in (
        (filters.get("ac_field", "AC"), filters.get("min_ac"), filters.get("max_ac")),  # noqa: E501
        (filters.get("af_field", "AF"), filters.get("min_af"), filters.get("max_af")),  # noqa: E501
    ):
        if lo is not None:
            parts.append(f"{label} >= {lo}")
        if hi is not None:
            parts.append(f"{label} <= {hi}")
    return " and ".join(parts) if parts else "no filters"
