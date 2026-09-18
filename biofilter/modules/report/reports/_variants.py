"""
Reading what a user typed as a variant.

Shared by every report that accepts rsIDs and positions in the same list
as gene names. Keeping one implementation matters for the same reason it
does in `_resolution.py`: the six reports this module was written to
replace each carried their own copy of these parsers, and they had
already drifted — one of them mapped `mito` to chromosome 25 and another
did not, so the same input landed on different chromosomes depending on
which report you asked.

The chromosome encoding is the build's: 1-22, then X=23, Y=24, MT=25.
"""

from __future__ import annotations

import re
from typing import Any, Optional

import pyarrow as pa

_RSID = re.compile(r"^rs\d+$", re.IGNORECASE)
_CHR_POS_ALLELE = re.compile(
    r"^([^:\s]+)[:\-_\s]+(\d+)[:\-_\s]+([ACGTN*]+)[:\-_\s]+([ACGTN*]+)$", re.I
)
_CHR_POS = re.compile(r"^([^:\s]+)[:\-_\s]+(\d+)$")

#: Non-numeric chromosomes, as the build encodes them. `mito` and
#: `mitochondria` are here because two of the legacy reports accepted
#: them and dropping the spelling would silently reject input that used
#: to work.
_CHROMOSOME_CODES = {
    "x": 23,
    "y": 24,
    "m": 25,
    "mt": 25,
    "mito": 25,
    "mitochondria": 25,
}

#: What `classify()` can return in `input_kind`.
INPUT_KINDS = ("gene", "rsid", "chr_pos", "chr_pos_allele", "invalid")


def parse_chromosome(value: Any) -> Optional[int]:
    """`chr17`, `17`, `X`, `MT` → the integer the build stores."""
    text = str(value or "").strip().lower()
    for prefix in ("chromosome", "chrom", "chr"):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    text = text.strip()
    if text in _CHROMOSOME_CODES:
        return _CHROMOSOME_CODES[text]
    try:
        number = int(text)
    except ValueError:
        return None
    return number if 1 <= number <= 25 else None


def classify(value: Any) -> dict[str, Any]:
    """
    Read one input as a variant, or fall back to a gene name.

    Anything that is not an rsID or a position is treated as a gene —
    that is what lets the shapes share one list rather than needing a
    parameter to say which was meant.
    """
    text = str(value).strip()
    row: dict[str, Any] = {
        "input_value": text,
        "input_kind": "gene",
        "rsid": None,
        "chromosome": None,
        "position": None,
        "reference_allele": None,
        "alternate_allele": None,
        "term": text.lower(),
    }
    if not text:
        row["input_kind"] = "invalid"
        return row

    if _RSID.match(text):
        row.update(input_kind="rsid", rsid=text.lower())
        return row

    match = _CHR_POS_ALLELE.match(text)
    if match:
        chromosome = parse_chromosome(match.group(1))
        if chromosome:
            row.update(
                input_kind="chr_pos_allele",
                chromosome=chromosome,
                position=int(match.group(2)),
                reference_allele=match.group(3).upper(),
                alternate_allele=match.group(4).upper(),
            )
            return row

    match = _CHR_POS.match(text)
    if match:
        chromosome = parse_chromosome(match.group(1))
        if chromosome:
            row.update(
                input_kind="chr_pos",
                chromosome=chromosome,
                position=int(match.group(2)),
            )
            return row

    return row


def input_table(values: list[Any]) -> pa.Table:
    """The classified input, typed, ready to register as a relation."""
    parsed = [classify(v) for v in values]
    return pa.table(
        {
            "input_value": pa.array([r["input_value"] for r in parsed], pa.string()),
            "input_kind": pa.array([r["input_kind"] for r in parsed], pa.string()),
            "rsid": pa.array([r["rsid"] for r in parsed], pa.string()),
            "chromosome": pa.array([r["chromosome"] for r in parsed], pa.int32()),
            "position": pa.array([r["position"] for r in parsed], pa.int64()),
            "reference_allele": pa.array(
                [r["reference_allele"] for r in parsed], pa.string()
            ),
            "alternate_allele": pa.array(
                [r["alternate_allele"] for r in parsed], pa.string()
            ),
            "term": pa.array([r["term"] for r in parsed], pa.string()),
        }
    )
