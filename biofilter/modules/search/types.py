from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Optional

MatchMethod = Literal[
    "exact_name",
    "exact_normalized",
    "db_pool",
    "fuzzy_pool",
]

ResolutionStatus = Literal["resolved", "ambiguous", "not_found"]


@dataclass(frozen=True)
class NormalizedQuery:
    """Represents multiple deterministic normalization variants of the same input."""

    raw: str
    basic: str
    strict: str
    tokens: tuple[str, ...] = ()


@dataclass
class Candidate:
    """A ranked candidate Entity result."""

    entity_id: int
    entity_type: Optional[str] = None

    primary_name: Optional[str] = None
    matched_name: Optional[str] = None  # alias or primary that matched
    matched_name_id: Optional[int] = None  # EntityName.id if available

    method: MatchMethod = "db_pool"
    score: float = 0.0  # normalized to 0..100

    # Optional provenance/debug fields
    data_source: Optional[str] = None
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class Resolution:
    status: ResolutionStatus
    query: NormalizedQuery
    best: Optional[Candidate] = None
    candidates: list[Candidate] = field(default_factory=list)

    # Audit
    reason: Optional[str] = None
    min_score: float = 90.0
    min_delta: float = 5.0
