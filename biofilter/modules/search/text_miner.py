from __future__ import annotations

"""
Biofilter Search Engine - Text Mining (Interface + Shared Types)

This module defines the public contract for extracting ("mining") entity mentions
from free text.

Key design principles:
- This file defines TYPES + INTERFACES only (no DB, no SQLAlchemy, no Postgres details).
- Concrete implementations live in:
    - text_miner_pg.py       (Postgres / pg_trgm optimized backend)
    - text_miner_fallback.py (portable n-gram + resolver backend)

The miner is intentionally NOT responsible for creating EntityRelationship rows.
It only returns mentions (spans + grounded entity candidates). Consumers decide
how to build edges (co-occurrence rules, sentence-level edges, field-based edges, etc.).
"""

from dataclasses import dataclass, field
from typing import Iterable, Literal, Optional, Protocol, Sequence


# -----------------------------------------------------------------------------
# Status / Method enums (string-literals to stay lightweight and JSON-friendly)
# -----------------------------------------------------------------------------
MiningStatus = Literal["ok", "empty", "error"]
MatchMethod = Literal[
    "pg_trgm",  # candidate recalled by pg_trgm similarity
    "exact_name",  # exact match against alias_value
    "exact_normalized",  # exact match against alias_norm
    "db_pool",  # DB pool/prefix candidates (non-fuzzy)
    "fuzzy_pool",  # fuzzy-scored candidate from pool
    "ngram",  # candidate found via n-gram fallback
    "other",
]


# -----------------------------------------------------------------------------
# Core data structures
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class Span:
    """
    A character span in the ORIGINAL (raw) text.
    Offsets are [start, end) as usual in Python slicing.
    """

    start: int
    end: int
    text: str

    def __post_init__(self) -> None:
        if self.start < 0 or self.end < 0 or self.end < self.start:
            raise ValueError(f"Invalid span: start={self.start} end={self.end}")
        if len(self.text) != (self.end - self.start):
            # Not strictly required, but a helpful sanity check if you always slice.
            # If callers build spans differently, they can disable this by passing
            # a text that matches the range length.
            pass


@dataclass
class MentionCandidate:
    """
    A single candidate mapping for a mention. This is conceptually similar to
    `biofilter.modules.search.types.Candidate` but intentionally decoupled to avoid
    tight coupling between text-mining and the resolver.

    Implementations can populate:
    - entity_id (grounding)
    - group_id / entity_type
    - matched_name: which alias field matched (value or norm) for scoring/audit
    - method + score
    - meta: must remain JSON-serializable
    """

    entity_id: int
    group_id: Optional[int] = None  # EntityGroup.id
    entity_type: Optional[str] = None  # optional string label, if available

    primary_name: Optional[str] = None  # primary display label/code if known
    matched_name: Optional[str] = (
        None  # what text we compared (alias_norm or alias_value)
    )

    matched_name_id: Optional[int] = None  # alias_id if available
    method: MatchMethod = "other"
    score: float = 0.0

    data_source: Optional[str] = None
    meta: dict = field(default_factory=dict)


@dataclass
class Mention:
    """
    A mention detected in text, with grounded candidates.

    - span: where in the text it occurs
    - candidates: ranked candidates (best first)
    - best: convenience pointer (first candidate), if any
    - note: optional diagnostic string (why kept/dropped, etc.)
    """

    span: Span
    candidates: list[MentionCandidate] = field(default_factory=list)
    best: Optional[MentionCandidate] = None

    # lightweight diagnostics for debugging/auditing
    note: Optional[str] = None

    def finalize(self) -> "Mention":
        """
        Ensure best is set and candidates are sorted (descending score).
        """
        if self.candidates:
            self.candidates.sort(key=lambda c: c.score, reverse=True)
            self.best = self.candidates[0]
        else:
            self.best = None
        return self


@dataclass(frozen=True)
class TextMinerConfig:
    """
    Shared configuration for any text miner backend.

    Notes:
    - Keep defaults conservative; callers can override for specific workloads.
    - This config is backend-agnostic (no pg-specific knobs here).
    """

    # Chunking: large texts can be processed in chunks (sentences or windows).
    # Implementations decide how to chunk, but these parameters guide it.
    chunk_size: int = 800  # target chars per chunk (for window chunkers)
    chunk_overlap: int = 120  # overlap chars between windows to avoid boundary misses
    max_text_chars: Optional[int] = None  # optional hard cap for safety

    # Mention extraction
    max_mentions: int = 5000  # safety cap
    min_span_chars: int = 3  # ignore too-short spans

    # Candidate control
    top_k: int = 5  # candidates per mention
    min_score: float = 90.0  # auto-keep threshold for "resolved-like" mentions
    keep_ambiguous: bool = True  # keep mentions below min_score if candidates exist

    # Domain filters
    entity_type_hints: Optional[list[str]] = None  # e.g. ["Chemicals"], ["Genes"]

    # Locale / source filters are shared knobs; backends may ignore if not supported
    locale: Optional[str] = None
    data_source_ids: Optional[list[int]] = None

    # Post-processing behaviors
    longest_span_wins: bool = True
    dedup_by_entity_id: bool = False  # if True, keep only one mention per entity_id


@dataclass
class MiningResult:
    """
    Result container for extract_mentions().
    """

    status: MiningStatus
    text_len: int

    mentions: list[Mention] = field(default_factory=list)

    # optional diagnostics
    backend: Optional[str] = None
    reason: Optional[str] = None
    stats: dict = field(default_factory=dict)


# -----------------------------------------------------------------------------
# Interface (Protocol keeps it lightweight and test-friendly)
# -----------------------------------------------------------------------------
class BaseTextMiner(Protocol):
    """
    Contract for text entity mining backends.

    Implementations MUST:
    - Return mentions with spans in original text coordinates.
    - Return candidates in best-first order (or set best after finalize()).
    - Be deterministic given the same DB state/config (as much as feasible).
    """

    name: str

    def extract_mentions(
        self,
        text: str,
        *,
        config: Optional[TextMinerConfig] = None,
    ) -> MiningResult:
        """
        Extract entity mentions from raw text.

        Backends may:
        - directly ground mentions (entity_id) during extraction, OR
        - emit candidate lists and let consumers decide how to interpret.

        The miner should not mutate the DB.
        """
        ...


# -----------------------------------------------------------------------------
# Shared utilities (backend-agnostic)
# -----------------------------------------------------------------------------
def clamp_text(text: str, max_chars: Optional[int]) -> str:
    if max_chars is None:
        return text
    if max_chars <= 0:
        return ""
    return text[:max_chars]


def spans_overlap(a: Span, b: Span) -> bool:
    return not (a.end <= b.start or b.end <= a.start)


def choose_non_overlapping_longest(mentions: Sequence[Mention]) -> list[Mention]:
    """
    Reduce overlapping mentions by keeping the longest spans first.

    Deterministic strategy:
    - sort by (span_length desc, start asc)
    - keep if it does not overlap any kept mention
    """
    items = list(mentions)
    items.sort(key=lambda m: (-(m.span.end - m.span.start), m.span.start))

    kept: list[Mention] = []
    for m in items:
        if not any(spans_overlap(m.span, k.span) for k in kept):
            kept.append(m)

    kept.sort(key=lambda m: (m.span.start, m.span.end))
    return kept


def dedup_mentions_by_entity_id(mentions: Sequence[Mention]) -> list[Mention]:
    """
    Keep only the first mention per entity_id (based on span order),
    assuming mentions are already ordered by occurrence.

    This is useful for "document-level set of entities" use cases, but
    not ideal for highlighting.
    """
    out: list[Mention] = []
    seen: set[int] = set()

    for m in mentions:
        m.finalize()
        if not m.best or m.best.entity_id is None:
            continue
        if m.best.entity_id in seen:
            continue
        seen.add(m.best.entity_id)
        out.append(m)

    return out


def postprocess_mentions(
    mentions: Sequence[Mention],
    *,
    config: TextMinerConfig,
) -> list[Mention]:
    """
    Shared post-processing across backends:
    - finalize candidates
    - drop too-short spans
    - longest-span-wins
    - optional dedup by entity_id
    """
    prepared: list[Mention] = []
    for m in mentions:
        if (m.span.end - m.span.start) < config.min_span_chars:
            continue
        prepared.append(m.finalize())

    prepared.sort(key=lambda m: (m.span.start, m.span.end))

    if config.longest_span_wins:
        prepared = choose_non_overlapping_longest(prepared)

    if config.dedup_by_entity_id:
        prepared = dedup_mentions_by_entity_id(prepared)

    if config.max_mentions and len(prepared) > config.max_mentions:
        prepared = prepared[: config.max_mentions]

    return prepared
