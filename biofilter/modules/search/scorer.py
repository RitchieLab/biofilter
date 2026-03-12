from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from rapidfuzz import fuzz

from biofilter.modules.search.types import Candidate, NormalizedQuery


@dataclass(frozen=True)
class ScoringConfig:
    """
    Scores are normalized to 0..100.

    - `min_score`: minimum score to accept resolution
    - `min_delta`: required margin between top1 and top2 to auto-resolve
    """

    min_score: float = 90.0
    min_delta: float = 5.0

    # weights / boosts
    primary_name_boost: float = 5.0
    type_match_boost: float = 3.0


class CandidateScorer:
    def __init__(self, config: ScoringConfig | None = None):
        self.config = config or ScoringConfig()

    def exact_score(self, query: NormalizedQuery, cand: Candidate) -> float:
        """
        Deterministic exact scoring: used when DB already found exact matches.
        """
        if not cand.matched_name:
            return 0.0

        q = query.strict
        m = cand.matched_name

        if m == query.raw:
            return 100.0
        if m == query.basic:
            return 99.0
        if m == q:
            return 98.0
        return 95.0

    def fuzzy_score(self, query: NormalizedQuery, cand_text: str) -> float:
        """
        RapidFuzz scoring. We use token_set_ratio because it is robust to
        reordering and extra tokens (great for biomedical aliases).
        """
        if not cand_text:
            return 0.0
        return float(fuzz.token_set_ratio(query.strict, cand_text))

    def apply_heuristics(
        self,
        score: float,
        cand: Candidate,
        *,
        entity_type_hint: Optional[str] = None,
    ) -> float:
        s = score

        # Primary-name boost if available and we know the match is primary
        if cand.meta.get("is_primary_name") is True:
            s += self.config.primary_name_boost

        # Entity-type preference boost
        if (
            entity_type_hint
            and cand.entity_type
            and cand.entity_type == entity_type_hint
        ):
            s += self.config.type_match_boost

        return min(100.0, s)
