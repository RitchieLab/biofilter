from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Optional

from biofilter.modules.search.normalizers import TextNormalizer
from biofilter.modules.search.scorer import CandidateScorer, ScoringConfig
from biofilter.modules.search.types import Candidate, NormalizedQuery, Resolution


# ---- DB retrieval protocol (pluggable) ---------------------------------------
# The retriever returns a pool of Candidate objects with matched_name filled.
RetrieverFn = Callable[[NormalizedQuery, int, Optional[list[str]]], list[Candidate]]
# args: (query, pool_limit, entity_type_hints)


@dataclass(frozen=True)
class ResolverConfig:
    pool_limit: int = 1000
    top_k: int = 20

    # Auto-resolution thresholds (RapidFuzz scores 0..100)
    min_score: float = 90.0
    min_delta: float = 5.0

    # Behavior
    enable_fuzzy_fallback: bool = True


class TermResolver:
    """
    Term resolution pipeline:

    1) Normalize text (deterministic)
    2) Retrieve candidate pool via DB retriever (exact + loose pool)
    3) If exact hit exists -> score/rank quickly
    4) If weak/empty -> fuzzy score within pool using RapidFuzz
    5) Resolve best / ambiguous / not_found with auditability
    """

    def __init__(
        self,
        retriever: RetrieverFn,
        *,
        normalizer: TextNormalizer | None = None,
        scorer: CandidateScorer | None = None,
        config: ResolverConfig | None = None,
    ):
        self.retriever = retriever
        self.normalizer = normalizer or TextNormalizer()
        self.config = config or ResolverConfig()
        scoring_cfg = ScoringConfig(min_score=self.config.min_score, min_delta=self.config.min_delta)
        self.scorer = scorer or CandidateScorer(scoring_cfg)

    def search(
        self,
        text: str,
        *,
        entity_type_hints: Optional[list[str]] = None,
        limit: Optional[int] = None,
    ) -> list[Candidate]:
        q = self.normalizer.build(text)
        pool = self.retriever(q, self.config.pool_limit, entity_type_hints)

        # Score (exact if method indicates exact, otherwise fuzzy if needed)
        scored = self._score_candidates(q, pool, entity_type_hints=entity_type_hints)

        scored.sort(key=lambda c: c.score, reverse=True)
        return scored[: (limit or self.config.top_k)]

    def resolve_best(
        self,
        text: str,
        *,
        entity_type_hints: Optional[list[str]] = None,
    ) -> Resolution:
        q = self.normalizer.build(text)
        pool = self.retriever(q, self.config.pool_limit, entity_type_hints)

        if not pool:
            return Resolution(
                status="not_found",
                query=q,
                candidates=[],
                reason="No candidates returned from retriever.",
                min_score=self.config.min_score,
                min_delta=self.config.min_delta,
            )

        candidates = self._score_candidates(q, pool, entity_type_hints=entity_type_hints)
        candidates.sort(key=lambda c: c.score, reverse=True)
        candidates = candidates[: self.config.top_k]

        top1 = candidates[0]
        top2 = candidates[1] if len(candidates) > 1 else None

        if top1.score >= self.config.min_score:
            if top2 is None or (top1.score - top2.score) >= self.config.min_delta:
                return Resolution(
                    status="resolved",
                    query=q,
                    best=top1,
                    candidates=candidates,
                    reason="Top candidate meets min_score and min_delta.",
                    min_score=self.config.min_score,
                    min_delta=self.config.min_delta,
                )
            return Resolution(
                status="ambiguous",
                query=q,
                best=top1,
                candidates=candidates,
                reason="Top candidate meets min_score but not min_delta (ambiguous).",
                min_score=self.config.min_score,
                min_delta=self.config.min_delta,
            )

        return Resolution(
            status="not_found",
            query=q,
            best=top1,
            candidates=candidates,
            reason="Top candidate below min_score.",
            min_score=self.config.min_score,
            min_delta=self.config.min_delta,
        )

    # ---------------------------------------------------------------------
    def _score_candidates(
        self,
        q: NormalizedQuery,
        candidates: list[Candidate],
        *,
        entity_type_hints: Optional[list[str]] = None,
    ) -> list[Candidate]:
        hints = entity_type_hints or []
        hint = hints[0] if hints else None  # v1: simple single-type preference

        out: list[Candidate] = []

        # If we already have exact matches, score deterministically first.
        for c in candidates:
            if c.method in ("exact_name", "exact_normalized"):
                base = self.scorer.exact_score(q, c)
                c.score = self.scorer.apply_heuristics(base, c, entity_type_hint=hint)
                out.append(c)

        # If exact results exist, include the rest but keep scoring light.
        have_exact = len(out) > 0

        if have_exact:
            for c in candidates:
                if c in out:
                    continue
                # Light fuzzy re-rank as tie-breaker (optional)
                base = self.scorer.fuzzy_score(q, c.matched_name or c.primary_name or "")
                c.method = c.method or "db_pool"
                c.score = self.scorer.apply_heuristics(base, c, entity_type_hint=hint)
                out.append(c)
            return out

        # No exact hits: fuzzy fallback on the entire pool
        # if self.config.enable_fuzzy_fallback:
        #     for c in candidates:
        #         text = c.matched_name or c.primary_name or ""
        #         base = self.scorer.fuzzy_score(q, text)
        #         # c.method = "fuzzy_pool"
        #         c.method = c.method or "fuzzy_pool"
        #         c.score = self.scorer.apply_heuristics(base, c, entity_type_hint=hint)
        #         out.append(c)
        #     return out
        if self.config.enable_fuzzy_fallback:
            for c in candidates:
                text = c.matched_name or c.primary_name or ""

                if c.method == "pg_trgm":
                    pg = c.meta.get("pg_trgm_score")
                    if pg is not None:
                        base = float(pg) * 100.0
                    else:
                        base = self.scorer.fuzzy_score(q, text)
                else:
                    base = self.scorer.fuzzy_score(q, text)

                c.method = c.method or "fuzzy_pool"
                c.score = self.scorer.apply_heuristics(base, c, entity_type_hint=hint)
                out.append(c)
            return out


        # Fallback disabled: return unscored
        return candidates
