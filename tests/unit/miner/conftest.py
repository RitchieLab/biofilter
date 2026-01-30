# tests/unit/miner/conftest.py
from __future__ import annotations

import pytest

from biofilter.modules.search.text_miner import Mention, MentionCandidate, Span


@pytest.fixture()
def make_span():
    def _make_span(start: int, end: int, text: str | None = None) -> Span:
        if text is None:
            # Default: build text with correct length to satisfy sanity expectations
            text = "x" * (end - start)
        return Span(start=start, end=end, text=text)

    return _make_span


@pytest.fixture()
def make_candidate():
    def _make_candidate(
        *,
        entity_id: int,
        score: float = 100.0,
        group_id: int | None = None,
        matched_name: str | None = None,
    ) -> MentionCandidate:
        return MentionCandidate(
            entity_id=entity_id,
            group_id=group_id,
            matched_name=matched_name,
            score=score,
            method="other",
            meta={},
        )

    return _make_candidate


@pytest.fixture()
def make_mention(make_candidate, make_span):
    def _make_mention(
        *,
        start: int,
        end: int,
        entity_id: int | None = None,
        score: float = 100.0,
        candidates: list[MentionCandidate] | None = None,
        text: str | None = None,
        note: str | None = None,
    ) -> Mention:
        span = make_span(start, end, text=text)
        if candidates is None:
            candidates = []
            if entity_id is not None:
                candidates.append(make_candidate(entity_id=entity_id, score=score))
        return Mention(span=span, candidates=candidates, note=note)

    return _make_mention
