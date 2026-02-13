"""
Docstring for tests.unit.miner.test_text_miner_core

1) Testes unitários (sem DB) — focar na lógica “pura”

Crie tests/unit/test_text_miner_core.py cobrindo:

tokenização + offsets (fallback):

entrada: "HGNC:5 A1BG-AS1" → tokens com spans corretos.

ngram windowing e cap:

garante que respeita max_windows_per_chunk.

overlap resolution (postprocess):

se você tem spans sobrepostos, “longest-span-wins” de fato remove o menor.

clamp_text + chunking:

chunk_size/overlap gera janelas determinísticas.

Esses testes não precisam do Biofilter nem Session. Só do text_miner.py + text_miner_fallback.py.
"""

# tests/unit/miner/test_text_miner_core.py
from __future__ import annotations

import pytest

from biofilter.modules.search.text_miner import (
    TextMinerConfig,
    clamp_text,
    spans_overlap,
    choose_non_overlapping_longest,
    dedup_mentions_by_entity_id,
    postprocess_mentions,
)

pytestmark = pytest.mark.unit


# -----------------------------------------------------------------------------
# clamp_text
# -----------------------------------------------------------------------------
def test_clamp_text_no_limit():
    assert clamp_text("abcdef", None) == "abcdef"


def test_clamp_text_positive_limit():
    assert clamp_text("abcdef", 3) == "abc"


def test_clamp_text_zero_or_negative_limit():
    assert clamp_text("abcdef", 0) == ""
    assert clamp_text("abcdef", -1) == ""


# -----------------------------------------------------------------------------
# spans_overlap
# -----------------------------------------------------------------------------
def test_spans_overlap_non_overlapping(make_span):
    a = make_span(0, 5)
    b = make_span(5, 10)  # touching boundary => not overlap
    assert spans_overlap(a, b) is False


def test_spans_overlap_overlapping(make_span):
    a = make_span(0, 6)
    b = make_span(5, 10)
    assert spans_overlap(a, b) is True


def test_spans_overlap_containment(make_span):
    a = make_span(0, 10)
    b = make_span(2, 5)
    assert spans_overlap(a, b) is True


# -----------------------------------------------------------------------------
# choose_non_overlapping_longest
# -----------------------------------------------------------------------------
def test_choose_non_overlapping_longest_prefers_longer_span(make_mention):
    # Overlap: keep the longer (0..10) over (2..6)
    m_long = make_mention(start=0, end=10, entity_id=1)
    m_short = make_mention(start=2, end=6, entity_id=2)

    out = choose_non_overlapping_longest([m_short, m_long])
    assert len(out) == 1
    assert out[0].span.start == 0 and out[0].span.end == 10


def test_choose_non_overlapping_longest_is_deterministic_on_ties(make_mention):
    # Same length, overlaps: tie-break by start asc (so keep start=0)
    m1 = make_mention(start=0, end=5, entity_id=1)
    m2 = make_mention(start=1, end=6, entity_id=2)

    out = choose_non_overlapping_longest([m2, m1])
    assert len(out) == 1
    assert out[0].span.start == 0 and out[0].span.end == 5


def test_choose_non_overlapping_longest_keeps_multiple_non_overlapping(make_mention):
    m1 = make_mention(start=0, end=5, entity_id=1)
    m2 = make_mention(start=6, end=10, entity_id=2)

    out = choose_non_overlapping_longest([m2, m1])
    assert [(m.span.start, m.span.end) for m in out] == [(0, 5), (6, 10)]


# -----------------------------------------------------------------------------
# dedup_mentions_by_entity_id
# -----------------------------------------------------------------------------
def test_dedup_mentions_by_entity_id_keeps_first_occurrence(make_mention):
    m1 = make_mention(start=0, end=5, entity_id=10, score=95.0)
    m2 = make_mention(start=10, end=15, entity_id=10, score=99.0)  # same entity later

    out = dedup_mentions_by_entity_id([m1, m2])
    assert len(out) == 1
    assert out[0].span.start == 0
    assert out[0].best is not None
    assert out[0].best.entity_id == 10


def test_dedup_mentions_by_entity_id_skips_mentions_without_best(make_mention):
    # Mention with empty candidates => best=None => should be ignored
    m_empty = make_mention(start=0, end=5, candidates=[])
    m_ok = make_mention(start=6, end=10, entity_id=11)

    out = dedup_mentions_by_entity_id([m_empty, m_ok])
    assert len(out) == 1
    assert out[0].best is not None
    assert out[0].best.entity_id == 11


# -----------------------------------------------------------------------------
# postprocess_mentions
# -----------------------------------------------------------------------------
def test_postprocess_drops_too_short_spans(make_mention):
    cfg = TextMinerConfig(min_span_chars=3, longest_span_wins=False)
    m_ok = make_mention(start=0, end=3, entity_id=1)  # len=3 ok
    m_bad = make_mention(start=5, end=7, entity_id=2)  # len=2 drop

    out = postprocess_mentions([m_bad, m_ok], config=cfg)
    assert len(out) == 1
    assert out[0].best is not None
    assert out[0].best.entity_id == 1


def test_postprocess_finalizes_and_sorts_candidates(make_candidate, make_span):
    cfg = TextMinerConfig(longest_span_wins=False)

    span = make_span(0, 10, text="x" * 10)
    c1 = make_candidate(entity_id=1, score=50.0)
    c2 = make_candidate(entity_id=2, score=99.0)

    from biofilter.modules.search.text_miner import Mention

    m = Mention(span=span, candidates=[c1, c2])  # unsorted
    out = postprocess_mentions([m], config=cfg)

    assert len(out) == 1
    assert out[0].best is not None
    assert out[0].best.entity_id == 2  # highest score first


def test_postprocess_longest_span_wins(make_mention):
    cfg = TextMinerConfig(longest_span_wins=True)

    m_long = make_mention(start=0, end=12, entity_id=1)
    m_short = make_mention(start=2, end=6, entity_id=2)

    out = postprocess_mentions([m_short, m_long], config=cfg)
    assert len(out) == 1
    assert out[0].best is not None
    assert out[0].best.entity_id == 1


def test_postprocess_dedup_by_entity_id(make_mention):
    cfg = TextMinerConfig(longest_span_wins=False, dedup_by_entity_id=True)

    m1 = make_mention(start=0, end=5, entity_id=10)
    m2 = make_mention(start=6, end=9, entity_id=10)

    out = postprocess_mentions([m1, m2], config=cfg)
    assert len(out) == 1
    assert out[0].best is not None
    assert out[0].best.entity_id == 10
    assert out[0].span.start == 0


def test_postprocess_respects_max_mentions(make_mention):
    cfg = TextMinerConfig(longest_span_wins=False, max_mentions=2)

    m1 = make_mention(start=0, end=3, entity_id=1)
    m2 = make_mention(start=4, end=7, entity_id=2)
    m3 = make_mention(start=8, end=11, entity_id=3)

    out = postprocess_mentions([m1, m2, m3], config=cfg)
    assert len(out) == 2
    assert [m.best.entity_id for m in out if m.best] == [1, 2]
