"""
criar um unit test “adapter-level” que valida o contrato do PgTrgmTextMiner
sem Postgres real, sem pg_trgm, sem SQLAlchemy de verdade.

A ideia do adapter test é:

1. garantir que o backend:
    -   retorna MiningResult com status correto (empty quando texto em branco;
        ok quando há candidates)
    -   chama _pg_trgm_query() com windows derivados do normalizer (a gente
        não precisa validar a lógica do pg_trgm aqui)
    -   agrega spans + candidates e passa por postprocess_mentions

2. garantir que o “plumbing” está certo:
    -   score convertido de 0..1 → 0..100
    -   method="pg_trgm"
    -   meta inclui alias_value/alias_norm/pg_trgm_score

Pra isso vamos:
- monkeypatch _is_postgres pra não levantar o RuntimeError no __init__
- fornecer um session fake (objeto qualquer)
- monkeypatch miner._pg_trgm_query pra devolver “rows” fake
- monkeypatch miner._iter_chunks pra devolver exatamente 1 chunk controlado
- opcional: não patchar TextNormalizer, porque ele já existe e gera
strict/tokens do seu jeito (mas vamos simplificar as janelas: configurar
PgTextMinerConfig com include_chunk_query=True e max_windows_per_chunk=1)
"""

# tests/unit/miner/test_text_miner_pg_adapter.py
from __future__ import annotations

from types import SimpleNamespace

import pytest

from biofilter.modules.search.text_miner import TextMinerConfig
from biofilter.modules.search.text_miner_pg import PgTextMinerConfig, PgTrgmTextMiner


pytestmark = pytest.mark.unit


class _FakeSession:
    """No-op placeholder. We patch miner._pg_trgm_query so it won't use DB."""

    pass


def _row(**kwargs):
    """
    Build a row-like object that supports attribute access (r.alias_value, etc.).
    SQLAlchemy returns Row objects, but for unit tests we can use SimpleNamespace.
    """
    return SimpleNamespace(**kwargs)


def test_pg_text_miner_empty_text_returns_empty(monkeypatch):
    # Bypass postgres requirement
    import biofilter.modules.search.text_miner_pg as mod

    monkeypatch.setattr(mod, "_is_postgres", lambda session: True)

    miner = PgTrgmTextMiner(_FakeSession())
    res = miner.extract_mentions("   \n\t  ")

    assert res.status == "empty"
    assert res.text_len >= 0
    assert res.backend == "pg_trgm"
    assert res.mentions == []


def test_pg_text_miner_happy_path_builds_mentions(monkeypatch):
    import biofilter.modules.search.text_miner_pg as mod

    # 1) Bypass postgres requirement
    monkeypatch.setattr(mod, "_is_postgres", lambda session: True)

    # 2) Create miner with very small window generation
    pg_cfg = PgTextMinerConfig(
        similarity_threshold=0.30,
        per_query_limit=50,
        max_windows_per_chunk=1,  # keep deterministic
        min_window_tokens=3,
        max_window_tokens=3,
        include_chunk_query=True,  # will use nq.strict as single query
        use_alias_value_similarity=False,
    )
    miner = PgTrgmTextMiner(_FakeSession(), cfg=pg_cfg)

    raw_text = "We measured ((R)-3-Hydroxybutanoyl)(n-2) in plasma."

    # 3) Force a single chunk that covers whole text (so spans can be found)
    monkeypatch.setattr(
        miner,
        "_iter_chunks",
        lambda text, cfg: [(0, len(text), text)],
    )

    # 4) Patch pg query to return deterministic fake rows
    # IMPORTANT: pg_trgm_score is 0..1; miner converts to 0..100
    fake_rows = [
        _row(
            alias_id=123,
            entity_id=195576,
            group_id=10,
            alias_value="((R)-3-Hydroxybutanoyl)(n-2)",
            alias_norm="((r)-3-hydroxybutanoyl)(n-2)",
            alias_type="label",
            xref_source="CheBI",
            is_primary=False,
            locale="en",
            data_source_id=39,
            etl_package_id=36,
            primary_name="CHEBI:3",
            pg_trgm_score=1.0,
        )
    ]

    monkeypatch.setattr(miner, "_pg_trgm_query", lambda qwin, cfg: list(fake_rows))

    # 5) Execute
    cfg = TextMinerConfig(
        entity_type_hints=["Chemicals"],  # not used here (we patched query), but ok
        top_k=5,
        min_score=90.0,
        keep_ambiguous=True,
        longest_span_wins=True,
        dedup_by_entity_id=False,
        chunk_size=9999,
        chunk_overlap=0,
    )

    res = miner.extract_mentions(raw_text, config=cfg)

    assert res.status == "ok"
    assert res.backend == "pg_trgm"
    assert len(res.mentions) >= 1

    m = res.mentions[0]
    m.finalize()

    assert m.best is not None
    assert m.best.entity_id == 195576
    assert m.best.group_id == 10
    assert m.best.method == "pg_trgm"
    assert m.best.score == 100.0  # 1.0 * 100

    # Span should match the visible alias_value (case-insensitive substring search)
    assert "Hydroxybutanoyl" in m.span.text
    assert m.span.start >= 0
    assert m.span.end <= len(raw_text)

    # Meta audit fields
    assert m.best.meta.get("alias_value") == "((R)-3-Hydroxybutanoyl)(n-2)"
    assert m.best.meta.get("alias_norm") == "((r)-3-hydroxybutanoyl)(n-2)"
    assert m.best.meta.get("pg_trgm_score") == 1.0


def test_pg_text_miner_fallback_span_is_chunk_when_no_substring(monkeypatch):
    import biofilter.modules.search.text_miner_pg as mod

    monkeypatch.setattr(mod, "_is_postgres", lambda session: True)

    miner = PgTrgmTextMiner(
        _FakeSession(),
        cfg=PgTextMinerConfig(max_windows_per_chunk=1, include_chunk_query=True),
    )

    raw_text = "No alias appears here."

    monkeypatch.setattr(
        miner,
        "_iter_chunks",
        lambda text, cfg: [(0, len(text), text)],
    )

    # Alias values that won't be found => miner will fallback to chunk span
    fake_rows = [
        _row(
            alias_id=1,
            entity_id=999,
            group_id=10,
            alias_value="NOT_IN_TEXT",
            alias_norm="not_in_text",
            alias_type="label",
            xref_source="CheBI",
            is_primary=False,
            locale="en",
            data_source_id=39,
            etl_package_id=36,
            primary_name=None,
            pg_trgm_score=0.9,
        )
    ]
    monkeypatch.setattr(miner, "_pg_trgm_query", lambda qwin, cfg: list(fake_rows))

    res = miner.extract_mentions(raw_text)

    assert res.status == "ok"
    assert len(res.mentions) == 1

    m = res.mentions[0].finalize()
    assert m.span.start == 0
    assert m.span.end == len(raw_text)
    assert m.best is not None
    assert m.best.entity_id == 999
