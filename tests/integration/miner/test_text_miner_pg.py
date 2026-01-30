"""

"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import select, text as sql_text
from sqlalchemy.orm import Session

from biofilter import Biofilter
from biofilter.modules.db.models.model_entities import Entity, EntityAlias
from biofilter.modules.search.text_miner import TextMinerConfig
from biofilter.modules.search.text_miner_pg import PgTextMinerConfig, PgTrgmTextMiner

pytestmark = pytest.mark.integration


def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(
            f"Missing env var {name}. "
            f"Example: export {name}='postgresql+psycopg2://user:pass@host:5432/dbname'"
        )
    return val


@pytest.fixture(scope="session")
def biofilter_prod() -> Biofilter:
    """
    Integration Biofilter instance pointing to a real Postgres database.
    """
    # db_uri = _require_env("BIOFILTER_DB_URI")
    db_uri = "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_prod"  # TODO: Passar para o conftest

    bf = Biofilter(db_uri=db_uri, debug_mode=False)
    try:
        yield bf
    finally:
        bf.db.close()


@pytest.fixture(scope="session")
def prod_session(biofilter_prod: Biofilter) -> Session:
    """
    Real SQLAlchemy Session from Biofilter DB component.
    """
    return biofilter_prod.db.get_session()


@pytest.fixture(scope="session")
def pg_miner(prod_session: Session) -> PgTrgmTextMiner:
    """
    Postgres-backed miner. Skips if not running on Postgres.
    """
    dialect = getattr(getattr(prod_session, "bind", None), "dialect", None)
    if not dialect or dialect.name != "postgresql":
        pytest.skip("PgTrgmTextMiner integration tests require PostgreSQL.")

    # Optional: ensure pg_trgm is available (will raise if extension missing)
    try:
        prod_session.execute(sql_text("SELECT similarity('abc', 'abc')"))
    except Exception:
        pytest.skip("pg_trgm extension not available (similarity() not found).")

    miner_cfg = PgTextMinerConfig(
        similarity_threshold=0.30,
        per_query_limit=200,
        max_windows_per_chunk=60,
        min_window_tokens=1,
        max_window_tokens=3,
        include_chunk_query=False,
        use_alias_value_similarity=False,
    )
    return PgTrgmTextMiner(prod_session, cfg=miner_cfg)


def _pick_alias(
    session: Session,
    *,
    group_id: int,
    alias_type: str | None = None,
    xref_source: str | None = None,
    locale: str | None = "en",
) -> EntityAlias:
    """
    Pick one alias row from DB that is active and belongs to an active entity.
    We prefer short-ish alias_value to avoid chunk boundary issues.
    """
    filters = [
        EntityAlias.group_id == group_id,
        EntityAlias.is_active.is_(True),
        Entity.is_active.is_(True),
        EntityAlias.alias_value.isnot(None),
        EntityAlias.alias_value != "",
    ]
    if locale:
        filters.append(EntityAlias.locale == locale)
    if alias_type:
        filters.append(EntityAlias.alias_type == alias_type)
    if xref_source:
        filters.append(EntityAlias.xref_source == xref_source)

    stmt = (
        select(EntityAlias)
        .join(Entity, Entity.id == EntityAlias.entity_id)
        .where(*filters)
        .order_by(EntityAlias.is_primary.desc(), EntityAlias.id.asc())
        .limit(1)
    )

    row = session.execute(stmt).scalars().first()
    if not row:
        pytest.skip(
            f"No suitable alias found for group_id={group_id}, "
            f"alias_type={alias_type}, xref_source={xref_source}, locale={locale}."
        )
    return row


def _find_best_by_alias_id(mentions, alias_id: int):
    for m in mentions:
        m.finalize()
        if m.best and m.best.matched_name_id == alias_id:
            return m
    return None


def test_text_miner_pg_empty(pg_miner: PgTrgmTextMiner):
    res = pg_miner.extract_mentions("   \n\t")
    assert res.status == "empty"
    assert res.backend == "pg_trgm"
    assert res.mentions == []


def test_text_miner_pg_finds_gene_symbol_exact(pg_miner: PgTrgmTextMiner, prod_session: Session):
    # Genes group_id appears to be 2 in your environment; keep this consistent with your EntityGroup table.
    # If this changes, either update the test or resolve it dynamically (optional enhancement).
    gene_alias = _pick_alias(prod_session, group_id=2, alias_type="symbol")

    raw = gene_alias.alias_value
    assert raw is not None and raw.strip()

    text_doc = f"This paper discusses the gene {raw} in multiple assays."
    res = pg_miner.extract_mentions(
        text_doc,
        config=TextMinerConfig(
            entity_type_hints=["2"],  # restrict to Genes by group id
            top_k=5,
            min_score=90.0,
            keep_ambiguous=True,
            longest_span_wins=True,
            dedup_by_entity_id=False,
        ),
    )

    assert res.status == "ok"
    assert len(res.mentions) >= 1

    m = _find_best_by_alias_id(res.mentions, int(gene_alias.id))
    assert m is not None, "Expected to recover the inserted gene alias as a mention."

    # Span should include the alias_value (case-insensitive best-effort match)
    assert raw.lower() in m.span.text.lower()

    # Best candidate should be grounded to the correct entity
    assert m.best is not None
    assert m.best.entity_id == int(gene_alias.entity_id)
    assert m.best.group_id == int(gene_alias.group_id)
    # assert m.best.method == "pg_trgm"
    assert m.best.method in ("exact_name", "exact_normalized", "pg_trgm")
    assert m.best.score >= 90.0
    assert m.best.meta.get("alias_value") == gene_alias.alias_value
    assert m.best.meta.get("alias_norm") == gene_alias.alias_norm


def test_text_miner_pg_finds_chemical_label_exact(pg_miner: PgTrgmTextMiner, prod_session: Session):
    # Chemicals group_id appears to be 10 in your environment (CheBI labels).
    chem_alias = _pick_alias(prod_session, group_id=10, alias_type="label", xref_source="CheBI")

    raw = chem_alias.alias_value
    assert raw is not None and raw.strip()

    text_doc = f"We quantified {raw} using LC-MS/MS in serum samples."
    res = pg_miner.extract_mentions(
        text_doc,
        config=TextMinerConfig(
            entity_type_hints=["10"],  # restrict to Chemicals by group id
            top_k=5,
            min_score=85.0,            # chemicals can be noisier; keep slightly lower
            keep_ambiguous=True,
            longest_span_wins=True,
            dedup_by_entity_id=False,
        ),
    )

    assert res.status == "ok"
    assert len(res.mentions) >= 1

    m = _find_best_by_alias_id(res.mentions, int(chem_alias.id))
    assert m is not None, "Expected to recover the inserted chemical alias as a mention."

    assert raw.lower() in m.span.text.lower()

    assert m.best is not None
    assert m.best.entity_id == int(chem_alias.entity_id)
    assert m.best.group_id == int(chem_alias.group_id)
    assert m.best.method == "pg_trgm"
    assert m.best.score >= 70.0  # similarity can be < 0.9 depending on alias format
    assert m.best.meta.get("xref_source") == "CheBI"


def test_text_miner_pg_dedup_by_entity_id(pg_miner: PgTrgmTextMiner, prod_session: Session):
    gene_alias = _pick_alias(prod_session, group_id=2, alias_type="symbol")
    raw = gene_alias.alias_value
    assert raw is not None and raw.strip()

    # Repeat the same mention multiple times; dedup should keep only one mention per entity_id
    text_doc = f"{raw} is important. Later we mention {raw} again. And again: {raw}."
    res = pg_miner.extract_mentions(
        text_doc,
        config=TextMinerConfig(
            entity_type_hints=["2"],
            top_k=5,
            keep_ambiguous=True,
            dedup_by_entity_id=True,
            longest_span_wins=True,
        ),
    )

    assert res.status == "ok"
    # With dedup_by_entity_id=True we expect at most 1 mention for that entity
    hits = []
    for m in res.mentions:
        m.finalize()
        if m.best and m.best.entity_id == int(gene_alias.entity_id):
            hits.append(m)
    assert len(hits) <= 1
