# tests/integration/conftest.py
from __future__ import annotations

import os
import pytest

from biofilter import Biofilter
from biofilter.modules.search.resolver import TermResolver, ResolverConfig
from biofilter.modules.search.db_retriever import make_entity_alias_retriever, DBRetrieverConfig

pytestmark = pytest.mark.integration


def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(
            f"Missing env var {name}. "
            f"Example: export {name}='postgresql+psycopg2://user:pass@host:5432/dbname'"
        )
    return val

def _get_db_uri() -> str:
    # Prefer env var, fallback to local dev if not set
    return os.getenv("BIOFILTER_DB_URI") or "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_prod"


@pytest.fixture(scope="session")
def biofilter_prod() -> Biofilter:
    """
    Integration Biofilter instance pointing to a real database.
    Configure BIOFILTER_DB_URI to point to prod/staging.
    """
    # db_uri = _require_env("BIOFILTER_DB_URI")

    bf = Biofilter(db_uri=_get_db_uri(), debug_mode=False)

    try:
        yield bf
    finally:
        bf.db.close()


@pytest.fixture(scope="session")
def resolver(biofilter_prod: Biofilter) -> TermResolver:
    session = biofilter_prod.db.get_session()

    retriever = make_entity_alias_retriever(
        session,
        cfg=DBRetrieverConfig(
            locale="en",
            pool_limit=1500,
            exact_limit=300,
            prefer_primary_first=True,
            require_active_alias=True,
            require_active_entity=True,
            # pg_trgm layer
            pgtrgm_enabled=True,
            pgtrgm_min_score=0.30,
            pgtrgm_stop_score=0.99,
            pgtrgm_limit=500,
        ),
    )

    return TermResolver(
        retriever,
        config=ResolverConfig(
            pool_limit=1500,
            top_k=20,
            min_score=90.0,
            min_delta=5.0,
            enable_fuzzy_fallback=True,
        ),
    )