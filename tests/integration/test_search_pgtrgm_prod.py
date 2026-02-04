# tests/integration/test_search_pgtrgm_prod.py
from __future__ import annotations

import pytest

from biofilter.modules.search.resolver import TermResolver

pytestmark = pytest.mark.integration


def test_pgtrgm_layer_is_used_for_chemicals_search(resolver: TermResolver):
    """
    Validate that Postgres pg_trgm candidates are present in the ranked results.
    """
    results = resolver.search(
        "((R)-3-Hydroxybutanoyl)(n-2)",
        entity_type_hints=["Chemicals"],
        limit=20,
    )

    assert results, "Expected at least one candidate"

    methods = {c.method for c in results}
    assert "pg_trgm" in methods, f"Expected pg_trgm in methods, got: {methods}"

    # Optional: sanity-check group
    assert any(c.meta.get("group_id") == 10 for c in results)


def test_pgtrgm_perfect_match_resolves_best(resolver: TermResolver):
    """
    With a strong/near-perfect match, resolution should be resolved or ambiguous,
    and the best candidate should come from pg_trgm (method preserved).
    """
    res = resolver.resolve_best(
        # "((R)-3-Hydroxybutanoyl)(n-2)",
        "Phenazopyridine",
        entity_type_hints=["Chemicals"],
    )

    assert res.status in ("resolved", "ambiguous")
    assert res.best is not None

    # Requires the method-preservation fix in TermResolver
    assert res.best.method == "pg_trgm"

    # From db_retriever_pgtrgm meta
    assert res.best.meta.get("pg_trgm_score", 0.0) >= 0.90
    assert res.best.meta.get("group_id") == 10


def test_pgtrgm_partial_query_still_returns_candidates(resolver: TermResolver):
    """
    Partial chemical names should still return a candidate list containing pg_trgm hits.
    """
    results = resolver.search(
        "((R)-3-Hydroxybutanoyl)",
        entity_type_hints=["Chemicals"],
        limit=20,
    )

    assert results, "Expected candidates for partial query"
    assert any(c.method == "pg_trgm" for c in results), "Expected at least one pg_trgm candidate"
