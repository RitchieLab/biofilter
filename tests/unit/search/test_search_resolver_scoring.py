from biofilter.modules.search.resolver import TermResolver
from biofilter.modules.search.types import Candidate


def fake_retriever(query, pool_limit, entity_type_hints):
    # Simulate DB pool of aliases
    return [
        Candidate(
            entity_id=1,
            entity_type="Disease",
            primary_name="Alzheimer disease",
            matched_name="Alzheimer disease",
            method="exact_name",
            meta={"is_primary_name": True},
        ),
        Candidate(
            entity_id=2,
            entity_type="Disease",
            primary_name="Alzheimer's disease",
            matched_name="Alzheimer's disease",
            method="db_pool",
        ),
        Candidate(
            entity_id=3,
            entity_type="Gene",
            primary_name="ALZ",
            matched_name="ALZ",
            method="db_pool",
        ),
    ]


def test_resolve_best_exact():
    r = TermResolver(fake_retriever)
    res = r.resolve_best("Alzheimer disease", entity_type_hints=["Disease"])
    assert res.status == "resolved"
    assert res.best is not None
    assert res.best.entity_id == 1
    assert res.best.score >= 90
