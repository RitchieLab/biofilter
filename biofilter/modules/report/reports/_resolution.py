"""
Turning what a user typed into entities.

Shared by the reports that take names rather than ids. `resolve_entity`
is the whole of one; `expand_entity_neighborhood` does it as a first step.
Keeping one implementation matters most for fuzzy matching, where two
copies would score differently and nobody would notice.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional, Sequence

MATCH_MODES = ("exact", "like", "fuzzy")
DEFAULT_SIMILARITY_THRESHOLD = 80.0

#: The alias column an input is compared against. Pre-normalised in the
#: bundle, so exact matching is an equality join rather than a `lower()`
#: wrapped around a column, which would cost the parquet statistics.
ALIAS_KEY = "lower(coalesce(a.alias_norm, a.alias_value))"

#: Which alias wins when an input matches several of the same entity.
ALIAS_RANK = """
    CASE
        WHEN a.is_primary THEN 0
        WHEN lower(a.alias_type) IN ('symbol', 'preferred') THEN 1
        WHEN lower(a.alias_type) = 'code' THEN 2
        WHEN lower(a.alias_type) IN ('synonym', 'name') THEN 3
        ELSE 4
    END
"""

#: Convenient spellings for entity groups, beyond the group's own name.
#: The group names themselves are read from the bundle, so this only has
#: to carry what a person would plausibly type instead.
GROUP_SYNONYMS = {
    "gene": "genes",
    "disease": "diseases",
    "chemical": "chemicals",
    "pathway": "pathways",
    "protein": "proteins",
    "variant": "variants",
    # `go` is deliberately absent. It is the prefix of every GO
    # identifier, so accepting it as a hint would turn `GO:0006915` into
    # the term `0006915` — which is exactly how the relational version
    # returned a disease when asked for a GO term.
    "go_terms": "gene ontology",
    "goterms": "gene ontology",
    "phenotype": "phenotypes",
    "cell_type": "cell types",
}


def validate_match_mode(mode: Any) -> str:
    resolved = str(mode or "exact").lower()
    if resolved not in MATCH_MODES:
        raise ValueError(f"match_mode must be one of {MATCH_MODES}. Got: {mode!r}")
    return resolved


def match_clause(mode: str, threshold: float) -> tuple[str, str]:
    """
    The join condition and the score column, for one match mode.

    Returns `(join_on, score_expression)`, both referring to alias `a`
    and the registered input relation `i`.
    """
    if mode == "exact":
        return f"{ALIAS_KEY} = i.input_value_norm", "CAST(NULL AS DOUBLE)"

    if mode == "like":
        # One direction: the input occurs somewhere in the alias.
        #
        # Not the reverse. `contains(input, alias)` would make every
        # one-character alias match every input that contains that
        # character — asking for BRCA1 returned the aliases "1" and "a1"
        # while this was symmetric.
        return f"contains({ALIAS_KEY}, i.input_value_norm)", "CAST(NULL AS DOUBLE)"

    # Scored in the join, so only rows above the threshold are ever built.
    # The relational version scored every alias in the bundle in Python.
    similarity = f"jaro_winkler_similarity({ALIAS_KEY}, i.input_value_norm) * 100"
    return f"{similarity} >= {threshold}", similarity


def split_type_hint(
    value: str, known_hints: Iterable[str]
) -> tuple[Optional[str], str]:
    """
    Split `gene:BRCA1` into a type hint and a term.

    A prefix counts as a hint **only when it names an entity group**.
    Anything else stays part of the term, which is what makes
    `GO:0006915`, `MONDO:0007254` and `HGNC:11998` survive: the
    relational version treated every prefix as a hint, so those became
    the terms `0006915`, `0007254` and `11998` — and `0006915` resolved
    to a disease. A GO id silently returned a disease.
    """
    text = str(value).strip()
    if ":" not in text:
        return None, text

    prefix, rest = text.split(":", 1)
    hint = prefix.strip().lower()
    term = rest.strip()
    if hint in set(known_hints) and term:
        return hint, term
    return None, text


def hint_to_group(hint: Optional[str]) -> Optional[str]:
    """The entity group a hint names, lowercased, or None."""
    if not hint:
        return None
    return GROUP_SYNONYMS.get(hint, hint)
