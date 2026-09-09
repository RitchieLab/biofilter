"""
Guard the invariant the parallel branch design rests on.

ADR-003 §2.3: no variant table may reference an entity surrogate. The
variant branch writes parquet without a database, in parallel with the
core branch that is still assigning those ids, so a variant table holding
an `entity_id` could not be built independently — the two branches would
have to be ordered, and the whole shape of the build would change.

The invariant holds today by construction rather than by enforcement
(`variant_masters` carries the comment `# Provenance (no FK)`), which is
exactly the kind of thing that erodes. These tests make it fail loudly.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine

from biofilter.modules.db.base import Base
from biofilter.utils.db_loader import register_imperative_tables

# Columns that would tie a variant row to an id minted by the core branch.
ENTITY_SURROGATES = {"entity_id", "gene_entity_id", "protein_entity_id"}

# Tables the variant branch produces, and the ones it depends on for
# lookups. `variant_gwas*` are included: they are associations rather than
# variants, but they ship in the variant branch and must stay independent
# the same way.
VARIANT_TABLE_PREFIX = "variant_"


@pytest.fixture(scope="module")
def variant_tables():
    """Every variant table, with the imperative ones mapped in."""
    engine = create_engine("sqlite://")
    register_imperative_tables(engine)
    return {
        name: table
        for name, table in Base.metadata.tables.items()
        if name.startswith(VARIANT_TABLE_PREFIX)
    }


def test_variant_tables_are_mapped(variant_tables):
    """Guard the guard: an empty set would make the checks below vacuous."""
    assert "variant_masters" in variant_tables
    assert "variant_molecular_effects" in variant_tables


def test_no_variant_table_declares_an_entity_column(variant_tables):
    offenders = {
        name: sorted(ENTITY_SURROGATES & {c.name for c in table.columns})
        for name, table in variant_tables.items()
        if ENTITY_SURROGATES & {c.name for c in table.columns}
    }
    assert not offenders, (
        f"Variant tables carrying an entity surrogate: {offenders}. "
        f"ADR-003 §2.3 keeps the variant branch independent of the core "
        f"branch; an entity id can only be resolved after the core branch "
        f"has minted it, which would force the two to be ordered. Link to "
        f"genes by natural key (HGNC_ID, gene_symbol) instead."
    )


def test_no_variant_table_has_a_foreign_key_outside_the_variant_branch(
    variant_tables,
):
    offenders = {}
    for name, table in variant_tables.items():
        outside = sorted(
            f"{fk.parent.name} -> {fk.target_fullname}"
            for fk in table.foreign_keys
            if not fk.column.table.name.startswith(VARIANT_TABLE_PREFIX)
        )
        if outside:
            offenders[name] = outside

    assert not offenders, (
        f"Variant tables with foreign keys into non-variant tables: "
        f"{offenders}. The variant branch is built with no database "
        f"available, so a constraint against a core table cannot be "
        f"satisfied at write time and makes the branch dependent on the "
        f"core branch having run."
    )
