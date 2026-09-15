"""
A bundle small enough to commit, shaped like a real one.

Every report migrated to the native module tests against this. It stays
deliberately small — three genes and a handful of variants — but it
carries the structure that makes gene annotation interesting: a gene
with no build-38 location, an alias that resolves case-insensitively,
relationships pointing both ways, and a gene whose range contains
variants.

It also reproduces on purpose the trap that cost us 2.4 billion
invisible rows: a partitioned table with an empty same-named parent file
beside it. A reader that scans the directory picks the wrong one; a
reader that reads the manifest cannot.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# Entity ids. Genes are 1-3; the neighbours they relate to are 10 and 20.
TP53, BRCA1, NOLOC = 1, 2, 3
PROTEIN, PATHWAY = 10, 20

GROUP_GENES, GROUP_PROTEINS, GROUP_PATHWAYS = 1, 2, 3


def _t(**columns) -> pa.Table:
    return pa.table(columns)


def _tables() -> dict[str, pa.Table]:
    """The core tables, as a bundle spells them."""
    return {
        "entity_groups": _t(
            id=pa.array([GROUP_GENES, GROUP_PROTEINS, GROUP_PATHWAYS], pa.int64()),
            name=pa.array(["Genes", "Proteins", "Pathways"]),
            description=pa.array([None, None, None], pa.string()),
        ),
        "entities": _t(
            id=pa.array([TP53, BRCA1, NOLOC, PROTEIN, PATHWAY], pa.int64()),
            group_id=pa.array(
                [GROUP_GENES, GROUP_GENES, GROUP_GENES, GROUP_PROTEINS, GROUP_PATHWAYS],
                pa.int64(),
            ),
            has_conflict=pa.array([False] * 5),
            is_active=pa.array([True] * 5),
        ),
        "omic_status": _t(
            id=pa.array([1], pa.int64()),
            name=pa.array(["active"]),
            description=pa.array([None], pa.string()),
        ),
        "gene_locus_groups": _t(
            id=pa.array([1], pa.int64()),
            name=pa.array(["protein-coding gene"]),
            description=pa.array([None], pa.string()),
        ),
        "gene_locus_types": _t(
            id=pa.array([1], pa.int64()),
            name=pa.array(["gene with protein product"]),
            description=pa.array([None], pa.string()),
        ),
        "gene_masters": _t(
            id=pa.array([101, 102, 103], pa.int64()),
            entity_id=pa.array([TP53, BRCA1, NOLOC], pa.int64()),
            symbol=pa.array(["TP53", "BRCA1", "NOLOC1"]),
            hgnc_status=pa.array(["Approved", "Approved", "Approved"]),
            omic_status_id=pa.array([1, 1, 1], pa.int64()),
            locus_group_id=pa.array([1, 1, 1], pa.int64()),
            locus_type_id=pa.array([1, 1, 1], pa.int64()),
            chromosome=pa.array([17, 17, 22], pa.int32()),
        ),
        "gene_groups": _t(
            id=pa.array([1, 2], pa.int64()),
            name=pa.array(["p53 family", "BRCA1 A complex"]),
            description=pa.array([None, None], pa.string()),
        ),
        "gene_group_memberships": _t(
            gene_id=pa.array([101, 102], pa.int64()),
            group_id=pa.array([1, 2], pa.int64()),
        ),
        "entity_aliases": _t(
            id=pa.array(list(range(1, 11)), pa.int64()),
            entity_id=pa.array(
                [TP53, TP53, TP53, TP53, TP53, BRCA1, BRCA1, BRCA1, NOLOC, NOLOC],
                pa.int64(),
            ),
            alias_value=pa.array(
                [
                    "TP53", "HGNC:11998", "ENSG00000141510", "7157", "p53",
                    "BRCA1", "HGNC:1100", "ENSG00000012048",
                    "NOLOC1", "no-location gene",
                ]
            ),
            alias_norm=pa.array(
                [
                    "tp53", "hgnc:11998", "ensg00000141510", "7157", "p53",
                    "brca1", "hgnc:1100", "ensg00000012048",
                    "noloc1", "no-location gene",
                ]
            ),
            alias_type=pa.array(
                [
                    "symbol", "code", "code", "code", "synonym",
                    "symbol", "code", "code",
                    "symbol", "name",
                ]
            ),
            xref_source=pa.array(
                [
                    "HGNC", "HGNC", "ENSEMBL", "ENTREZ", "HGNC",
                    "HGNC", "HGNC", "ENSEMBL",
                    "HGNC", "HGNC",
                ]
            ),
            is_primary=pa.array(
                [True, False, False, False, False,
                 True, False, False,
                 True, False]
            ),
            is_active=pa.array([True] * 10),
        ),
        # NOLOC deliberately has none: it is the `partial` case.
        "entity_locations": _t(
            id=pa.array([1, 2], pa.int64()),
            entity_id=pa.array([TP53, BRCA1], pa.int64()),
            build=pa.array([38, 38], pa.int32()),
            chromosome=pa.array([17, 17], pa.int32()),
            start_pos=pa.array([100, 5000], pa.int64()),
            end_pos=pa.array([400, 5400], pa.int64()),
        ),
        # TP53 on the left once and on the right once, so the report has
        # to count both directions.
        "entity_relationships": _t(
            id=pa.array([1, 2, 3], pa.int64()),
            entity_1_id=pa.array([TP53, TP53, PATHWAY], pa.int64()),
            entity_1_group_id=pa.array(
                [GROUP_GENES, GROUP_GENES, GROUP_PATHWAYS], pa.int64()
            ),
            entity_2_id=pa.array([PROTEIN, PATHWAY, TP53], pa.int64()),
            entity_2_group_id=pa.array(
                [GROUP_PROTEINS, GROUP_PATHWAYS, GROUP_GENES], pa.int64()
            ),
            relationship_type_id=pa.array([1, 1, 1], pa.int64()),
        ),
    }


def _variant_partitions() -> dict[int, pa.Table]:
    """Three variants inside TP53's range (100-400), two elsewhere."""
    return {
        17: _t(
            chromosome=pa.array([17] * 4, pa.int32()),
            position=pa.array([150, 200, 300, 9000], pa.int64()),
            reference_allele=pa.array(["A"] * 4),
            alternate_allele=pa.array(["G"] * 4),
            variant_key=pa.array([f"17:{p}:A:G" for p in (150, 200, 300, 9000)]),
        ),
        22: _t(
            chromosome=pa.array([22], pa.int32()),
            position=pa.array([777], pa.int64()),
            reference_allele=pa.array(["C"]),
            alternate_allele=pa.array(["T"]),
            variant_key=pa.array(["22:777:C:T"]),
        ),
    }


@pytest.fixture
def fixture_bundle(tmp_path: Path) -> Path:
    root = tmp_path / "20260101"
    tables_dir = root / "tables"
    entries: list[dict] = []

    def write(rel: str, data: pa.Table, **entry) -> None:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(data, path)
        entries.append(
            {
                "file": rel,
                "rows": data.num_rows,
                "bytes": path.stat().st_size,
                **entry,
            }
        )

    for name, table in _tables().items():
        write(f"tables/{name}.parquet", table, name=name, branch="core")

    for chrom, table in _variant_partitions().items():
        write(
            f"tables/variant_masters/variant_masters_chr{chrom}.parquet",
            table,
            name=f"variant_masters_chr{chrom}",
            table="variant_masters",
            branch="variant",
        )

    # Undeclared, empty, wrong schema — the shadowing parent.
    pq.write_table(
        pa.table(
            {
                "variant_id": pa.array([], pa.int64()),
                "position_start": pa.array([], pa.int64()),
            }
        ),
        tables_dir / "variant_masters.parquet",
    )

    manifest = {
        "manifest_version": 2,
        "biofilter_version": "4.3.0",
        "schema_version": "4.3.0",
        "format": "parquet",
        "created_at": "2026-01-01T00:00:00+00:00",
        "bundle_id": "fixturebundle0001",
        "tables": entries,
    }
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2))
    (root / "build_record.json").write_text(
        json.dumps({"built_at": "2026-01-01T00:00:00+00:00", "steps": []})
    )
    return root
