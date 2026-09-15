"""
A bundle small enough to commit, shaped like a real one.

The fixture deliberately reproduces the trap that cost us 2.4 billion
invisible rows: a partitioned table that also has an empty same-named
parent file sitting beside it. A reader that scans the directory picks
the wrong one; a reader that reads the manifest cannot.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def _write(path: Path, table: pa.Table) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return {"rows": table.num_rows, "bytes": path.stat().st_size}


@pytest.fixture
def fixture_bundle(tmp_path: Path) -> Path:
    root = tmp_path / "20260101"
    tables = root / "tables"
    entries: list[dict] = []

    genes = pa.table(
        {
            "entity_id": pa.array([1, 2, 3], pa.int64()),
            "symbol": pa.array(["TP53", "BRCA1", "EGFR"]),
            "chromosome": pa.array([17, 17, 7], pa.int32()),
        }
    )
    meta = _write(tables / "gene_masters.parquet", genes)
    entries.append({"name": "gene_masters", "file": "tables/gene_masters.parquet",
                    "branch": "core", **meta})

    # Two chromosomes, so partition pruning has something to prune.
    for chrom, positions in ((21, [100, 200]), (22, [300, 400, 500])):
        variants = pa.table(
            {
                "chromosome": pa.array([chrom] * len(positions), pa.int32()),
                "position": pa.array(positions, pa.int64()),
                "variant_key": pa.array([f"{chrom}:{p}:A:G" for p in positions]),
                "rsid": pa.array([f"rs{chrom}{p}" for p in positions]),
            }
        )
        rel = f"tables/variant_masters/variant_masters_chr{chrom}.parquet"
        meta = _write(root / rel, variants)
        entries.append(
            {
                "name": f"variant_masters_chr{chrom}",
                "table": "variant_masters",
                "file": rel,
                "branch": "variant",
                **meta,
            }
        )

    # The trap: an empty parent beside the partition directory, with the
    # wrong schema, exactly as the 4.3.0 bundles carried it. It is NOT
    # declared in the manifest, so a manifest-driven reader ignores it.
    pq.write_table(
        pa.table({"variant_id": pa.array([], pa.int64()),
                  "position_start": pa.array([], pa.int64())}),
        tables / "variant_masters.parquet",
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
