"""
The bundle id has to identify content, not a run.

ADR-003 makes a bundle an immutable release that cannot be rebuilt once
its sources move on, so the id's job is to say *which* data an answer came
from and to be recomputable from the bundle itself. That only holds if it
ignores everything about the execution — the clock above all.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from biofilter.modules.bundle.builder import BundleBuilder


def _fingerprint(manifest: dict) -> str:
    """Mirror of the builder's derivation, over a manifest dict."""
    payload = json.dumps(
        sorted(
            (t.get("name"), t.get("rows"), t.get("bytes"))
            for t in manifest["tables"]
            if t.get("name") != "biofilter_metadata"
        ),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


@pytest.fixture
def manifest():
    return {
        "created_at": "2026-09-09T20:00:00+00:00",
        "tables": [
            {"name": "entities", "rows": 100, "bytes": 2048},
            {"name": "gene_masters", "rows": 50, "bytes": 1024},
            {"name": "biofilter_metadata", "rows": 1, "bytes": 2343},
        ],
    }


def test_same_content_yields_the_same_id(manifest):
    """Two builds of identical data must agree, whenever they ran."""
    later = dict(manifest, created_at="2027-01-01T00:00:00+00:00")
    assert _fingerprint(manifest) == _fingerprint(later)


def test_changed_row_count_changes_the_id(manifest):
    changed = json.loads(json.dumps(manifest))
    changed["tables"][0]["rows"] = 101
    assert _fingerprint(changed) != _fingerprint(manifest)


def test_table_order_does_not_change_the_id(manifest):
    shuffled = dict(manifest, tables=list(reversed(manifest["tables"])))
    assert _fingerprint(shuffled) == _fingerprint(manifest)


def test_metadata_table_is_excluded(manifest):
    """
    `biofilter_metadata` is rewritten to carry the id, so counting it
    would make the id depend on its own value and stop it being
    recomputable from the finished bundle.
    """
    changed = json.loads(json.dumps(manifest))
    for table in changed["tables"]:
        if table["name"] == "biofilter_metadata":
            table["bytes"] = 9999
    assert _fingerprint(changed) == _fingerprint(manifest)


def test_table_name_maps_to_its_partitioned_file():
    """
    A bundle groups per-chromosome files under a directory named for the
    table; the mapping back is what the manifest records.
    """
    assert BundleBuilder._table_of("variant_masters_chr21") == "variant_masters"
    assert BundleBuilder._table_of("variant_rsid_chr1") == "variant_rsid"
    # Not a chromosome suffix: the name stands on its own.
    assert BundleBuilder._table_of("variant_gwas") == "variant_gwas"
    assert BundleBuilder._table_of("entity_aliases") == "entity_aliases"
