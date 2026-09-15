"""
What the build must refuse to publish, and what it must not leave behind.

Both guards here exist because the `20260910` bundle was published
without them. `alphamissense` and `gtex_v10_eqtl` were in the plan, were
recorded as done, contributed no table, and nothing said so; and every
partitioned variant table shipped an empty same-named parent file beside
the directory holding its rows, so reports read zeroes.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from biofilter.modules.bundle.builder import BundleBuilder


class _Logger:
    def __init__(self):
        self.lines = []

    def log(self, message, level="INFO"):
        self.lines.append((level, message))


@pytest.fixture
def builder():
    obj = BundleBuilder.__new__(BundleBuilder)
    obj.logger = _Logger()
    return obj


# ---------------------------------------------------------------------------
# A planned source that produced nothing
# ---------------------------------------------------------------------------


def test_a_source_that_moved_no_file_is_named(builder):
    builder._moved_by_source = {
        "gnomad_joint": [Path("tables/variant_masters/variant_masters_chr1.parquet")],
        "alphamissense": [],
        "gtex_v10_eqtl": [],
    }
    assert builder._sources_without_output([]) == ["alphamissense", "gtex_v10_eqtl"]


def test_every_source_contributing_leaves_nothing_to_report(builder):
    builder._moved_by_source = {
        "gnomad_joint": [Path("a.parquet")],
        "alphamissense": [Path("b.parquet")],
    }
    assert builder._sources_without_output([]) == []


# ---------------------------------------------------------------------------
# Parent stubs
# ---------------------------------------------------------------------------


def test_the_stub_beside_a_partition_directory_is_dropped(builder, tmp_path):
    out = tmp_path / "bundle"
    tables = out / "tables"
    (tables / "variant_masters").mkdir(parents=True)

    child = tables / "variant_masters" / "variant_masters_chr21.parquet"
    child.write_bytes(b"PAR1payload")
    stub = tables / "variant_masters.parquet"
    stub.write_bytes(b"PAR1")
    kept = tables / "entities.parquet"
    kept.write_bytes(b"PAR1")

    (out / "manifest.json").write_text(json.dumps({"tables": [
        {"name": "variant_masters", "rows": 0, "file": "tables/variant_masters.parquet"},
        {"name": "entities", "rows": 12, "file": "tables/entities.parquet"},
    ]}))

    builder._drop_parent_stubs(out, tables, [child])

    assert not stub.exists()
    assert kept.exists()
    assert child.exists()

    manifest = json.loads((out / "manifest.json").read_text())
    assert [t["name"] for t in manifest["tables"]] == ["entities"]


def test_a_table_with_no_partition_directory_keeps_its_file(builder, tmp_path):
    """Only the tables the variant branch actually partitioned are stubs."""
    out = tmp_path / "bundle"
    tables = out / "tables"
    (tables / "variant_gtex").mkdir(parents=True)
    child = tables / "variant_gtex" / "variant_gtex_chr1.parquet"
    child.write_bytes(b"PAR1")

    lonely = tables / "chemical_masters.parquet"
    lonely.write_bytes(b"PAR1")

    (out / "manifest.json").write_text(json.dumps({"tables": [
        {"name": "chemical_masters", "rows": 0, "file": "tables/chemical_masters.parquet"},
    ]}))

    builder._drop_parent_stubs(out, tables, [child])

    assert lonely.exists()
    manifest = json.loads((out / "manifest.json").read_text())
    assert [t["name"] for t in manifest["tables"]] == ["chemical_masters"]


# ---------------------------------------------------------------------------
# What a move leaves behind
# ---------------------------------------------------------------------------


def test_pruning_removes_the_directories_a_move_emptied(tmp_path):
    """
    An empty `predictions/` left under `processed/AlphaMissense/
    alphamissense/` is what made the ETL's skip check read the source as
    still having output, so its transform was skipped as not-applicable
    and the next bundle lost the table.
    """
    src = tmp_path / "processed" / "AlphaMissense" / "alphamissense"
    (src / "predictions").mkdir(parents=True)

    BundleBuilder._prune_empty_dirs(src)

    assert not src.exists()


def test_pruning_keeps_a_directory_that_still_holds_a_file(tmp_path):
    src = tmp_path / "processed" / "GTEx" / "gtex_v10_eqtl"
    (src / "evidence").mkdir(parents=True)
    (src / "evidence" / "kept.parquet").write_bytes(b"PAR1")
    (src / "empty").mkdir()

    BundleBuilder._prune_empty_dirs(src)

    assert (src / "evidence" / "kept.parquet").exists()
    assert not (src / "empty").exists()


# ---------------------------------------------------------------------------
# Which table a file belongs to
# ---------------------------------------------------------------------------


def _stamped(path, table, source):
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([pa.field("chromosome", pa.int32())]).with_metadata({
        b"biofilter_table": table.encode(),
        b"biofilter_source": source.encode(),
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"chromosome": [21]}, schema=schema), path)
    return path


def test_the_table_comes_from_the_footer_not_the_name(tmp_path):
    """
    `variant_masters_gnomad_chr21` cannot be split back into table and
    source by any rule — `variant_masters` and `variant_masters_gnomad`
    are both plausible. The footer settles it.
    """
    path = _stamped(
        tmp_path / "variant_masters_gnomad_chr21.parquet",
        "variant_masters",
        "gnomad",
    )
    assert BundleBuilder._table_for(path) == "variant_masters"


def test_an_unstamped_file_still_maps_by_name(tmp_path):
    """A processed/ tree written before the stamp existed must assemble."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "variant_rsid_chr21.parquet"
    pq.write_table(pa.table({"chromosome": [21]}), path)

    assert BundleBuilder._table_for(path) == "variant_rsid"


# ---------------------------------------------------------------------------
# Empty tables
# ---------------------------------------------------------------------------


def test_a_table_with_no_rows_is_left_out(builder, tmp_path):
    """
    Absence is the honest signal. A declared empty table is counted as
    present by `db verify` and returns zero rows to a report that joins
    it, which is indistinguishable from data saying nothing.
    """
    out = tmp_path / "bundle"
    tables = out / "tables"
    tables.mkdir(parents=True)
    for name in ("chemical_masters", "entities", "biofilter_metadata"):
        (tables / f"{name}.parquet").write_bytes(b"PAR1")

    (out / "manifest.json").write_text(json.dumps({"tables": [
        {"name": "chemical_masters", "rows": 0, "file": "tables/chemical_masters.parquet"},
        {"name": "entities", "rows": 203393, "file": "tables/entities.parquet"},
        {"name": "biofilter_metadata", "rows": 0, "file": "tables/biofilter_metadata.parquet"},
    ]}))

    builder._drop_empty_tables(out, tables)

    assert not (tables / "chemical_masters.parquet").exists()
    assert (tables / "entities.parquet").exists()
    # Exempt: it is rewritten afterwards to carry the bundle id, and a
    # bundle that cannot say what it is is not usable.
    assert (tables / "biofilter_metadata.parquet").exists()

    manifest = json.loads((out / "manifest.json").read_text())
    assert sorted(t["name"] for t in manifest["tables"]) == [
        "biofilter_metadata", "entities",
    ]


def test_a_bundle_with_no_empty_tables_is_untouched(builder, tmp_path):
    out = tmp_path / "bundle"
    tables = out / "tables"
    tables.mkdir(parents=True)
    (tables / "entities.parquet").write_bytes(b"PAR1")
    manifest = {"tables": [
        {"name": "entities", "rows": 12, "file": "tables/entities.parquet"},
    ]}
    (out / "manifest.json").write_text(json.dumps(manifest))

    builder._drop_empty_tables(out, tables)

    assert json.loads((out / "manifest.json").read_text()) == manifest


# ---------------------------------------------------------------------------
# Keeping processed/ for a second build
# ---------------------------------------------------------------------------


def _builder_with_source(tmp_path, *, keep_processed):
    obj = BundleBuilder.__new__(BundleBuilder)
    obj.logger = _Logger()
    obj.keep_processed = keep_processed
    obj.processed_path = tmp_path / "processed"
    obj.plan = {"branches": {"variant": {"sources": [
        {"name": "gnomad_joint_chr22", "source_system": "gnomAD", "include": True},
    ]}}}
    src = obj.processed_path / "gnomAD" / "gnomad_joint_chr22"
    src.mkdir(parents=True)
    _stamped(
        src / "variant_masters_gnomad_chr22.parquet",
        "variant_masters",
        "gnomad",
    )
    return obj, src


def test_keep_processed_leaves_the_source_files_in_place(tmp_path):
    """
    A chromosome-subset bundle built to develop against must not consume
    the parquet the eventual full bundle needs — that chromosome would
    have to be downloaded and transformed again.
    """
    builder, src = _builder_with_source(tmp_path, keep_processed=True)
    tables = tmp_path / "bundle" / "tables"
    tables.mkdir(parents=True)

    moved = builder._move_variant_tables(tables)

    assert len(moved) == 1
    assert (tables / "variant_masters" / "variant_masters_gnomad_chr22.parquet").is_file()
    assert (src / "variant_masters_gnomad_chr22.parquet").is_file()


def test_the_default_still_moves(tmp_path):
    """
    Moving is right for a full build: the variant parquet is most of a
    bundle's size and copying would need both copies on disk at once.
    """
    builder, src = _builder_with_source(tmp_path, keep_processed=False)
    tables = tmp_path / "bundle" / "tables"
    tables.mkdir(parents=True)

    moved = builder._move_variant_tables(tables)

    assert len(moved) == 1
    assert (tables / "variant_masters" / "variant_masters_gnomad_chr22.parquet").is_file()
    assert not src.exists()
