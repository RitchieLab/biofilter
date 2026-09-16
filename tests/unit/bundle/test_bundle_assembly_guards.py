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


# ---------------------------------------------------------------------------
# Folding a later run into an existing bundle
# ---------------------------------------------------------------------------


def _bundle(tmp_path, entries):
    """A bundle with a manifest and the files it declares."""
    out = tmp_path / "bundle"
    (out / "tables").mkdir(parents=True)
    tables = []
    for name, table in entries:
        directory = out / "tables" / table
        directory.mkdir(exist_ok=True)
        path = directory / f"{name}.parquet"
        _stamped(path, table, "gnomad")
        tables.append({
            "name": name, "table": table, "branch": "variant",
            "rows": 1, "file": str(path.relative_to(out)),
            "bytes": path.stat().st_size,
        })
    (out / "manifest.json").write_text(json.dumps({
        "bundle_id": "0000000000000000", "tables": tables,
    }))
    return out


def _merging_builder(tmp_path, out, sources):
    builder = BundleBuilder.__new__(BundleBuilder)
    builder.logger = _Logger()
    builder.keep_processed = False
    builder.merge_into = out
    builder.processed_path = tmp_path / "processed"
    builder.plan = {"branches": {"variant": {"sources": [
        {"name": n, "source_system": "gnomAD", "include": True} for n in sources
    ]}}}
    for name in sources:
        (builder.processed_path / "gnomAD" / name).mkdir(parents=True)
    return builder


def test_a_stage_is_folded_into_the_bundle_it_targets(tmp_path, monkeypatch):
    """
    A genome runs in stages. Without this each stage either publishes a
    partial bundle or waits for all of them, which is the same as having
    no stages.
    """
    out = _bundle(tmp_path, [("variant_masters_gnomad_chr22", "variant_masters")])
    builder = _merging_builder(tmp_path, out, ["gnomad_joint_chr1"])
    _stamped(
        builder.processed_path / "gnomAD" / "gnomad_joint_chr1"
        / "variant_masters_gnomad_chr1.parquet",
        "variant_masters", "gnomad",
    )
    monkeypatch.setattr(builder, "_stamp_metadata", lambda *a: None)
    monkeypatch.setattr(builder, "_resync_manifest_entry", lambda *a: None)

    assert builder._merge() is True

    manifest = json.loads((out / "manifest.json").read_text())
    names = sorted(t["name"] for t in manifest["tables"])
    assert names == [
        "variant_masters_gnomad_chr1", "variant_masters_gnomad_chr22",
    ]
    # Derived from content, and the content grew.
    assert manifest["bundle_id"] != "0000000000000000"
    assert (out / "tables" / "variant_masters"
            / "variant_masters_gnomad_chr1.parquet").is_file()


def test_folding_the_same_chromosome_twice_is_refused(tmp_path):
    """
    Re-folding would double its rows, and nothing here can know which
    copy is the right one.
    """
    out = _bundle(tmp_path, [("variant_masters_gnomad_chr1", "variant_masters")])
    builder = _merging_builder(tmp_path, out, ["gnomad_joint_chr1"])
    source = (
        builder.processed_path / "gnomAD" / "gnomad_joint_chr1"
        / "variant_masters_gnomad_chr1.parquet"
    )
    _stamped(source, "variant_masters", "gnomad")

    assert builder._merge() is False
    # Checked before anything moves: a collision found halfway would
    # leave the bundle holding part of a run.
    assert source.is_file()
    manifest = json.loads((out / "manifest.json").read_text())
    assert len(manifest["tables"]) == 1
    assert manifest["bundle_id"] == "0000000000000000"


def test_a_directory_that_is_not_a_bundle_is_refused(tmp_path):
    empty = tmp_path / "somewhere"
    empty.mkdir()
    builder = _merging_builder(tmp_path, empty, ["gnomad_joint_chr1"])

    assert builder._merge() is False


def test_the_merge_is_recorded_beside_the_original_build(tmp_path, monkeypatch):
    """The id changes on every merge; the record is how the sequence is
    read back afterwards."""
    out = _bundle(tmp_path, [])
    (out / "build_record.json").write_text(json.dumps({"started_at": "x"}))
    builder = _merging_builder(tmp_path, out, ["gnomad_joint_chr1"])
    _stamped(
        builder.processed_path / "gnomAD" / "gnomad_joint_chr1"
        / "variant_masters_gnomad_chr1.parquet",
        "variant_masters", "gnomad",
    )
    monkeypatch.setattr(builder, "_stamp_metadata", lambda *a: None)
    monkeypatch.setattr(builder, "_resync_manifest_entry", lambda *a: None)

    builder._merge()

    record = json.loads((out / "build_record.json").read_text())
    assert record["started_at"] == "x"
    merge = record["merges"][0]
    assert merge["sources"] == ["gnomad_joint_chr1"]
    assert merge["tables"] == ["variant_masters"]
    assert merge["bundle_id_before"] == "0000000000000000"
    assert merge["bundle_id_after"] != merge["bundle_id_before"]


def test_a_source_that_produced_nothing_stops_the_merge(tmp_path):
    """The same guard the normal assembly has: a planned source that
    contributed no table means the run is not what was asked for."""
    out = _bundle(tmp_path, [])
    builder = _merging_builder(tmp_path, out, ["gnomad_joint_chr1"])

    assert builder._merge() is False
