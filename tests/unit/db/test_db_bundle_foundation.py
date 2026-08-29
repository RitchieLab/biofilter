"""
Bundle foundation: manifest v2, partition handling, verification, and
parquet:// registration of partitioned datasets.
"""

from __future__ import annotations

import json

import duckdb
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from biofilter.modules.db.database import (
    Database,
    _tolerant_json_deserializer,
)
from biofilter.modules.db.transfer import (
    MANIFEST_VERSION,
    collect_provenance,
    export_full_clone,
    partition_child_tables,
    sha256_file,
    verify_bundle,
)


# ---------------------------------------------------------------------------
# JSON deserializer
# ---------------------------------------------------------------------------


def test_tolerant_json_deserializer_passes_through_decoded_values():
    """duckdb_engine hands back already-decoded JSON; json.loads() would
    raise TypeError on it."""
    assert _tolerant_json_deserializer({"a": 1}) == {"a": 1}
    assert _tolerant_json_deserializer([1, 2]) == [1, 2]


def test_tolerant_json_deserializer_still_parses_strings():
    assert _tolerant_json_deserializer('{"a": 1}') == {"a": 1}


def test_duckdb_engine_kwargs_install_the_deserializer():
    db = Database()
    kwargs = db._engine_kwargs("duckdb:///:memory:")
    assert kwargs["json_deserializer"] is _tolerant_json_deserializer


def test_postgres_engine_kwargs_do_not_install_it():
    """psycopg2 already decodes JSON and the PG dialect handles it."""
    db = Database()
    kwargs = db._engine_kwargs("postgresql+psycopg2://u:p@localhost/x")
    assert "json_deserializer" not in kwargs


# ---------------------------------------------------------------------------
# Checksums
# ---------------------------------------------------------------------------


def test_sha256_file_is_stable_and_content_sensitive(tmp_path):
    a = tmp_path / "a.bin"
    b = tmp_path / "b.bin"
    a.write_bytes(b"biofilter")
    b.write_bytes(b"biofilter")
    assert sha256_file(a) == sha256_file(b)

    b.write_bytes(b"biofilteR")
    assert sha256_file(a) != sha256_file(b)


def test_sha256_file_returns_none_for_missing_file(tmp_path):
    assert sha256_file(tmp_path / "nope.bin") is None


# ---------------------------------------------------------------------------
# Partition detection (non-PG dialects have none)
# ---------------------------------------------------------------------------


def test_partition_child_tables_is_empty_on_sqlite(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'p.db'}", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))
    assert partition_child_tables(engine) == set()


def test_collect_provenance_is_empty_without_etl_tables(tmp_path):
    """A bundle exported from a DB without ETL tables must still export."""
    engine = create_engine(f"sqlite:///{tmp_path / 'p.db'}", future=True)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))
    assert collect_provenance(engine) == []


# ---------------------------------------------------------------------------
# Manifest v2
# ---------------------------------------------------------------------------


@pytest.fixture()
def sqlite_engine(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'src.db'}", future=True)
    with engine.begin() as conn:
        conn.execute(
            text("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)")
        )
        conn.execute(
            text(
                "INSERT INTO widgets (id, name) VALUES "
                "(1, 'alpha'), (2, 'beta'), (3, 'gamma')"
            )
        )
    return engine


def test_export_writes_manifest_v2_with_size_and_hash(sqlite_engine, tmp_path):
    out = export_full_clone(
        sqlite_engine,
        tmp_path / "bundle",
        biofilter_version="4.2.0",
        schema_version="test",
    )

    manifest = json.loads((out / "manifest.json").read_text())
    assert manifest["manifest_version"] == MANIFEST_VERSION
    assert manifest["format"] == "parquet"
    assert manifest["partition_children_included"] is False

    entry = next(t for t in manifest["tables"] if t["name"] == "widgets")
    assert entry["rows"] == 3
    assert entry["bytes"] > 0
    assert len(entry["sha256"]) == 64
    assert sha256_file(out / entry["file"]) == entry["sha256"]


def test_export_can_skip_checksums(sqlite_engine, tmp_path):
    out = export_full_clone(
        sqlite_engine,
        tmp_path / "bundle",
        biofilter_version="4.2.0",
        schema_version="test",
        checksums=False,
    )
    manifest = json.loads((out / "manifest.json").read_text())
    entry = next(t for t in manifest["tables"] if t["name"] == "widgets")
    assert "sha256" not in entry
    assert entry["bytes"] > 0


# ---------------------------------------------------------------------------
# verify_bundle
# ---------------------------------------------------------------------------


def test_verify_bundle_accepts_an_untouched_bundle(sqlite_engine, tmp_path):
    out = export_full_clone(
        sqlite_engine,
        tmp_path / "bundle",
        biofilter_version="4.2.0",
        schema_version="test",
    )
    report = verify_bundle(out)
    assert report["ok"] is True
    assert report["problems"] == []
    assert report["tables_verified"] == report["tables_declared"] > 0


def test_verify_bundle_detects_a_corrupted_payload(sqlite_engine, tmp_path):
    out = export_full_clone(
        sqlite_engine,
        tmp_path / "bundle",
        biofilter_version="4.2.0",
        schema_version="test",
    )
    manifest = json.loads((out / "manifest.json").read_text())
    target = out / manifest["tables"][0]["file"]
    target.write_bytes(target.read_bytes() + b"corruption")

    report = verify_bundle(out)
    assert report["ok"] is False
    assert any("size mismatch" in p for p in report["problems"])


def test_verify_bundle_detects_a_missing_file(sqlite_engine, tmp_path):
    out = export_full_clone(
        sqlite_engine,
        tmp_path / "bundle",
        biofilter_version="4.2.0",
        schema_version="test",
    )
    manifest = json.loads((out / "manifest.json").read_text())
    (out / manifest["tables"][0]["file"]).unlink()

    report = verify_bundle(out)
    assert report["ok"] is False
    assert any("missing file" in p for p in report["problems"])


def test_verify_bundle_requires_a_manifest(tmp_path):
    (tmp_path / "empty").mkdir()
    with pytest.raises(FileNotFoundError):
        verify_bundle(tmp_path / "empty")


# ---------------------------------------------------------------------------
# parquet:// source discovery
# ---------------------------------------------------------------------------


def _write_parquet(path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def test_flat_files_become_one_view_each(tmp_path):
    tables = tmp_path / "tables"
    _write_parquet(tables / "genes.parquet", pd.DataFrame({"id": [1, 2]}))
    _write_parquet(tables / "proteins.parquet", pd.DataFrame({"id": [3]}))

    db = Database()
    db._parquet_dir = tables
    names = [name for name, _ in db._discover_bundle_sources()]
    assert names == ["genes", "proteins"]


def test_directories_become_hive_partitioned_datasets(tmp_path):
    tables = tmp_path / "tables"
    _write_parquet(
        tables / "variant_masters" / "chromosome=1" / "part-0.parquet",
        pd.DataFrame({"variant_id": [1, 2]}),
    )
    _write_parquet(
        tables / "variant_masters" / "chromosome=2" / "part-0.parquet",
        pd.DataFrame({"variant_id": [3]}),
    )

    db = Database()
    db._parquet_dir = tables
    sources = dict(db._discover_bundle_sources())
    assert set(sources) == {"variant_masters"}
    assert "hive_partitioning = true" in sources["variant_masters"]


def test_legacy_partition_children_are_dropped_when_parent_present(tmp_path):
    """Old bundles shipped parent + children; the parent already holds
    every row, so the children are duplicates."""
    tables = tmp_path / "tables"
    _write_parquet(
        tables / "variant_masters.parquet", pd.DataFrame({"id": [1, 2, 3]})
    )
    _write_parquet(
        tables / "variant_masters_chr_1.parquet", pd.DataFrame({"id": [1]})
    )
    _write_parquet(
        tables / "variant_masters_chr_2.parquet", pd.DataFrame({"id": [2]})
    )

    db = Database()
    db._parquet_dir = tables
    names = [name for name, _ in db._discover_bundle_sources()]
    assert names == ["variant_masters"]


def test_orphan_partition_children_are_kept(tmp_path):
    """Without the parent, the children are the only copy of the data —
    dropping them would silently lose rows."""
    tables = tmp_path / "tables"
    _write_parquet(
        tables / "variant_masters_chr_1.parquet", pd.DataFrame({"id": [1]})
    )

    db = Database()
    db._parquet_dir = tables
    names = [name for name, _ in db._discover_bundle_sources()]
    assert names == ["variant_masters_chr_1"]


def test_partitioned_dataset_is_queryable_end_to_end(tmp_path):
    """The partition key must come back as a real column, and rows from
    every partition must be visible through the single view."""
    tables = tmp_path / "tables"
    _write_parquet(
        tables / "variant_masters" / "chromosome=1" / "part-0.parquet",
        pd.DataFrame({"variant_id": [1, 2]}),
    )
    _write_parquet(
        tables / "variant_masters" / "chromosome=2" / "part-0.parquet",
        pd.DataFrame({"variant_id": [3]}),
    )

    db = Database()
    db._parquet_dir = tables
    db.engine = create_engine(
        "duckdb:///:memory:", **db._engine_kwargs("duckdb:///:memory:")
    )  # noqa E501
    assert db._register_parquet_views() == 1

    with db.engine.connect() as conn:
        total = conn.execute(
            text("SELECT count(*) FROM variant_masters")
        ).scalar()
        by_chrom = conn.execute(
            text(
                "SELECT chromosome, count(*) FROM variant_masters "
                "GROUP BY 1 ORDER BY 1"
            )
        ).fetchall()

    assert total == 3
    assert [tuple(r) for r in by_chrom] == [(1, 2), (2, 1)]


def test_registration_fails_loudly_on_an_empty_directory(tmp_path):
    tables = tmp_path / "tables"
    tables.mkdir()

    db = Database()
    db._parquet_dir = tables
    db.engine = create_engine("duckdb:///:memory:")
    with pytest.raises(FileNotFoundError):
        db._register_parquet_views()


def test_exists_db_sees_partition_only_bundles(tmp_path):
    """A bundle may hold no top-level *.parquet at all."""
    tables = tmp_path / "tables"
    _write_parquet(
        tables / "variant_masters" / "chromosome=1" / "part-0.parquet",
        pd.DataFrame({"variant_id": [1]}),
    )

    db = Database()
    db.db_uri = f"parquet://{tables}"
    db._normalize_uri(db.db_uri)
    assert db.exists_db() is True


def test_duckdb_reads_the_partition_key_as_a_column(tmp_path):
    """Guards the hive_partitioning assumption the registration relies on."""
    root = tmp_path / "ds"
    _write_parquet(
        root / "chromosome=7" / "part-0.parquet", pd.DataFrame({"v": [1]})
    )
    rows = (
        duckdb.connect()
        .execute(
            f"SELECT chromosome, v FROM read_parquet("
            f"'{root}/**/*.parquet', hive_partitioning = true)"
        )
        .fetchall()
    )
    assert rows == [(7, 1)]
