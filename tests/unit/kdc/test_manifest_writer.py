# tests/test_manifest_writer.py
from __future__ import annotations

import json
from pathlib import Path

import pytest

from biofilter.modules.kdc.manifest_writer import KDSManifestWriter


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_write_single_asset_manifest(tmp_path: Path):
    out_dir = tmp_path / "HGNC" / "hgnc"
    out_dir.mkdir(parents=True)

    result = KDSManifestWriter.write(
        output_dir=out_dir,
        source_system="HGNC",
        data_source="hgnc",
        asset="masterdata",
        release="hgnc_20260206",
        assembly="NA",
        path_pattern="master_data.parquet",
        partitioning=[],
        dtp_name="dtp_hgnc",
        dtp_version="1.0.0",
        parameters={"foo": "bar"},
        inputs=[{"name": "hgnc_complete_set.txt", "checksum_md5": "abc"}],
        multi_asset=False,
    )

    assert result.path.name == "_manifest.json"
    assert result.path.exists()

    payload = _read_json(result.path)
    assert payload["kds_manifest_version"] == "1.0"
    assert payload["source_system"] == "HGNC"
    assert payload["data_source"] == "hgnc"
    assert payload["asset"] == "masterdata"
    assert payload["release"] == "hgnc_20260206"
    assert payload["assembly"] == "NA"

    assert payload["storage"]["format"] == "parquet"
    assert payload["storage"]["base_path"] == str(out_dir.resolve())
    assert payload["storage"]["path_pattern"] == "master_data.parquet"
    assert payload["storage"]["partitioning"] == []

    assert payload["dtp"]["name"] == "dtp_hgnc"
    assert payload["dtp"]["version"] == "1.0.0"
    assert payload["parameters"]["foo"] == "bar"
    assert payload["inputs"][0]["name"] == "hgnc_complete_set.txt"


def test_write_multi_asset_manifest(tmp_path: Path):
    out_dir = tmp_path / "Reactome" / "reactome"
    out_dir.mkdir(parents=True)

    result = KDSManifestWriter.write(
        output_dir=out_dir,
        source_system="Reactome",
        data_source="reactome",
        asset="relationships",
        release="reactome_20260206",
        assembly="NA",
        path_pattern="relationship_data.parquet",
        multi_asset=True,
    )

    assert result.path.name == "_manifest.relationships.json"
    assert result.path.exists()

    payload = _read_json(result.path)
    assert payload["asset"] == "relationships"
    assert payload["storage"]["path_pattern"] == "relationship_data.parquet"


def test_overwrite_false_raises(tmp_path: Path):
    out_dir = tmp_path / "NCBI" / "dbsnp_chry"
    out_dir.mkdir(parents=True)

    # First write
    KDSManifestWriter.write(
        output_dir=out_dir,
        source_system="NCBI",
        data_source="dbsnp_chry",
        asset="variants",
        release="manual",
        assembly="GRCh38",
        path_pattern="processed_part_*.parquet",
        multi_asset=False,
        overwrite=True,
    )

    # Second write should fail if overwrite=False
    with pytest.raises(FileExistsError):
        KDSManifestWriter.write(
            output_dir=out_dir,
            source_system="NCBI",
            data_source="dbsnp_chry",
            asset="variants",
            release="manual",
            assembly="GRCh38",
            path_pattern="processed_part_*.parquet",
            multi_asset=False,
            overwrite=False,
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        # Missing/empty required fields
        {"source_system": "", "data_source": "x", "asset": "a", "release": "r"},
        {"source_system": "x", "data_source": "", "asset": "a", "release": "r"},
        {"source_system": "x", "data_source": "x", "asset": "", "release": "r"},
        {"source_system": "x", "data_source": "x", "asset": "a", "release": ""},
    ],
)
def test_required_fields_validation(tmp_path: Path, kwargs: dict):
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with pytest.raises(ValueError):
        KDSManifestWriter.write(
            output_dir=out_dir,
            assembly="NA",
            path_pattern="*.parquet",
            **kwargs,
        )


def test_dtp_block_is_cleaned_when_none(tmp_path: Path):
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    result = KDSManifestWriter.write(
        output_dir=out_dir,
        source_system="HGNC",
        data_source="hgnc",
        asset="masterdata",
        release="manual",
        assembly="NA",
        path_pattern="master_data.parquet",
        # dtp_name/dtp_version omitted
    )

    payload = _read_json(result.path)
    # dtp exists but should be an empty dict (or at least not contain null keys)
    assert "dtp" in payload
    assert payload["dtp"] == {}
