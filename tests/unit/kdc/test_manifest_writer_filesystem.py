from __future__ import annotations

import json
from pathlib import Path

from biofilter.modules.kdc.manifest_writer import KDSManifestWriter


def test_write_manifest_into_test_data_folder():
    """
    Writes a manifest into tests/test_data/kdc and validates
    the resulting file and payload.
    """
    base_dir = Path(__file__).parents[2] / "test_data" / "kdc"
    out_dir = base_dir / "HGNC" / "hgnc"

    # Ensure folder exists
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = out_dir / "_manifest.json"
    if manifest_path.exists():
        manifest_path.unlink()  # clean previous runs

    result = KDSManifestWriter.write(
        output_dir=out_dir,
        source_system="HGNC",
        data_source="hgnc",
        asset="masterdata",
        release="hgnc_test_release",
        assembly="NA",
        path_pattern="master_data.parquet",
        dtp_name="dtp_hgnc",
        dtp_version="test",
        parameters={"env": "test"},
    )

    assert result.path.exists()
    assert result.path.name == "_manifest.json"

    payload = json.loads(result.path.read_text())

    assert payload["source_system"] == "HGNC"
    assert payload["data_source"] == "hgnc"
    assert payload["asset"] == "masterdata"
    assert payload["release"] == "hgnc_test_release"
    assert payload["storage"]["base_path"] == str(out_dir.resolve())
    assert payload["storage"]["path_pattern"] == "master_data.parquet"
