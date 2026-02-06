# biofilter/modules/kdc/scanner.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from biofilter.modules.kdc.manifest import KDSManifest, find_manifests_in_dir, load_manifest
from biofilter.modules.kdc.utils import sha256_json, sha256_text

try:
    import pyarrow as pa  # noqa: F401
    import pyarrow.dataset as ds
except Exception:  # pragma: no cover
    ds = None  # type: ignore


@dataclass
class ScannedAsset:
    manifest_path: Optional[Path]
    manifest_hash: Optional[str]

    source_system: str
    data_source: str
    asset: str
    release: str
    assembly: str

    base_path: Path
    path_pattern: str
    partitioning: list[str]

    schema_json: dict[str, Any]
    schema_hash: str

    row_count: Optional[int]
    file_count: Optional[int]

    dtp_name: Optional[str]
    dtp_version: Optional[str]
    parameters_json: Optional[dict[str, Any]]
    parameters_hash: Optional[str]
    inputs_json: Optional[list[dict[str, Any]]]


def _read_parquet_schema(base_path: Path, pattern: str) -> tuple[dict[str, Any], str]:
    """
    Read schema using PyArrow Dataset (metadata only). Does not load full data.
    Returns:
      schema_json: {"fields":[{"name":..., "type":..., "nullable":...}, ...]}
      schema_hash: sha256 of schema_json
    """
    if ds is None:
        raise RuntimeError("pyarrow is required for KDC schema scanning (pip install pyarrow).")

    # Create dataset; allow partitioning inference
    dataset = ds.dataset(str(base_path), format="parquet")
    schema = dataset.schema

    fields = []
    for f in schema:
        fields.append(
            {
                "name": f.name,
                "type": str(f.type),
                "nullable": bool(f.nullable),
            }
        )

    schema_json = {"fields": fields}
    return schema_json, sha256_json(schema_json)


def _estimate_counts(base_path: Path) -> tuple[Optional[int], Optional[int]]:
    """
    Best-effort count:
    - file_count: count parquet files (can be expensive for huge trees; still ok for MVP)
    - row_count: uses dataset fragments metadata (fast-ish, but may vary)
    """
    if ds is None:
        return None, None

    dataset = ds.dataset(str(base_path), format="parquet")
    fragments = list(dataset.get_fragments())
    file_count = len(fragments)

    # Row count from metadata if available
    total_rows = 0
    have_rows = True
    for frag in fragments[:2000]:  # safety cap for very large datasets
        try:
            md = frag.metadata
            if md is None:
                have_rows = False
                break
            total_rows += md.num_rows
        except Exception:
            have_rows = False
            break

    return (total_rows if have_rows else None), file_count


def scan_manifests(kds_root: Path) -> list[Path]:
    """
    Find all manifest files under KDS.

    Supports both:
    - canonical: .../release=.../assembly=.../_manifest.json
    - transitional: .../_manifest.<asset>.json
    """
    manifests: list[Path] = []
    # Any file named _manifest*.json anywhere in KDS
    manifests.extend(sorted(kds_root.rglob("_manifest.json")))
    manifests.extend(sorted(kds_root.rglob("_manifest.*.json")))
    # De-dup
    return sorted(set(manifests))


def scan_asset_from_manifest(manifest_path: Path) -> ScannedAsset:
    raw = load_manifest(manifest_path)
    manifest = KDSManifest.from_json(raw)
    manifest.validate_minimal()

    manifest_hash = sha256_json(raw)

    # Determine base_path:
    # - Prefer manifest.storage.base_path
    # - Else: infer as the directory containing the manifest
    base_path = Path(manifest.base_path) if manifest.base_path else manifest_path.parent

    # Determine pattern/partitioning
    path_pattern = manifest.path_pattern or "**/*.parquet"
    partitioning = manifest.partitioning or []

    schema_json, schema_hash = _read_parquet_schema(base_path, path_pattern)
    row_count, file_count = _estimate_counts(base_path)

    parameters_hash = sha256_json(manifest.parameters) if manifest.parameters else None

    return ScannedAsset(
        manifest_path=manifest_path,
        manifest_hash=manifest_hash,
        source_system=manifest.source_system,
        data_source=manifest.data_source,
        asset=manifest.asset,
        release=manifest.release,
        assembly=manifest.assembly,
        base_path=base_path,
        path_pattern=path_pattern,
        partitioning=partitioning,
        schema_json=schema_json,
        schema_hash=schema_hash,
        row_count=row_count,
        file_count=file_count,
        dtp_name=manifest.dtp_name,
        dtp_version=manifest.dtp_version,
        parameters_json=manifest.parameters,
        parameters_hash=parameters_hash,
        inputs_json=manifest.inputs,
    )
