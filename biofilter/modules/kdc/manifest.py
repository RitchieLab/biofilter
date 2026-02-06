# biofilter/modules/kdc/manifest.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True)
class KDSManifest:
    """
    Minimal manifest contract.

    This is intentionally permissive: we validate only what the catalog needs.
    """

    manifest_version: str
    source_system: str
    data_source: str
    asset: str
    release: str
    assembly: str

    created_at: Optional[str] = None

    dtp_name: Optional[str] = None
    dtp_version: Optional[str] = None

    parameters: Optional[dict[str, Any]] = None
    inputs: Optional[list[dict[str, Any]]] = None

    # Physical storage hints
    base_path: Optional[str] = None
    path_pattern: Optional[str] = None
    partitioning: Optional[list[str]] = None

    @staticmethod
    def from_json(data: dict[str, Any]) -> "KDSManifest":
        dtp = data.get("dtp") or {}
        storage = data.get("storage") or {}

        return KDSManifest(
            manifest_version=str(data.get("kds_manifest_version", "1.0")),
            source_system=str(data["source_system"]),
            data_source=str(data["data_source"]),
            asset=str(data["asset"]),
            release=str(data["release"]),
            assembly=str(data.get("assembly", "NA")),
            created_at=data.get("created_at"),
            dtp_name=dtp.get("name"),
            dtp_version=dtp.get("version"),
            parameters=data.get("parameters"),
            inputs=data.get("inputs"),
            base_path=storage.get("base_path"),
            path_pattern=storage.get("path_pattern"),
            partitioning=storage.get("partitioning"),
        )

    def validate_minimal(self) -> None:
        required = [
            ("source_system", self.source_system),
            ("data_source", self.data_source),
            ("asset", self.asset),
            ("release", self.release),
            ("assembly", self.assembly),
        ]
        missing = [k for k, v in required if not v]
        if missing:
            raise ValueError(f"Manifest missing required fields: {missing}")


def load_manifest(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def find_manifests_in_dir(dir_path: Path) -> list[Path]:
    """
    Transitional layout:
      _manifest.json (single-asset folder)
      _manifest.<asset>.json (multi-asset folder)

    Canonical layout:
      .../release=.../assembly=.../_manifest.json
    """
    manifests: list[Path] = []
    manifests.extend(sorted(dir_path.glob("_manifest.json")))
    manifests.extend(sorted(dir_path.glob("_manifest.*.json")))
    return manifests
