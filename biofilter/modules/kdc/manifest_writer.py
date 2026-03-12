# biofilter/modules/kdc/manifest_writer.py
"""
KDSManifestWriter

Writes KDS manifest JSON files that describe Parquet assets in the KDS (currently: processed/).

Supports:
- single-asset folder:  _manifest.json
- multi-asset folder:   _manifest.<asset>.json

This is intentionally lightweight and DTP-friendly.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps_stable(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, indent=2, ensure_ascii=False)


def _ensure_non_empty(name: str, value: Optional[str]) -> str:
    if value is None:
        raise ValueError(f"{name} is required")
    v = str(value).strip()
    if not v:
        raise ValueError(f"{name} is required")
    return v


def _ensure_list_str(values: Optional[Iterable[str]]) -> list[str]:
    if not values:
        return []
    out: list[str] = []
    for v in values:
        s = str(v).strip()
        if s:
            out.append(s)
    return out


def _normalize_inputs(
    inputs: Optional[list[dict[str, Any]]]
) -> Optional[list[dict[str, Any]]]:
    if not inputs:
        return None
    cleaned: list[dict[str, Any]] = []
    for item in inputs:
        if not isinstance(item, dict):
            raise ValueError("inputs must be a list of dict objects")
        cleaned.append(item)
    return cleaned


def _normalize_parameters(
    parameters: Optional[dict[str, Any]]
) -> Optional[dict[str, Any]]:
    if not parameters:
        return None
    if not isinstance(parameters, dict):
        raise ValueError("parameters must be a dict")
    return parameters


@dataclass(frozen=True)
class ManifestWriteResult:
    path: Path
    payload: dict[str, Any]


class KDSManifestWriter:
    """
    Manifest writer for KDS assets.
    """

    MANIFEST_VERSION = "1.0"

    @staticmethod
    def manifest_filename(asset: str, *, multi_asset: bool) -> str:
        asset = _ensure_non_empty("asset", asset)
        return f"_manifest.{asset}.json" if multi_asset else "_manifest.json"

    @staticmethod
    def build_payload(
        *,
        output_dir: Path | str,
        source_system: str,
        data_source: str,
        asset: str,
        release: str,
        assembly: str = "NA",
        path_pattern: str = "**/*.parquet",
        partitioning: Optional[list[str]] = None,
        format: str = "parquet",
        dtp_name: Optional[str] = None,
        dtp_version: Optional[str] = None,
        parameters: Optional[dict[str, Any]] = None,
        inputs: Optional[list[dict[str, Any]]] = None,
        primary_key: Optional[list[str]] = None,
        link_keys: Optional[list[str]] = None,
        created_at: Optional[str] = None,
    ) -> dict[str, Any]:
        out_dir = Path(output_dir).expanduser()

        payload: dict[str, Any] = {
            "kds_manifest_version": KDSManifestWriter.MANIFEST_VERSION,
            "created_at": created_at or _utc_now_iso(),
            "source_system": _ensure_non_empty("source_system", source_system),
            "data_source": _ensure_non_empty("data_source", data_source),
            "asset": _ensure_non_empty("asset", asset),
            "release": _ensure_non_empty("release", release),
            "assembly": _ensure_non_empty("assembly", assembly),
            "dtp": {
                "name": dtp_name,
                "version": dtp_version,
            },
            "storage": {
                "format": _ensure_non_empty("format", format),
                # keep base_path as a string; caller can choose relative or absolute
                "base_path": str(out_dir),
                "path_pattern": _ensure_non_empty("path_pattern", path_pattern),
                "partitioning": _ensure_list_str(partitioning),
            },
        }

        params = _normalize_parameters(parameters)
        if params is not None:
            payload["parameters"] = params

        ins = _normalize_inputs(inputs)
        if ins is not None:
            payload["inputs"] = ins

        contract: dict[str, Any] = {}
        pk = _ensure_list_str(primary_key)
        lk = _ensure_list_str(link_keys)
        if pk:
            contract["primary_key"] = pk
        if lk:
            contract["link_keys"] = lk
        if contract:
            payload["contract"] = contract

        # Remove empty dtp block keys if None (keep dtp block for consistency, but clean it)
        dtp = payload.get("dtp", {})
        if isinstance(dtp, dict):
            dtp = {k: v for k, v in dtp.items() if v is not None}
            payload["dtp"] = dtp

        return payload

    @staticmethod
    def write(
        *,
        output_dir: Path | str,
        source_system: str,
        data_source: str,
        asset: str,
        release: str,
        assembly: str = "NA",
        path_pattern: str = "**/*.parquet",
        partitioning: Optional[list[str]] = None,
        format: str = "parquet",
        dtp_name: Optional[str] = None,
        dtp_version: Optional[str] = None,
        parameters: Optional[dict[str, Any]] = None,
        inputs: Optional[list[dict[str, Any]]] = None,
        primary_key: Optional[list[str]] = None,
        link_keys: Optional[list[str]] = None,
        multi_asset: bool = False,
        overwrite: bool = True,
        ensure_output_dir_exists: bool = True,
    ) -> ManifestWriteResult:
        """
        Write a manifest JSON file into the output_dir.

        - multi_asset=False => _manifest.json
        - multi_asset=True  => _manifest.<asset>.json
        """
        out_dir = Path(output_dir).expanduser().resolve()

        if ensure_output_dir_exists and not out_dir.exists():
            raise FileNotFoundError(f"output_dir does not exist: {out_dir}")

        payload = KDSManifestWriter.build_payload(
            output_dir=out_dir,
            source_system=source_system,
            data_source=data_source,
            asset=asset,
            release=release,
            assembly=assembly,
            path_pattern=path_pattern,
            partitioning=partitioning,
            format=format,
            dtp_name=dtp_name,
            dtp_version=dtp_version,
            parameters=parameters,
            inputs=inputs,
            primary_key=primary_key,
            link_keys=link_keys,
        )

        filename = KDSManifestWriter.manifest_filename(asset, multi_asset=multi_asset)
        path = out_dir / filename

        if path.exists() and not overwrite:
            raise FileExistsError(f"Manifest already exists: {path}")

        path.write_text(_json_dumps_stable(payload) + "\n", encoding="utf-8")

        return ManifestWriteResult(path=path, payload=payload)


# Como usar no DTP:
"""
from biofilter.modules.kdc.manifest_writer import KDSManifestWriter

# single-asset folder (HGNC)
KDSManifestWriter.write(
    output_dir=self.processed_dir,
    source_system=self.datasource.source_system.name,
    data_source=self.datasource.name,
    asset="masterdata",
    release="hgnc_20260206",
    assembly="NA",
    path_pattern="master_data.parquet",
    dtp_name=self.name,
    dtp_version=self.version,
    parameters={"notes": "first automated manifest"},
)

# multi-asset folder (Reactome)
KDSManifestWriter.write(
    output_dir=self.processed_dir,
    source_system="Reactome",
    data_source="reactome",
    asset="relationships",
    release="reactome_20260206",
    assembly="NA",
    path_pattern="relationship_data.parquet",
    dtp_name=self.name,
    dtp_version=self.version,
    multi_asset=True,
)

"""
