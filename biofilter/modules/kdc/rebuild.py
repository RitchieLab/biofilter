# biofilter/modules/kdc/rebuild.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import text as sql_text
from sqlalchemy.orm import Session

from biofilter.modules.kdc.scanner import (
    ScannedAsset,
    scan_manifests,
    scan_asset_from_manifest,
)

from biofilter.modules.db.models.model_kdc import (  # adjust import path to your project
    KDCAsset,
    KDCAssetVersion,
    KDCSchema,
    KDCSchemaField,
    KDCLineage,
    KDCScanRun,
)


@dataclass
class RebuildResult:
    assets_scanned: int
    versions_upserted: int
    warnings: list[str]


def _upsert_asset(session: Session, s: ScannedAsset) -> KDCAsset:
    asset = (
        session.query(KDCAsset)
        .filter(
            KDCAsset.source_system == s.source_system,
            KDCAsset.data_source == s.data_source,
            KDCAsset.asset == s.asset,
        )
        .one_or_none()
    )
    if asset is None:
        asset = KDCAsset(
            source_system=s.source_system,
            data_source=s.data_source,
            asset=s.asset,
        )
        session.add(asset)
        session.flush()
    return asset


def _upsert_asset_version(
    session: Session, asset: KDCAsset, s: ScannedAsset
) -> KDCAssetVersion:
    # Uniqueness choice (current model): identity by manifest_hash
    av = (
        session.query(KDCAssetVersion)
        .filter(
            KDCAssetVersion.asset_id == asset.id,
            KDCAssetVersion.release == s.release,
            KDCAssetVersion.assembly == s.assembly,
            KDCAssetVersion.manifest_hash == s.manifest_hash,
        )
        .one_or_none()
    )

    if av is None:
        av = KDCAssetVersion(
            asset_id=asset.id,
            release=s.release,
            assembly=s.assembly,
            status="ACTIVE",
            base_path=str(s.base_path),
            path_pattern=s.path_pattern,
            partitioning=s.partitioning,
            row_count=s.row_count,
            file_count=s.file_count,
            manifest_path=str(s.manifest_path) if s.manifest_path else None,
            manifest_hash=s.manifest_hash,
        )
        session.add(av)
        session.flush()
    else:
        # Update mutable fields (scanner can refresh counts/layout)
        av.base_path = str(s.base_path)
        av.path_pattern = s.path_pattern
        av.partitioning = s.partitioning
        av.row_count = s.row_count
        av.file_count = s.file_count
        av.manifest_path = str(s.manifest_path) if s.manifest_path else av.manifest_path
        av.status = "ACTIVE"

    return av


def _upsert_schema(session: Session, av: KDCAssetVersion, s: ScannedAsset) -> KDCSchema:
    schema = (
        session.query(KDCSchema)
        .filter(KDCSchema.asset_version_id == av.id)
        .one_or_none()
    )
    if schema is None:
        schema = KDCSchema(
            asset_version_id=av.id,
            schema_json=s.schema_json,
            schema_hash=s.schema_hash,
            primary_key=None,
            link_keys=None,
        )
        session.add(schema)
        session.flush()
    else:
        schema.schema_json = s.schema_json
        schema.schema_hash = s.schema_hash
    return schema


def _rebuild_fields_from_schema(
    session: Session, av: KDCAssetVersion, schema: KDCSchema
) -> None:
    """
    Scanner-generated field rows (minimal) are derived from schema_json.
    We delete-and-recreate for idempotency (safe: metadata table).
    """
    session.query(KDCSchemaField).filter(
        KDCSchemaField.asset_version_id == av.id
    ).delete()

    pk = set(schema.primary_key or [])
    lk = set(schema.link_keys or [])

    fields = schema.schema_json.get("fields", [])
    for f in fields:
        session.add(
            KDCSchemaField(
                asset_version_id=av.id,
                field_name=f["name"],
                data_type=f["type"],
                nullable=f.get("nullable", None),
                is_primary_key=f["name"] in pk,
                is_link_key=f["name"] in lk,
                status="ACTIVE",
            )
        )


def _reset_kdc_tables(session: Session, *, keep_scan_runs: bool = True) -> None:
    """
    Reset KDC catalog tables.

    - Postgres: TRUNCATE ... CASCADE is fast and resets identities.
    - Others: fallback to DELETE in FK-safe order.
    """
    dialect = session.get_bind().dialect.name

    if dialect == "postgresql":
        tables = [
            "kdc_schema_fields",
            "kdc_lineages",
            "kdc_schemas",
            "kdc_asset_versions",
            "kdc_assets",
        ]
        if not keep_scan_runs:
            tables.append("kdc_scan_runs")

        # TRUNCATE is fastest; RESTART IDENTITY keeps ids clean
        sql = "TRUNCATE " + ", ".join(tables) + " RESTART IDENTITY CASCADE;"
        session.execute(sql_text(sql))
        return

    # Fallback: DELETE in safe order (works on sqlite/mysql)
    session.query(KDCSchemaField).delete()
    session.query(KDCLineage).delete()
    session.query(KDCSchema).delete()
    session.query(KDCAssetVersion).delete()
    session.query(KDCAsset).delete()
    if not keep_scan_runs:
        session.query(KDCScanRun).delete()


def _upsert_lineage(
    session: Session, av: KDCAssetVersion, s: ScannedAsset
) -> KDCLineage:
    lineage = (
        session.query(KDCLineage)
        .filter(KDCLineage.asset_version_id == av.id)
        .one_or_none()
    )
    if lineage is None:
        lineage = KDCLineage(
            asset_version_id=av.id,
            dtp_name=s.dtp_name,
            dtp_version=s.dtp_version,
            parameters_json=s.parameters_json,
            parameters_hash=s.parameters_hash,
            inputs_json=s.inputs_json,
        )
        session.add(lineage)
        session.flush()
    else:
        lineage.dtp_name = s.dtp_name
        lineage.dtp_version = s.dtp_version
        lineage.parameters_json = s.parameters_json
        lineage.parameters_hash = s.parameters_hash
        lineage.inputs_json = s.inputs_json
    return lineage


def rebuild_kdc(
    session: Session,
    kds_root: str | Path,
    *,
    dry_run: bool = False,
    strict: bool = False,
    reset: bool = False,
    keep_scan_runs: bool = True,
) -> RebuildResult:
    """
    Rebuild KDC tables by scanning KDS manifests and Parquet schemas.

    - dry_run: scan + validate but do not write to DB
    - strict: raise on any error; otherwise collect warnings and continue
    """
    root = Path(kds_root).expanduser().resolve()
    warnings: list[str] = []

    if reset:
        if dry_run:
            warnings.append("[KDC] reset=True ignored because dry_run=True.")
        else:
            _reset_kdc_tables(session, keep_scan_runs=keep_scan_runs)

    scan_run = KDCScanRun(kds_root=str(root), status="SUCCESS", summary_json=None)
    if not dry_run:
        session.add(scan_run)
        session.flush()

    manifests = scan_manifests(root)

    versions_upserted = 0
    for mp in manifests:
        try:
            scanned = scan_asset_from_manifest(mp)

            if dry_run:
                versions_upserted += 1
                continue

            asset = _upsert_asset(session, scanned)
            av = _upsert_asset_version(session, asset, scanned)
            schema = _upsert_schema(session, av, scanned)
            _rebuild_fields_from_schema(session, av, schema)
            _upsert_lineage(session, av, scanned)

            versions_upserted += 1

        except Exception as e:
            msg = f"[KDC] Failed scanning {mp}: {e}"
            if strict:
                if not dry_run:
                    scan_run.status = "FAILED"
                    scan_run.summary_json = {"error": msg, "warnings": warnings}
                    session.flush()
                raise
            warnings.append(msg)

    summary = {
        "manifests_found": len(manifests),
        "versions_upserted": versions_upserted,
        "warnings_count": len(warnings),
        "warnings": warnings[:50],  # cap
    }

    if not dry_run:
        scan_run.summary_json = summary
        scan_run.status = "FAILED" if warnings else "SUCCESS"
        session.flush()

    return RebuildResult(
        assets_scanned=len(manifests),
        versions_upserted=versions_upserted,
        warnings=warnings,
    )
