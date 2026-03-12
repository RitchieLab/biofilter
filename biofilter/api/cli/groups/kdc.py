# biofilter/api/cli/groups/kdc.py
from __future__ import annotations

import json
from typing import Optional

import click

from biofilter.biofilter import Biofilter
from biofilter.api.cli.common import require_db_uri, local_db_uri_option


@click.group()
def kdc():
    """Knowledge Data Catalog (KDC) commands."""
    pass


# @kdc.command("rebuild")
# @local_db_uri_option
# @click.option(
#     "--kds-root",
#     default="biofilter_data/processed",
#     show_default=True,
#     help="KDS root folder (currently implemented as processed/).",
# )
# @click.option("--dry-run", is_flag=True, help="Scan only; do not write to DB.")
# @click.option("--strict", is_flag=True, help="Fail fast on any error.")
# @click.option("--debug", is_flag=True, help="Enable debug logging.")

# @click.pass_context
# def rebuild(ctx, db_uri: str | None, kds_root: str, dry_run: bool, strict: bool, debug: bool):
#     """
#     Rebuild the KDC tables by scanning manifests and Parquet schemas under the KDS root.
#     """
#     db_uri = require_db_uri(ctx, local_db_uri=db_uri)
#     bf = Biofilter(db_uri=db_uri, debug_mode=debug)
#     bf.db.connect()

#     result = bf.kdc.rebuild(kds_root=kds_root, dry_run=dry_run, strict=strict)


#     # Nice compact output
#     click.echo("✅ KDC rebuild completed.")
#     click.echo(f"   • assets_scanned: {result.assets_scanned}")
#     click.echo(f"   • versions_upserted: {result.versions_upserted}")
#     if result.warnings:
#         click.echo(f"   • warnings: {len(result.warnings)}")
#         for w in result.warnings[:20]:
#             click.echo(f"     - {w}")
@kdc.command("rebuild")
@local_db_uri_option
@click.option(
    "--kds-root",
    default="biofilter_data/processed",
    show_default=True,
    help="KDS root folder (currently implemented as processed/).",
)
@click.option("--dry-run", is_flag=True, help="Scan only; do not write to DB.")
@click.option("--strict", is_flag=True, help="Fail fast on any error.")
@click.option(
    "--reset",
    is_flag=True,
    help="Truncate KDC catalog tables before rebuilding (clean slate).",
)
@click.option(
    "--drop-scan-history",
    is_flag=True,
    help="When used with --reset, also remove KDC scan run history.",
)
@click.option("--yes", is_flag=True, help="Skip confirmation prompt (for automation).")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def rebuild(
    ctx,
    db_uri: str | None,
    kds_root: str,
    dry_run: bool,
    strict: bool,
    reset: bool,
    drop_scan_history: bool,
    yes: bool,
    debug: bool,
):
    """
    Rebuild the KDC tables by scanning manifests and Parquet schemas under the KDS root.
    """

    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    if reset and not dry_run:
        if not yes:
            click.confirm(
                "⚠️  This will RESET the KDC catalog tables. Continue?",
                abort=True,
            )

        click.echo("🧹 Resetting KDC catalog before rebuild...")

    result = bf.kdc.rebuild(
        kds_root=kds_root,
        dry_run=dry_run,
        strict=strict,
        reset=reset,
        keep_scan_runs=not drop_scan_history,
    )

    click.echo("════════════════════════════════════")
    click.echo("✅ KDC rebuild completed.")
    click.echo(f"   • kds_root: {kds_root}")
    click.echo(f"   • assets_scanned: {result.assets_scanned}")
    click.echo(f"   • versions_upserted: {result.versions_upserted}")

    if dry_run:
        click.echo("   • mode: DRY-RUN (no DB writes)")

    if reset:
        click.echo("   • reset_mode: TRUE")

    if result.warnings:
        click.echo(f"   • warnings: {len(result.warnings)}")
        for w in result.warnings[:20]:
            click.echo(f"     - {w}")

    click.echo("════════════════════════════════════")


@kdc.command("list")
@local_db_uri_option
@click.option(
    "--source-system", default=None, help="Filter by source system (e.g., HGNC)."
)
@click.option("--data-source", default=None, help="Filter by data source (e.g., hgnc).")
@click.option("--asset", default=None, help="Filter by asset name (e.g., masterdata).")
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    help="Output as JSON.",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def list_cmd(
    ctx,
    db_uri: str | None,
    source_system: Optional[str],
    data_source: Optional[str],
    asset: Optional[str],
    as_json: bool,
    debug: bool,
):
    """
    List assets (and optionally versions) known by the KDC.
    """
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    # If user passes any filter, show versions. Otherwise show the compact asset list.
    if source_system or data_source or asset:
        versions = bf.kdc.list_asset_versions(
            source_system=source_system,
            data_source=data_source,
            asset=asset,
        )

        if as_json:
            payload = [
                {
                    "id": v.id,
                    "source_system": v.asset_ref.source_system,
                    "data_source": v.asset_ref.data_source,
                    "asset": v.asset_ref.asset,
                    "release": v.release,
                    "assembly": v.assembly,
                    "status": v.status,
                    "base_path": v.base_path,
                    "path_pattern": v.path_pattern,
                    "partitioning": v.partitioning,
                    "row_count": v.row_count,
                    "file_count": v.file_count,
                    "manifest_path": v.manifest_path,
                    "manifest_hash": v.manifest_hash,
                }
                for v in versions
            ]
            click.echo(json.dumps(payload, indent=2))
            return

        if not versions:
            click.echo("No asset versions found for the given filters.")
            return

        click.echo("KDC Asset Versions:")
        for v in versions:
            click.echo(
                f"- id={v.id} "
                f"{v.asset_ref.source_system}/{v.asset_ref.data_source}/{v.asset_ref.asset} "
                f"release={v.release} assembly={v.assembly} status={v.status}"
            )
        return

    # No filters: show compact assets list
    assets = bf.kdc.list_assets()
    if as_json:
        payload = [
            {"source_system": a[0], "data_source": a[1], "asset": a[2]} for a in assets
        ]
        click.echo(json.dumps(payload, indent=2))
        return

    if not assets:
        click.echo("No assets found.")
        return

    click.echo("KDC Assets:")
    for source_system_, data_source_, asset_ in assets:
        click.echo(f"- {source_system_}/{data_source_}/{asset_}")


@kdc.command("describe")
@local_db_uri_option
@click.option("--source-system", required=True, help="Source system (e.g., HGNC).")
@click.option("--data-source", required=True, help="Data source (e.g., hgnc).")
@click.option("--asset", required=True, help="Asset (e.g., masterdata).")
@click.option("--release", default="manual", show_default=True, help="Release tag.")
@click.option("--assembly", default="NA", show_default=True, help="Assembly tag.")
@click.option(
    "--limit",
    type=int,
    default=200,
    show_default=True,
    help="Max number of fields to print.",
)
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def describe(
    ctx,
    db_uri: str | None,
    source_system: str,
    data_source: str,
    asset: str,
    release: str,
    assembly: str,
    limit: int,
    as_json: bool,
    debug: bool,
):
    """
    Describe an asset version (schema fields / data dictionary).
    """
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    # Find matching version (simple strategy: first match)
    versions = bf.kdc.list_asset_versions(
        source_system=source_system,
        data_source=data_source,
        asset=asset,
    )

    match = None
    for v in versions:
        if v.release == release and v.assembly == assembly:
            match = v
            break

    if match is None:
        raise click.ClickException(
            f"Asset version not found: {source_system}/{data_source}/{asset} "
            f"release={release} assembly={assembly}"
        )

    fields = bf.kdc.describe_asset_version(match.id)

    if as_json:
        payload = [
            {
                "field_name": f.field_name,
                "data_type": f.data_type,
                "nullable": f.nullable,
                "is_primary_key": f.is_primary_key,
                "is_link_key": f.is_link_key,
                "semantics": f.semantics,
                "units": f.units,
                "links_to_entity": f.links_to_entity,
                "db_column": f.db_column,
                "status": f.status,
                "description": f.description,
            }
            for f in fields[:limit]
        ]
        click.echo(json.dumps(payload, indent=2))
        return

    click.echo(
        f"KDC Describe: {source_system}/{data_source}/{asset} "
        f"release={release} assembly={assembly} (id={match.id})"
    )
    click.echo(f"Fields (showing up to {min(limit, len(fields))}):")
    for f in fields[:limit]:
        flags = []
        if f.is_primary_key:
            flags.append("PK")
        if f.is_link_key:
            flags.append("LK")
        flags_txt = f"[{','.join(flags)}]" if flags else ""
        click.echo(f"- {f.field_name}: {f.data_type} nullable={f.nullable} {flags_txt}")
