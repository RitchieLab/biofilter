# biofilter/api/cli/groups/etl.py
from __future__ import annotations

import click
import pandas as pd

from biofilter.api.cli.common import local_db_uri_option, require_db_uri
from biofilter.biofilter import Biofilter


@click.group()
def etl():
    """Run and manage ETL pipelines."""
    pass


def _to_list_or_none(values):
    values = list(values) if values else []
    return values or None


def _classify_load_result(status: object) -> str:
    if status is None or pd.isna(status):
        return "never"
    value = str(status or "").strip().lower()
    if not value:
        return "never"
    if value in {"completed", "up-to-date"}:
        return "success"
    if value == "failed":
        return "fail"
    return value


def _format_ts(value: object) -> str | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return str(value)
    return ts.strftime("%Y-%m-%d %H:%M:%S")


@etl.command("update")
@local_db_uri_option
@click.option(
    "--source-system",
    multiple=True,
    help="Source system name (repeatable). Example: --source-system HGNC",
)
@click.option(
    "--data-source",
    multiple=True,
    help="Data source name (repeatable). Example: --data-source dbsnp_sample",
)
@click.option(
    "--run-step",
    multiple=True,
    type=click.Choice(["extract", "transform", "load"], case_sensitive=False),
    help="ETL step to run (repeatable). Default: all steps.",
)
@click.option(
    "--force-step",
    multiple=True,
    type=click.Choice(["extract", "transform", "load"], case_sensitive=False),
    help="ETL step to force (repeatable). Default: none.",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def update(ctx, db_uri, source_system, data_source, run_step, force_step, debug):  # noqa E501
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    bf.etl.update(
        source_system=_to_list_or_none(source_system),
        data_sources=_to_list_or_none(data_source),
        run_steps=_to_list_or_none(run_step),
        force_steps=_to_list_or_none(force_step),
    )


@etl.command("update-all")
@local_db_uri_option
@click.option(
    "--source-system",
    multiple=True,
    help="Source system name filter (repeatable). If omitted, includes all.",
)
@click.option(
    "--data-source",
    multiple=True,
    help="Data source filter (repeatable). If omitted, includes all.",
)
@click.option(
    "--drop-files/--keep-files",
    default=False,
    help="Delete raw/processed files after successful load for each data source.",
)
@click.option(
    "--only-active/--all",
    default=True,
    help="Use only active data sources/source systems (default: --only-active).",
)
@click.option(
    "--stop-on-error",
    is_flag=True,
    help="Stop update-all at first failed data source.",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def update_all(
    ctx,
    db_uri,
    source_system,
    data_source,
    drop_files,
    only_active,
    stop_on_error,
    debug,
):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    summary = bf.etl.update_all(
        source_system=_to_list_or_none(source_system),
        data_sources=_to_list_or_none(data_source),
        drop_files_on_success=drop_files,
        only_active=only_active,
        stop_on_error=stop_on_error,
    )
    click.echo(
        (
            "update-all summary: "
            f"selected={summary.get('selected', 0)} "
            f"skipped={summary.get('skipped', 0)} "
            f"processed={summary.get('processed', 0)} "
            f"succeeded={summary.get('succeeded', 0)} "
            f"failed={summary.get('failed', 0)}"
        )
    )


@etl.command("restart")
@local_db_uri_option
@click.option("--data-source", multiple=True, help="Data source name (repeatable).")  # noqa E501
@click.option("--source-system", multiple=True, help="Source system name (repeatable).")  # noqa E501
@click.option(
    "--delete-files",
    is_flag=True,
    help="Delete downloaded/processed files when restarting.",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def restart(ctx, db_uri, data_source, source_system, delete_files, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    bf.etl.restart(
        data_source=_to_list_or_none(data_source),
        source_system=_to_list_or_none(source_system),
        delete_files=delete_files,
    )


@etl.command("rollback")
@local_db_uri_option
@click.option(
    "--package-id",
    "package_ids",
    multiple=True,
    type=int,
    help="ETL package id to rollback (repeatable).",
)
@click.option("--data-source", multiple=True, help="Data source name (repeatable).")  # noqa E501
@click.option("--source-system", multiple=True, help="Source system name (repeatable).")  # noqa E501
@click.option(
    "--delete-files",
    is_flag=True,
    help="Delete downloaded/processed files when rolling back data sources.",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def rollback(
    ctx,
    db_uri,
    package_ids,
    data_source,
    source_system,
    delete_files,
    debug,
):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    ok = bf.etl.rollback(
        package_ids=list(package_ids) or None,
        data_source=_to_list_or_none(data_source),
        source_system=_to_list_or_none(source_system),
        delete_files=delete_files,
    )
    if not ok:
        raise click.ClickException("Rollback finished with errors.")


@etl.command("status")
@local_db_uri_option
@click.option("--source-system", multiple=True, help="Source system name (repeatable).")  # noqa E501
@click.option("--data-source", multiple=True, help="Data source name (repeatable).")  # noqa E501
@click.option(
    "--only-active/--all",
    default=False,
    help="Filter only active data sources/source systems (default: --all).",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def status(ctx, db_uri, source_system, data_source, only_active, debug):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)
    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    report_filters = {
        "source_system": _to_list_or_none(source_system),
        "data_sources": _to_list_or_none(data_source),
        "only_active": only_active,
    }

    df_status = bf.report.run("etl_status", **report_filters)
    if df_status is None or df_status.empty:
        click.echo("No data sources found.")
        return

    base_cols = [c for c in ["source_system", "data_source"] if c in df_status.columns]
    base = df_status[base_cols].drop_duplicates().copy()

    df_pkg = bf.report.run("etl_packages", **report_filters)
    latest_load = pd.DataFrame(columns=["data_source", "load_status", "last_execution"])

    if df_pkg is not None and not df_pkg.empty:
        loads = df_pkg.copy()
        if "operation_type" in loads.columns:
            loads = loads[
                loads["operation_type"].astype(str).str.lower().eq("load")
            ].copy()
        else:
            loads = loads.iloc[0:0].copy()

        if not loads.empty:
            loads["_created_at_sort"] = pd.to_datetime(
                loads.get("created_at"), errors="coerce"
            )
            loads["_package_id_sort"] = pd.to_numeric(
                loads.get("package_id"), errors="coerce"
            )
            loads = loads.sort_values(
                ["data_source", "_created_at_sort", "_package_id_sort"],
                ascending=[True, False, False],
            )
            loads_latest = loads.drop_duplicates(subset=["data_source"], keep="first")

            latest_load = loads_latest[["data_source", "load_status"]].copy()
            latest_load["last_execution"] = loads_latest["load_end"].where(
                loads_latest["load_end"].notna(),
                loads_latest["created_at"],
            )

    out = base.merge(latest_load, how="left", on="data_source")
    out["load_result"] = out["load_status"].map(_classify_load_result)
    out["last_execution"] = out["last_execution"].map(_format_ts)
    out = out[["source_system", "data_source", "load_result", "last_execution"]]
    out = out.sort_values(["source_system", "data_source"])

    click.echo(out.to_string(index=False))


# -----------------------------------------------------------------------------
# Recreate DB Indexs
# -----------------------------------------------------------------------------


@etl.command("index")
@local_db_uri_option
@click.option(
    "--group",
    "groups",
    multiple=True,
    help="Index group (repeatable). If omitted, rebuilds all groups.",
)
@click.option("--drop-only", is_flag=True, help="Only drop indexes, do not create.")  # noqa E501
@click.option("--no-drop-first", is_flag=True, help="Do not drop before creating.")  # noqa E501
@click.option(
    "--no-write-mode", is_flag=True, help="Disable DB write-mode tuning hooks."
)
@click.option("--no-read-mode", is_flag=True, help="Disable DB read-mode tuning hooks.")  # noqa E501
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def index(
    ctx, db_uri, groups, drop_only, no_drop_first, no_write_mode, no_read_mode, debug  # noqa E501
):
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    ok, msg = bf.etl.index(
        groups=list(groups) or None,
        drop_only=drop_only,
        drop_first=not no_drop_first,
        set_write_mode=not no_write_mode,
        set_read_mode=not no_read_mode,
    )

    if not ok:
        raise click.ClickException(msg)

    click.echo(msg)
