# biofilter/api/cli/groups/bundle.py
"""
Build a parquet bundle from a declared plan.

Under ADR-003 a bundle is an immutable, versioned release rather than an
export of a live database, which makes the plan that produced it part of
its identity: the same plan rebuilds the same bundle. `bundle plan`
writes that plan; `bundle build` consumes it.

The plan is the authority for a single build. The `active` flag on
`etl_data_sources` only seeds a new plan's defaults, and each DTP's own
JSON config still answers "what within a source" (which INFO fields,
which tissues) — the plan answers "which sources".
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import click

from biofilter.api.cli.common import local_db_uri_option, require_db_uri
from biofilter.biofilter import Biofilter
from biofilter.modules.db.models import ETLDataSource, ETLSourceSystem
from biofilter.utils.version import __version__

PLAN_VERSION = 1

# Data sources whose output is written straight to parquet, never staged
# through the relational database (ADR-003 §2.2). Everything else needs
# the SQLite staging pass, because those DTPs resolve entities against
# each other while they load.
VARIANT_DATA_TYPE = "Variant"


@click.group()
def bundle():
    """Plan and build parquet bundles."""
    pass


def _config_override_for(dtp_script: str) -> str | None:
    """
    Path of the DTP's packaged field/tissue config, when it has one.

    Recorded in the plan so a build states which selection it used, and
    so a rebuild can point at an archived copy instead of whatever the
    installed package currently ships.
    """
    candidate = (
        Path("biofilter/modules/etl/dtps/config") / f"{dtp_script}.json"
    )
    return str(candidate) if candidate.is_file() else None


@bundle.command("plan")
@local_db_uri_option
@click.option(
    "--out",
    "out_path",
    type=click.Path(dir_okay=False, path_type=Path),
    default=Path("bundle_plan.json"),
    show_default=True,
    help="Where to write the plan.",
)
@click.option(
    "--all-sources",
    is_flag=True,
    help=(
        "Enable every source in the plan, not only the ones currently "
        "flagged active. Note the variant sources cover the whole genome: "
        "the gnomAD download alone is about 1.5 TB."
    ),
)
@click.option(
    "--force",
    is_flag=True,
    help="Overwrite an existing plan file.",
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
@click.pass_context
def plan_cmd(ctx, db_uri, out_path: Path, all_sources: bool, force: bool, debug: bool):  # noqa: E501
    """
    Write a build plan listing every data source, flagged for inclusion.

    Edit the file to choose what the build covers, then pass it to
    `bundle build`. Sources are split into the two branches the build
    runs independently: `core`, staged through SQLite, and `variant`,
    written straight to parquet.
    """
    db_uri = require_db_uri(ctx, local_db_uri=db_uri)

    if out_path.exists() and not force:
        raise click.ClickException(
            f"{out_path} already exists. Pass --force to overwrite it, or "
            f"--out to write elsewhere. Refusing to discard a plan that "
            f"may have produced a published bundle."
        )

    bf = Biofilter(db_uri=db_uri, debug_mode=debug)
    bf.db.connect()

    with bf.core.require_db().get_session() as session:
        rows = (
            session.query(ETLDataSource, ETLSourceSystem.name)
            .join(ETLSourceSystem, ETLSourceSystem.id == ETLDataSource.source_system_id)  # noqa: E501
            .order_by(ETLDataSource.data_type, ETLDataSource.name)
            .all()
        )

        core, variant = [], []
        for ds, system_name in rows:
            entry = {
                "name": ds.name,
                "include": bool(all_sources or ds.active),
                "source_system": system_name,
                "data_type": ds.data_type,
                "dtp_script": ds.dtp_script,
                "dtp_version": ds.dtp_version,
                "config_override": _config_override_for(ds.dtp_script),
            }
            (variant if ds.data_type == VARIANT_DATA_TYPE else core).append(entry)  # noqa: E501

    plan = {
        "plan_version": PLAN_VERSION,
        "biofilter_version": __version__,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "_comment": [
            "Build plan for a parquet bundle (ADR-003).",
            "Set 'include' to choose what the build covers. This file is",
            "the authority for one build: the 'active' flag in the",
            "database only seeded these defaults, and each DTP's own JSON",
            "config still governs what is selected *within* a source.",
            "The plan is recorded in the bundle manifest, so an archived",
            "plan identifies how a published bundle was produced.",
            "",
            "Branches run independently and in parallel:",
            "  core    - staged through a throwaway SQLite, then dumped to",
            "            parquet. These DTPs resolve entities against each",
            "            other, so they need a transactional store.",
            "  variant - written straight to parquet, one file per",
            "            chromosome. No relational hop.",
            "",
            "Disk, not CPU, is the binding constraint on the variant",
            "branch: the full gnomAD download is ~1.53 TB while the",
            "largest single chromosome is ~126 GB. The build processes and",
            "discards raw files per chromosome rather than downloading",
            "everything first.",
        ],
        "branches": {
            "core": {
                "staging": "sqlite",
                "sources": core,
            },
            "variant": {
                "staging": "none",
                "sources": variant,
            },
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(plan, indent=2) + "\n", encoding="utf-8")

    n_core = sum(1 for s in core if s["include"])
    n_var = sum(1 for s in variant if s["include"])
    click.echo(f"📝 Plan written to {out_path}")
    click.echo(
        f"   core:    {n_core} of {len(core)} sources included"
    )
    click.echo(
        f"   variant: {n_var} of {len(variant)} sources included"
    )
    click.echo("   Edit 'include' flags, then run: biofilter bundle build")


@bundle.command("build")
@click.option(
    "--plan",
    "plan_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=Path("bundle_plan.json"),
    show_default=True,
    help="Plan to build from (see `bundle plan`).",
)
@click.option(
    "--data-root",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path("biofilter_data"),
    show_default=True,
    help="Where raw, processed and staging live.",
)
@click.option(
    "--restart",
    is_flag=True,
    help=(
        "Discard the staging database and start over. The default is to "
        "resume: an interrupted build re-runs only what is still pending."
    ),
)
@click.option(
    "--keep-raw",
    is_flag=True,
    help=(
        "Keep downloaded files after their output exists. Off by default "
        "because a full genome is ~1.53 TB of raw input against ~126 GB "
        "for the largest single chromosome."
    ),
)
@click.option("--debug", is_flag=True, help="Enable debug logging.")
def build_cmd(plan_path: Path, data_root: Path, restart: bool, keep_raw: bool, debug: bool):  # noqa: E501
    """
    Build a bundle from a plan.

    Creates a throwaway SQLite under <data-root>/staging, runs every
    included source against it, and assembles the bundle only once all of
    them have succeeded. Re-running resumes: finished sources are skipped.
    """
    from biofilter.modules.bundle import BundleBuilder

    plan = json.loads(plan_path.read_text(encoding="utf-8"))

    bf = Biofilter(debug_mode=debug)
    builder = BundleBuilder(
        plan,
        biofilter=bf,
        data_root=data_root,
        logger=bf.core.logger,
        keep_raw=keep_raw,
    )
    result = builder.run(restart=restart)

    click.echo("")
    for outcome in result.outcomes:
        mark = {"done": "✅", "failed": "❌", "skipped": "⏭️"}.get(outcome.status, "•")  # noqa: E501
        freed = f"  (freed {outcome.raw_freed_mb:,.0f} MB)" if outcome.raw_freed_mb else ""  # noqa: E501
        click.echo(f"  {mark} {outcome.name} [{outcome.branch}]{freed}")
        if outcome.detail:
            click.echo(f"      {outcome.detail}")

    if not result.ok:
        raise click.ClickException(
            f"{len(result.failed)} source(s) failed; the bundle was not "
            f"assembled. Run again to resume from where this stopped."
        )

    click.echo("")
    click.echo(f"Staging database: {result.staging_db}")
