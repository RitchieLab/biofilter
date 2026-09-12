"""
Drive a bundle build from a plan.

The build creates a throwaway SQLite, runs both branches against it, and
assembles the bundle only once every source has succeeded. The SQLite is
what makes the build resumable: it holds the ETL package tracking for
*both* branches, so a build that dies knows what it already did. Only the
destination of the data differs — the core branch stages rows there, the
variant branch writes parquet and uses it purely as a ledger.

See ADR-003 (notebooks/Andre/ADR/0003-parquet-native-build-pipeline.md).
"""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

VARIANT_BRANCH = "variant"
CORE_BRANCH = "core"

# Nothing is excluded. The ETL ledger looks like build state, but it is
# the bundle's provenance: `etl_data_sources` and `etl_packages` are what
# let a bundle answer which DTP versions and which source releases built
# it, and the `etl_status` / `etl_packages` reports read them straight
# from a `parquet://` bundle. The 4.2.0 bundle carries all of them.
#
# It matters more now than it did: a bundle cannot be rebuilt once its
# sources move on, so what it says about itself is the only account left.
CONTROL_TABLES: tuple = ()


@dataclass
class SourceOutcome:
    name: str
    branch: str
    status: str          # "done" | "skipped" | "failed"
    detail: str = ""
    raw_freed_mb: float = 0.0


@dataclass
class BuildResult:
    started_at: str
    finished_at: Optional[str] = None
    staging_db: Optional[str] = None
    outcomes: List[SourceOutcome] = field(default_factory=list)
    assembled: bool = False

    @property
    def failed(self) -> List[SourceOutcome]:
        return [o for o in self.outcomes if o.status == "failed"]

    @property
    def ok(self) -> bool:
        return not self.failed


class BundleBuilder:
    """
    Run every source in a plan, then assemble.

    Sources run one at a time inside the variant branch, and that is a
    requirement rather than a simplification. Disk is what binds this
    build: the full gnomAD download is ~1.53 TB while the largest single
    chromosome is ~126 GB, which only fits because raw files are discarded
    as soon as their parquet exists. Running chromosomes concurrently
    would multiply the peak by the number of workers and defeat the
    discard. Downloads gain nothing from concurrency either — measured
    against the gnomAD origin, four parallel range streams moved
    66.6 MB/s against 64.2 MB/s for one, because the local link saturates.
    """

    def __init__(
        self,
        plan: dict,
        *,
        biofilter,
        data_root: Path,
        logger,
        keep_raw: bool = False,
        bundle_dir: Optional[Path] = None,
        assemble: bool = True,
        min_free_gb: float = 0.0,
    ):
        self.plan = plan
        self.bf = biofilter
        self.data_root = Path(data_root)
        self.logger = logger
        self.keep_raw = keep_raw
        self.assemble = assemble
        self.min_free_gb = min_free_gb

        self.download_path = self.data_root / "raw"
        self.processed_path = self.data_root / "processed"
        self.staging_dir = self.data_root / "staging"
        self.bundle_dir = Path(bundle_dir) if bundle_dir else (
            self.data_root / "bundles"
            / datetime.now(timezone.utc).strftime("%Y%m%d")
        )

    # ------------------------------------------------------------------
    # Staging database
    # ------------------------------------------------------------------
    def staging_uri(self) -> str:
        return f"sqlite:///{self.staging_db_path()}"

    def staging_db_path(self) -> Path:
        return self.staging_dir / "bundle_staging.sqlite"

    def prepare_staging(self, restart: bool) -> str:
        """
        Create the staging database, or reuse the one a previous run left.

        Reusing it is what "resume" means here: the ETL package rows in it
        record which steps already completed, for both branches.
        """
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        path = self.staging_db_path()

        if path.exists() and restart:
            self.logger.log(
                f"🗑️  --restart: discarding the existing staging database "
                f"at {path}",
                "WARNING",
            )
            path.unlink()

        uri = self.staging_uri()
        if path.exists():
            self.logger.log(f"♻️  Resuming from {path}", "INFO")
            self.bf.db.connect(uri)
        else:
            self.logger.log(f"🏗️  Creating staging database at {path}", "INFO")
            self.bf.db.create_db(db_uri=uri, overwrite=False)

        self._pin_paths()
        self._sync_plan_selection()
        return uri

    def _sync_plan_selection(self) -> None:
        """
        Make the staging database agree with the plan about what runs.

        The ETL resolves data sources with an `active` filter, so a source
        the plan includes but the database has inactive is silently
        skipped with "No matching active DataSources found". The plan is
        meant to be the authority for a build — that was documented and
        not implemented, so the two disagreed and the build lost four
        sources to it.

        Only the staging database is touched. Whatever database the plan
        was generated from keeps its own flags.
        """
        from biofilter.modules.db.models import ETLDataSource

        wanted = {
            entry["name"]
            for branch in (CORE_BRANCH, VARIANT_BRANCH)
            for entry in self.included_sources(branch)
        }
        if not wanted:
            return

        changed = 0
        with self.bf.core.require_db().get_session() as session:
            for row in (
                session.query(ETLDataSource)
                .filter(ETLDataSource.name.in_(wanted))
                .all()
            ):
                if not row.active:
                    row.active = True
                    changed += 1
            session.commit()

        if changed:
            self.logger.log(
                f"   ⚙️  activated {changed} source(s) the plan includes",
                "INFO",
            )

    def _pin_paths(self) -> None:
        """
        Point the staging database's path settings at this build's root.

        The ETL reads download_path and processed_path from `system_config`
        rather than from its caller, and a freshly seeded database carries
        the packaged defaults. Left alone, the ETL writes under those
        defaults while this builder reclaims disk under --data-root: the
        two halves operate on different directories, the discard deletes
        the wrong copy, and the real download is never freed.
        """
        from biofilter.modules.db.models import SystemConfig

        wanted = {
            "download_path": f"{self.download_path}/",
            "processed_path": f"{self.processed_path}/",
        }
        with self.bf.core.require_db().get_session() as session:
            for key, value in wanted.items():
                row = (
                    session.query(SystemConfig)
                    .filter(SystemConfig.key == key)
                    .one_or_none()
                )
                if row is None:
                    session.add(
                        SystemConfig(
                            key=key,
                            value=value,
                            type="str",
                            description="Set by bundle build from --data-root.",  # noqa: E501
                            editable=True,
                        )
                    )
                elif row.value != value:
                    self.logger.log(
                        f"   ⚙️  {key}: {row.value} → {value}", "INFO"
                    )
                    row.value = value
            session.commit()

    # ------------------------------------------------------------------
    # Running the plan
    # ------------------------------------------------------------------
    def included_sources(self, branch: str) -> List[dict]:
        entries = self.plan.get("branches", {}).get(branch, {}).get("sources", [])  # noqa: E501
        return [e for e in entries if e.get("include")]

    def run(self, *, restart: bool = False) -> BuildResult:
        result = BuildResult(
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        result.staging_db = self.prepare_staging(restart)

        # Core first: its DTPs resolve entities against each other, so
        # they have to run in the order the plan lists them. The variant
        # branch does not depend on any of it and could run alongside,
        # but sharing one SQLite ledger across writers buys contention
        # for no wall-clock gain, since neither branch is CPU-bound while
        # the other is waiting on the network.
        for branch in (CORE_BRANCH, VARIANT_BRANCH):
            sources = self.included_sources(branch)
            if not sources:
                self.logger.log(f"⏭️  Branch '{branch}': nothing included", "INFO")  # noqa: E501
                continue

            self.logger.log(
                f"▶️  Branch '{branch}': {len(sources)} source(s)", "INFO"
            )
            for entry in sources:
                result.outcomes.append(self._run_source(entry, branch))

            if branch == CORE_BRANCH and not self.keep_raw:
                self._discard_core_processed(sources, result)

        if result.failed:
            names = ", ".join(o.name for o in result.failed)
            self.logger.log(
                f"⛔️ {len(result.failed)} source(s) failed: {names}. "
                f"The bundle is NOT assembled — a bundle missing a table "
                f"is indistinguishable from a complete one once published. "
                f"Fix the cause and run again; finished sources are kept "
                f"and will be skipped.",
                "ERROR",
            )
        elif not self.assemble:
            self.logger.log(
                "⏸️  Every included source finished. Assembly skipped "
                "(--no-assemble): this is one stage of a staged build, and "
                "a bundle assembled now would hold only what has run so "
                "far. Run the final stage without the flag.",
                "INFO",
            )
        else:
            result.assembled = self._assemble()

        result.finished_at = datetime.now(timezone.utc).isoformat()
        return result

    def _run_source(self, entry: dict, branch: str) -> SourceOutcome:
        name = entry["name"]
        # The core branch still loads into the staging database; the
        # variant branch stops at transform, because its parquet is the
        # artifact and there is nothing to load it into.
        steps = (
            ["extract", "transform"]
            if branch == VARIANT_BRANCH
            else ["extract", "transform", "load"]
        )

        free_gb = self._free_gb()
        if self.min_free_gb and free_gb < self.min_free_gb:
            detail = (
                f"only {free_gb:,.0f} GB free, below the {self.min_free_gb:,.0f} GB "  # noqa: E501
                f"floor. A single gnomAD chromosome needs up to 67 GB of raw "
                f"before its parquet exists and the raw can go."
            )
            self.logger.log(f"⛔️ {name}: {detail}", "ERROR")
            return SourceOutcome(name, branch, "failed", detail)

        if self._already_done(entry, branch, steps):
            self.logger.log(f"⏭️  {name}: already complete, skipping", "INFO")
            return SourceOutcome(name, branch, "skipped")

        self.logger.log(f"── {name} ({branch})", "INFO")
        try:
            self.bf.etl.update(
                data_sources=[name],
                run_steps=steps,
            )
        except Exception as exc:  # noqa: BLE001
            return SourceOutcome(name, branch, "failed", f"{type(exc).__name__}: {exc}")  # noqa: E501

        if not self._source_succeeded(name, steps):
            return SourceOutcome(
                name, branch, "failed",
                "ETL reported a failed or missing step; see the log above",
            )

        freed = 0.0
        if not self.keep_raw:
            freed = self._discard_raw(entry, branch)

        return SourceOutcome(name, branch, "done", raw_freed_mb=freed)

    def _free_gb(self) -> float:
        """Free space on the volume holding the data root."""
        target = self.data_root if self.data_root.exists() else Path(".")
        usage = shutil.disk_usage(target)
        return usage.free / 1024 ** 3

    def _already_done(
        self,
        entry: dict,
        branch: str,
        steps: List[str],
    ) -> bool:
        """
        Whether this source can be skipped outright on a resumed build.

        This has to happen here rather than inside the ETL. The ETL's own
        skip is decided *after* the fact: `_run_extract` calls the DTP,
        gets a hash back, and only then labels the package up-to-date — so
        the download happens either way. That is affordable for a source
        measured in megabytes, but this build deletes raw files on
        purpose, so a resumed run would re-fetch them just to conclude
        nothing changed. At gnomAD's scale that is 1.53 TB per resume.

        A source is done when every step it needed reached a terminal
        success *and* the output it was supposed to leave is still there.
        The second half matters: without it this would skip a source whose
        parquet was deleted, which is the failure the ETL fix addresses.
        """
        if not self._source_succeeded(entry["name"], steps):
            return False

        # The variant branch's parquet is the artifact and must survive;
        # the core branch's rows live in the staging database, and its
        # processed files are deliberately gone.
        # The variant branch's parquet is the artifact, so it has to still
        # be there. The core branch's rows live in the staging database
        # and its processed files survive until the whole branch is done,
        # so the ledger alone settles it.
        if branch != VARIANT_BRANCH:
            return True

        produced = self.processed_path / entry["source_system"] / entry["name"]  # noqa: E501
        if produced.is_dir() and any(produced.rglob("*.parquet")):
            return True

        self.logger.log(
            f"♻️  {entry['name']} is recorded as done, but its parquet is "
            f"gone. Re-running it.",
            "WARNING",
        )
        return False

    def _source_succeeded(self, name: str, steps: List[str]) -> bool:
        """
        Confirm every step this source needed reached a terminal success.

        Checked against the package ledger rather than trusting the call
        to have raised: a DTP that returns `(False, msg)` marks its
        package failed without raising, and the CLI prints a success line
        at the end of the run either way.
        """
        from biofilter.modules.db.models import ETLDataSource, ETLPackage

        ok_states = {"completed", "up-to-date", "not-applicable"}
        with self.bf.core.require_db().get_session() as session:
            ds = (
                session.query(ETLDataSource)
                .filter(ETLDataSource.name == name)
                .one_or_none()
            )
            if ds is None:
                return False

            for step in steps:
                pkg = (
                    session.query(ETLPackage)
                    .filter(
                        ETLPackage.data_source_id == ds.id,
                        ETLPackage.operation_type == step,
                    )
                    .order_by(ETLPackage.id.desc())
                    .first()
                )
                if pkg is None:
                    return False
                if str(getattr(pkg, f"{step}_status", "")) not in ok_states:
                    return False
        return True

    # ------------------------------------------------------------------
    # Disk reclaim
    # ------------------------------------------------------------------
    def _discard_raw(self, entry: dict, branch: str) -> float:
        """
        Delete what this source no longer needs, and only that.

        The two branches differ in where the canonical data ends up, so
        they differ in what is safe to remove:

        - variant: the processed parquet *is* the bundle's copy, so the
          raw VCFs go and the parquet stays.
        - core: the rows now live in the staging database, so both the
          raw download and the processed intermediate go.

        Without this the variant branch needs 1.53 TB for a full genome
        instead of the ~126 GB one chromosome occupies.
        """
        targets = [self.download_path / entry["source_system"] / entry["name"]]

        freed = 0.0
        for directory in targets:
            if not directory.is_dir():
                continue
            size = sum(
                f.stat().st_size for f in directory.rglob("*") if f.is_file()
            )
            shutil.rmtree(directory)
            freed += size / 1024 ** 2

        if freed:
            self.logger.log(
                f"   🧹 freed {freed:,.0f} MB of intermediates", "INFO"
            )
        return freed


    def _discard_core_processed(self, sources: List[dict], result) -> None:
        """
        Drop the core branch's processed files, once the branch is done.

        Not per source, because a core source's processed output is not
        private to it: every `*_relationships` DTP reads the parquet its
        master wrote, from the master's directory. Worse, relationships
        need *every* master loaded before they run — they resolve across
        sources — so the masters cannot simply be paired with their
        children and reclaimed early either.

        Reclaiming per source deleted those inputs and cost two sources a
        full re-run. Detecting the coupling by name was tried and was
        already wrong: `kegg_relationships` reads `kegg_pathways`, which
        no `<name>_relationships` rule matches.

        Waiting costs nothing worth managing. The core branch's processed
        files total about 72 MB, against gigabytes for a single variant
        chromosome — this whole reclaim exists for the variant branch.

        Skipped entirely if any core source failed, so a resumed build
        still finds what it needs.
        """
        failed = {o.name for o in result.failed}
        if failed:
            self.logger.log(
                f"   ↺ keeping processed files: {len(failed)} core "
                f"source(s) failed and a resume will need them",
                "INFO",
            )
            return

        freed = 0.0
        for entry in sources:
            directory = (
                self.processed_path / entry["source_system"] / entry["name"]
            )
            if not directory.is_dir():
                continue
            freed += sum(
                f.stat().st_size for f in directory.rglob("*") if f.is_file()
            ) / 1024 ** 2
            shutil.rmtree(directory)

        if freed:
            self.logger.log(
                f"   🧹 freed {freed:,.0f} MB of core intermediates", "INFO"
            )

    # ------------------------------------------------------------------
    # Assembly
    # ------------------------------------------------------------------
    def _assemble(self) -> bool:
        """
        Assemble the bundle from what the branches produced.

        Deliberately the last step and all-or-nothing: every source has to
        have succeeded before anything is published, because a bundle
        missing a table looks exactly like a complete one to whoever reads
        it.
        """
        from biofilter.modules.db.transfer import export_full_clone
        from biofilter.utils.version import __version__

        out = self.bundle_dir
        if out.exists() and any(out.iterdir()):
            self.logger.log(
                f"❌ {out} already holds a bundle. Refusing to overwrite: a "
                f"published bundle is the only surviving account of the "
                f"data it carries, since the sources it was built from "
                f"have moved on. Pass a different --out.",
                "ERROR",
            )
            return False

        out.mkdir(parents=True, exist_ok=True)
        tables_dir = out / "tables"

        self.logger.log(f"📦 Assembling bundle at {out}", "INFO")

        # Core tables come straight out of the staging database. The ETL's
        # own bookkeeping is build state, not bundle content.
        engine = self.bf.core.require_db().engine

        # Only exclude what is actually there: export_full_clone raises on
        # an unknown name, and the staging database is built with
        # create_all, so it has no alembic_version to exclude.
        from sqlalchemy import inspect as sa_inspect

        present = set(sa_inspect(engine).get_table_names())
        export_full_clone(
            engine,
            out,
            biofilter_version=__version__,
            schema_version=__version__,
            fmt="parquet",
            exclude_tables=[t for t in CONTROL_TABLES if t in present],
            checksums=False,
        )

        moved = self._move_variant_tables(tables_dir)
        bundle_id = self._finalise_manifest(out, moved)
        self._stamp_metadata(tables_dir, bundle_id)
        self._resync_manifest_entry(out, "biofilter_metadata")
        self._write_build_record(out)

        self.logger.log(
            f"✅ Bundle assembled: {out} "
            f"({len(moved)} variant file(s) moved in)",
            "INFO",
        )
        return True

    def _move_variant_tables(self, tables_dir: Path) -> List[Path]:
        """
        Move each variant parquet into a directory named for its table.

        A subdirectory per table is what makes the reader work. It groups
        files two ways — `<table>.parquet` as one view, `<table>/` as one
        view over everything beneath it — and has no rule for sibling
        files sharing a prefix. Left flat, `variant_masters_chr1.parquet`
        through `_chr25.parquet` would register as 25 separate views and
        `variant_masters` would not exist at all.

        Moved rather than copied: the variant parquets are the bulk of a
        bundle (~15.6 GB for a full genome) and copying would need both
        copies on disk at once. The cost is that `processed/` is left
        empty, so re-assembling means re-running the transforms — which is
        why the build refuses to overwrite an existing bundle rather than
        quietly rebuilding one.
        """
        moved: List[Path] = []
        for entry in self.included_sources(VARIANT_BRANCH):
            src_dir = self.processed_path / entry["source_system"] / entry["name"]  # noqa: E501
            if not src_dir.is_dir():
                continue
            for parquet in sorted(src_dir.rglob("*.parquet")):
                table = self._table_of(parquet.stem)
                target_dir = tables_dir / table
                target_dir.mkdir(parents=True, exist_ok=True)
                target = target_dir / parquet.name
                shutil.move(str(parquet), str(target))
                moved.append(target)
        return moved

    @staticmethod
    def _table_of(stem: str) -> str:
        """`variant_masters_chr21` -> `variant_masters`."""
        marker = "_chr"
        idx = stem.rfind(marker)
        if idx == -1:
            return stem
        suffix = stem[idx + len(marker):]
        return stem[:idx] if suffix.isdigit() else stem

    def _finalise_manifest(self, out: Path, moved: List[Path]) -> str:
        """
        Fold the variant tables into the manifest and stamp the bundle id.

        `export_full_clone` only sees what the engine holds, so the
        variant parquets — produced outside any database — are absent from
        the manifest it writes. Each is added with the branch that made
        it, because the two branches carry different guarantees and a
        reader should be able to tell them apart.

        The id is derived from content, not from the run: the same tables
        with the same rows yield the same id, so it identifies and
        verifies the artifact instead of merely labelling an execution.
        `created_at` is excluded for that reason.
        """
        manifest_path = out / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        for table in manifest.get("tables", []):
            table["branch"] = CORE_BRANCH

        import pyarrow.parquet as pq

        for path in moved:
            manifest["tables"].append({
                "name": path.stem,
                "table": self._table_of(path.stem),
                "branch": VARIANT_BRANCH,
                "rows": pq.ParquetFile(path).metadata.num_rows,
                "file": str(path.relative_to(out)),
                "bytes": path.stat().st_size,
            })

        # biofilter_metadata is excluded from the fingerprint because it
        # is rewritten to carry the id itself; including it would make the
        # id depend on its own value and stop it being recomputable from
        # the finished bundle.
        fingerprint = json.dumps(
            sorted(
                (t.get("name"), t.get("rows"), t.get("bytes"))
                for t in manifest["tables"]
                if t.get("name") != "biofilter_metadata"
            ),
            sort_keys=True,
        )
        bundle_id = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:16]  # noqa: E501
        manifest["bundle_id"] = bundle_id
        manifest["plan"] = "bundle_plan.json"
        manifest["build_record"] = "build_record.json"

        manifest_path.write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )
        self.logger.log(f"   bundle_id: {bundle_id}", "INFO")
        return bundle_id

    def _resync_manifest_entry(self, out: Path, table_name: str) -> None:
        """
        Refresh one table's size in the manifest after rewriting its file.

        `_stamp_metadata` rewrites `biofilter_metadata.parquet` after the
        manifest has already recorded its size, so the declared bytes stop
        matching the file. `db verify` catches that as a size mismatch —
        which is the check doing its job, and the reason this exists.

        The bundle id is unaffected: that table is excluded from the
        fingerprint precisely because it is rewritten to carry the id.
        """
        manifest_path = out / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for table in manifest.get("tables", []):
            if table.get("name") != table_name:
                continue
            path = out / table["file"]
            if path.is_file():
                table["bytes"] = path.stat().st_size
                if table.get("sha256"):
                    table["sha256"] = None
        manifest_path.write_text(
            json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
        )

    def _stamp_metadata(self, tables_dir: Path, bundle_id: str) -> None:
        """
        Rewrite `biofilter_metadata` so the bundle stops misreporting itself.

        The row is seeded when the database is created and nothing ever
        updated it, so every bundle carried the values it was born with:
        the 4.2.0 bundle claims schema 4.1.0 with a null build_hash and a
        created_at from the day its source database was first made. A
        reader taking the version from the tables — the normal path — got
        the wrong answer.

        Written here rather than during the ETL because the id is derived
        from the finished tables, so it cannot exist any earlier.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        from biofilter.utils.version import __version__

        path = tables_dir / "biofilter_metadata.parquet"
        if not path.is_file():
            self.logger.log(
                "⚠️  biofilter_metadata is absent from the bundle; the "
                "version it reports cannot be corrected.",
                "WARNING",
            )
            return

        table = pq.read_table(path)
        now = datetime.now(timezone.utc)
        replacements = {
            "schema_version": __version__,
            "schema_revision": __version__,
            "etl_version": __version__,
            "build_hash": bundle_id,
            "description": f"Bundle {bundle_id}",
            "updated_at": now.replace(tzinfo=None),
        }

        columns, names = [], []
        for field in table.schema:
            names.append(field.name)
            if field.name in replacements:
                value = replacements[field.name]
                # The column types come from whatever the export produced,
                # and timestamps land as strings there, so coerce rather
                # than assume.
                if isinstance(value, datetime) and pa.types.is_string(field.type):  # noqa: E501
                    value = value.isoformat(sep=" ")
                columns.append(
                    pa.array([value] * table.num_rows, type=field.type)
                )
            else:
                columns.append(table.column(field.name))

        pq.write_table(
            pa.Table.from_arrays(columns, names=names),
            path,
            compression="zstd",
        )
        self.logger.log(
            f"   biofilter_metadata: version {__version__}, "
            f"build_hash {bundle_id}",
            "INFO",
        )

    def _write_build_record(self, out: Path) -> None:
        """
        Write the plan and a record of how the build ran, beside the data.

        These travel with the bundle because they are the only account of
        it that survives. A source pinned to `current` will have moved on,
        so the bundle cannot be rebuilt — what it holds has to be legible
        from the bundle itself, years later.
        """
        from biofilter.modules.db.models import ETLDataSource, ETLPackage

        (out / "bundle_plan.json").write_text(
            json.dumps(self.plan, indent=2) + "\n", encoding="utf-8"
        )

        steps = []
        with self.bf.core.require_db().get_session() as session:
            rows = (
                session.query(ETLPackage, ETLDataSource)
                .join(ETLDataSource, ETLDataSource.id == ETLPackage.data_source_id)  # noqa: E501
                .order_by(ETLPackage.id)
                .all()
            )
            for pkg, ds in rows:
                steps.append({
                    "data_source": ds.name,
                    "dtp_script": ds.dtp_script,
                    "dtp_version": ds.dtp_version,
                    "source_url": ds.source_url,
                    "step": pkg.operation_type,
                    "status": pkg.status,
                    "hash": pkg.extract_hash,
                    "started": str(
                        pkg.extract_start or pkg.transform_start or pkg.load_start or ""  # noqa: E501
                    ),
                    "finished": str(
                        pkg.load_end or pkg.transform_end or pkg.extract_end or ""  # noqa: E501
                    ),
                })

        (out / "build_record.json").write_text(
            json.dumps(
                {
                    "built_at": datetime.now(timezone.utc).isoformat(),
                    "biofilter_version": self.plan.get("biofilter_version"),
                    "steps": steps,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
