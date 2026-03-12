from __future__ import annotations

import importlib
import os
import glob
import shutil
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional, Sequence, Any

from sqlalchemy import MetaData, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

from biofilter.modules.db.database import Database
from biofilter.utils.logger import Logger
from biofilter.modules.db.models import ETLPackage, ETLDataSource, ETLSourceSystem
from biofilter.modules.etl.mixins.base_dtp_turning import DBTuningMixin


ETL_TABLE_PREFIX = "etl_"
PURGE_ORDER_OVERRIDE = [
    "VariantLocus",
    "VariantMaster",
    "EntityRelationship",
    "EntityAlias",
    "Entity",
]


def _is_etl_table(table_name: str) -> bool:
    return table_name.lower().startswith(ETL_TABLE_PREFIX)


@dataclass(frozen=True)
class StepResult:
    ok: bool
    message: str
    hash_value: Optional[str] = None


class ETLManager:
    """
    ETL Orchestrator.

    Design:
    - One SQLAlchemy session per DataSource run (extract→transform→load).
    - ETLPackage is updated throughout the run using the same session.
    - DTPs receive BOTH: `session` (for ORM) and `db` (for engine/dialect/mappings).
    """

    def __init__(self, debug_mode: bool, db: Database, logger: Optional[Logger] = None):
        self.debug_mode = debug_mode
        self.db = db
        self.logger = logger or Logger()

        # Small in-memory cache for dtp module imports
        self._dtp_module_cache: dict[str, Any] = {}

    # ---------------------------------------------------------------------
    # INDEX MANAGEMENT
    # ---------------------------------------------------------------------
    def rebuild_indexes(
        self,
        index_group: Optional[Iterable[str]] = None,
        drop_only: bool = False,
        drop_first: bool = True,
        set_write_mode: bool = True,
        set_read_mode: bool = True,
    ) -> tuple[bool, str]:
        """
        Rebuild (drop/create) indexes for selected groups using DBTuningMixin specs.
        Uses a short-lived session since this is admin-only.
        """
        with self.db.get_session() as session:
            tuning = DBTuningMixin()._bind_db_tuning(session, self.logger)

            index_catalog = {
                "entity": tuning.get_entity_index_specs,
                "entity_relationship": tuning.get_entity_relationship_index_specs,
                "entity_location": tuning.get_entity_location_index_specs,
                "gene": tuning.get_gene_index_specs,
                "protein": tuning.get_protein_index_specs,
                "go": tuning.get_go_index_specs,
                "pathway": tuning.get_pathway_index_specs,
                "gwas": tuning.get_variant_gwas_index_specs,
                "disease": tuning.get_disease_index_specs,
                "chemical": tuning.get_chemical_index_specs,
                "variant": tuning.get_variant_master_index_specs,
                "snp": tuning.get_snp_index_specs,
            }

            aliases = {
                "entity": "entity",
                "entities": "entity",
                "entity_relationship": "entity_relationship",
                "entity_location": "entity_location",
                "go": "go",
                "pathway": "pathway",
                "pathways": "pathway",
                "gwas": "gwas",
                "disease": "disease",
                "diseases": "disease",
                "chemical": "chemical",
                "chemicals": "chemical",
                "gene": "gene",
                "genes": "gene",
                "variant": "variant",
                "variants": "variant",
                "protein": "protein",
                "proteins": "protein",
            }

            selected = self._select_index_groups(index_group, index_catalog, aliases)
            if not selected:
                msg = "❌ No valid index groups selected. Nothing to do."
                self.logger.log(msg, "ERROR")
                return False, msg

            total_warnings = 0

            if set_write_mode:
                tuning.db_write_mode()

            # Drop
            if drop_only or drop_first:
                self.logger.log("🧹 Dropping indexes...", "INFO")
                for group_name, spec_fn in selected.items():
                    try:
                        specs = spec_fn
                        # specs = spec_fn()  # ✅ call it
                        if specs:
                            tuning.drop_indexes(specs)
                    except Exception as e:
                        total_warnings += 1
                        self.logger.log(
                            f"⚠️ Failed to drop indexes for {group_name}: {e}", "WARNING"
                        )

                if drop_only:
                    if set_read_mode:
                        tuning.db_read_mode()
                    final = f"✅ Dropped indexes with {total_warnings} warning(s)."
                    self.logger.log(final, "WARNING" if total_warnings else "INFO")
                    return (total_warnings == 0), final

            # Create
            self.logger.log("🏗️ Creating indexes...", "INFO")
            for group_name, spec_fn in selected.items():
                try:
                    specs = spec_fn
                    # specs = spec_fn()  # ✅ call it
                    if not specs:
                        continue
                    self.logger.log(f"🏗️ Creating indexes for {group_name}...", "INFO")
                    tuning.create_indexes(specs)
                except Exception as e:
                    total_warnings += 1
                    self.logger.log(
                        f"⚠️ Failed to create indexes for {group_name}: {e}", "WARNING"
                    )

            if set_read_mode:
                tuning.db_read_mode()

            final = f"✅ Index rebuild finished with {total_warnings} warning(s)."
            self.logger.log(final, "WARNING" if total_warnings else "INFO")
            return True, final

    def _select_index_groups(self, index_group, catalog, aliases):
        if not index_group:
            # return catalog
            return dict(catalog)  # make a shallow copy for safety

        selected = {}
        invalid = []
        for g in index_group:
            key = aliases.get(str(g).strip().lower())
            if key and key in catalog:
                selected[key] = catalog[key]
            else:
                invalid.append(g)

        if invalid:
            self.logger.log(
                f"⚠️ Unknown index groups ignored: {invalid}. Valid groups: {sorted(catalog.keys())}",
                "WARNING",
            )
        return selected

    # ---------------------------------------------------------------------
    # ETL MAIN ENTRY
    # ---------------------------------------------------------------------
    def start_process(
        self,
        source_system: Optional[Sequence[str]] = None,
        data_sources: Optional[Sequence[str]] = None,
        download_path: Optional[str] = None,
        processed_path: Optional[str] = None,
        run_steps: Optional[Sequence[str]] = None,
        force_steps: Optional[Sequence[str]] = None,
        use_conflict_csv: bool = False,
    ) -> None:
        if run_steps is None:
            run_steps = ["extract", "transform", "load"]
        if force_steps is None:
            force_steps = []

        run_steps = [s.lower().strip() for s in run_steps]
        force_steps = [s.lower().strip() for s in force_steps]

        # normalize inputs
        if isinstance(source_system, str):
            source_system = [source_system]
        if isinstance(data_sources, str):
            data_sources = [data_sources]

        if not source_system and not data_sources:
            self.logger.log(
                "❌ No source_system or data_sources provided. Aborting.", "ERROR"
            )
            return

        # Query DataSources in a short-lived session
        with self.db.get_session() as session:
            ds_ids = self._resolve_datasource_ids(session, source_system, data_sources)

        if not ds_ids:
            self.logger.log("⚠️ No matching active DataSources found.", "WARNING")
            return

        # Run each datasource with its OWN session (keeps package updates consistent per ds)
        for ds_id in ds_ids:
            with self.db.get_session() as session:
                ds = self._load_datasource(session, ds_id)
                self._run_one_datasource(
                    session=session,
                    ds=ds,
                    download_path=download_path,
                    processed_path=processed_path,
                    run_steps=run_steps,
                    force_steps=force_steps,
                    use_conflict_csv=use_conflict_csv,
                )

    def _resolve_datasource_ids(
        self,
        session: Session,
        source_system: Optional[Sequence[str]],
        data_sources: Optional[Sequence[str]],
    ) -> list[int]:
        q = session.query(ETLDataSource.id).filter(ETLDataSource.active.is_(True))

        if source_system:
            q = q.join(ETLSourceSystem).filter(
                ETLSourceSystem.name.in_(list(source_system))
            )

        if data_sources:
            q = q.filter(ETLDataSource.name.in_(list(data_sources)))

        return [row[0] for row in q.all()]

    def _load_datasource(self, session: Session, ds_id: int) -> ETLDataSource:
        ds = (
            session.query(ETLDataSource)
            .options(selectinload(ETLDataSource.source_system))
            .filter(ETLDataSource.id == ds_id)
            .one()
        )
        return ds

    def _run_one_datasource(
        self,
        session: Session,
        ds: ETLDataSource,
        download_path: Optional[str],
        processed_path: Optional[str],
        run_steps: Sequence[str],
        force_steps: Sequence[str],
        use_conflict_csv: bool,
    ) -> None:
        self.logger.log(
            f"🔁 Starting ETL for '{ds.name}' (source_system_id={ds.source_system_id}, data_source_id={ds.id})",
            "INFO",
        )

        try:
            module = self._load_dtp_module(ds)

            # ---- Extract
            if "extract" in run_steps:
                self._run_extract(
                    session=session,
                    module=module,
                    ds=ds,
                    download_path=download_path,
                    force_steps=force_steps,
                    use_conflict_csv=use_conflict_csv,
                )

            # ---- Transform
            if "transform" in run_steps:
                self._run_transform(
                    session=session,
                    module=module,
                    ds=ds,
                    download_path=download_path,
                    processed_path=processed_path,
                    force_steps=force_steps,
                    use_conflict_csv=use_conflict_csv,
                )

            # ---- Load
            if "load" in run_steps:
                self._run_load(
                    session=session,
                    module=module,
                    ds=ds,
                    processed_path=processed_path,
                    force_steps=force_steps,
                    use_conflict_csv=use_conflict_csv,
                )

            self.logger.log(f"🎉 ETL pipeline finished for '{ds.name}'", "INFO")

        except (SQLAlchemyError, Exception) as e:
            self.logger.log(f"❌ ETL failed for '{ds.name}': {e}", "ERROR")
            try:
                session.rollback()
            except Exception:
                pass

    # ---------------------------------------------------------------------
    # DTP LOADING
    # ---------------------------------------------------------------------
    def _load_dtp_module(self, ds: ETLDataSource):
        script_name = (ds.dtp_script or "").lower().strip()
        if not script_name:
            raise ValueError(f"DataSource '{ds.name}' has empty dtp_script")

        if script_name in self._dtp_module_cache:
            return self._dtp_module_cache[script_name]

        module_path = f"biofilter.modules.etl.dtps.{script_name}"
        module = importlib.import_module(module_path)
        self._dtp_module_cache[script_name] = module
        return module

    # ---------------------------------------------------------------------
    # PACKAGE HELPERS
    # ---------------------------------------------------------------------
    def _create_package(
        self, session: Session, data_source: ETLDataSource
    ) -> Optional[ETLPackage]:
        try:
            pkg = ETLPackage(
                data_source_id=data_source.id,
                status="running",
                operation_type="running",
                version_tag=None,
                note=None,
                active=True,
            )
            session.add(pkg)
            session.commit()
            self.logger.log(
                f"📦 Created ETLPackage ID={pkg.id} for data source '{data_source.name}'",
                "DEBUG",
            )
            return pkg
        except Exception as e:
            self.logger.log(f"❌ Error creating ETLPackage: {e}", "ERROR")
            try:
                session.rollback()
            except Exception:
                pass
            return None

    def _find_last_package(
        self,
        session: Session,
        ds_id: int,
        operation_type: str,
        ok_statuses: Sequence[str],
        order_field,
        extra_filters: Optional[list[Any]] = None,
    ) -> Optional[ETLPackage]:
        q = session.query(ETLPackage).filter(
            ETLPackage.data_source_id == ds_id,
            ETLPackage.operation_type == operation_type,
            ETLPackage.status.in_(list(ok_statuses)),
        )
        if extra_filters:
            for f in extra_filters:
                q = q.filter(f)
        return q.order_by(order_field.desc()).first()

    # ---------------------------------------------------------------------
    # STEP: EXTRACT
    # ---------------------------------------------------------------------
    def _run_extract(
        self,
        session: Session,
        module,
        ds: ETLDataSource,
        download_path: Optional[str],
        force_steps: Sequence[str],
        use_conflict_csv: bool,
    ) -> None:
        pkg = self._create_package(session, ds)
        if not pkg:
            self.logger.log(
                f"❌ Could not create extract package for '{ds.name}'.", "ERROR"
            )
            return

        pkg.operation_type = "extract"
        pkg.status = "running"
        pkg.extract_status = "running"
        pkg.extract_start = datetime.now()
        session.commit()

        dtp = module.DTP(
            logger=self.logger,
            debug_mode=self.debug_mode,
            datasource=ds,
            package=pkg,
            session=session,
            db=self.db,
            use_conflict_csv=use_conflict_csv,
        )

        ok, message, file_hash = dtp.extract(raw_dir=download_path)

        pkg.extract_end = datetime.now()
        pkg.extract_hash = file_hash

        if ok:
            last_same_hash = self._find_last_package(
                session=session,
                ds_id=ds.id,
                operation_type="extract",
                ok_statuses=["completed", "up-to-date"],
                order_field=ETLPackage.extract_end,
                extra_filters=[ETLPackage.extract_hash == file_hash],
            )

            if last_same_hash and "extract" not in force_steps:
                pkg.status = "up-to-date"
                pkg.extract_status = "up-to-date"
                pkg.stats = {
                    "note": "extract up-to-date (hash already processed)",
                    "previous_package_id": last_same_hash.id,
                    "hash": file_hash,
                }
                self.logger.log(
                    f"✅ [Extract] Up-to-date for '{ds.name}' (hash={file_hash})",
                    "INFO",
                )
            else:
                pkg.status = "completed"
                pkg.extract_status = "completed"
                pkg.stats = {"hash": file_hash}
                self.logger.log(
                    f"✅ [Extract] Completed for '{ds.name}' (hash={file_hash})", "INFO"
                )
        else:
            pkg.status = "failed"
            pkg.extract_status = "failed"
            pkg.stats = {"error": message, "step": "extract"}
            self.logger.log(message, "ERROR")
            self.logger.log(
                f"⛔️ ETL halted for '{ds.name}' due to extract failure", "ERROR"
            )

        session.commit()

    # ---------------------------------------------------------------------
    # STEP: TRANSFORM
    # ---------------------------------------------------------------------
    def _run_transform(
        self,
        session: Session,
        module,
        ds: ETLDataSource,
        download_path: Optional[str],
        processed_path: Optional[str],
        force_steps: Sequence[str],
        use_conflict_csv: bool,
    ) -> None:
        last_extract = self._find_last_package(
            session=session,
            ds_id=ds.id,
            operation_type="extract",
            ok_statuses=["completed", "up-to-date"],
            order_field=ETLPackage.extract_end,
        )

        if not last_extract:
            msg = (
                f"⚠️ No successful extract found for '{ds.name}' — cannot run transform."
            )
            self.logger.log(msg, "WARNING")

            pkg = self._create_package(session, ds)
            if not pkg:
                return
            pkg.operation_type = "transform"
            pkg.status = "failed"
            pkg.extract_status = "not-applicable"
            pkg.transform_status = "failed"
            pkg.load_status = "not-applicable"
            pkg.transform_start = datetime.now()
            pkg.transform_end = datetime.now()
            pkg.stats = {"error": msg, "step": "transform"}
            session.commit()
            return

        last_transform = self._find_last_package(
            session=session,
            ds_id=ds.id,
            operation_type="transform",
            ok_statuses=["completed", "up-to-date"],
            order_field=ETLPackage.transform_end,
            extra_filters=[ETLPackage.transform_hash == last_extract.extract_hash],
        )

        if last_transform and "transform" not in force_steps:
            pkg = self._create_package(session, ds)
            if not pkg:
                return
            pkg.operation_type = "transform"
            pkg.status = "not-applicable"  # keep your current semantics
            pkg.transform_status = "not-applicable"
            pkg.extract_status = "not-applicable"
            pkg.load_status = "not-applicable"
            pkg.transform_start = datetime.now()
            pkg.transform_end = datetime.now()
            pkg.transform_hash = last_extract.extract_hash
            pkg.stats = {
                "note": "transform skipped (already completed for this hash)",
                "source_extract_package_id": last_extract.id,
                "previous_transform_package_id": last_transform.id,
                "hash": last_extract.extract_hash,
            }
            session.commit()
            self.logger.log(
                f"⚙️  [Transform] Up-to-date for '{ds.name}' (package_id={pkg.id})",
                "INFO",
            )
            return

        pkg = self._create_package(session, ds)
        if not pkg:
            return

        pkg.operation_type = "transform"
        pkg.status = "running"
        pkg.transform_status = "running"
        pkg.transform_start = datetime.now()
        pkg.transform_hash = last_extract.extract_hash
        pkg.extract_status = "not-applicable"
        session.commit()

        self.logger.log(
            f"⚙️ [Transform] Running for '{ds.name}' (package_id={pkg.id})", "INFO"
        )

        dtp = module.DTP(
            logger=self.logger,
            debug_mode=self.debug_mode,
            datasource=ds,
            package=pkg,
            session=session,
            db=self.db,
            use_conflict_csv=use_conflict_csv,
        )

        ok, message = dtp.transform(download_path, processed_path)

        pkg.transform_end = datetime.now()

        if ok:
            pkg.status = "completed"
            pkg.transform_status = "completed"
            self.logger.log(f"✅ [Transform] Completed for '{ds.name}'", "INFO")
        else:
            pkg.status = "failed"
            pkg.transform_status = "failed"
            pkg.stats = {"error": message, "step": "transform"}
            self.logger.log(message, "ERROR")
            self.logger.log(f"❌ [Transform] Failed for '{ds.name}'", "ERROR")

        session.commit()

    # ---------------------------------------------------------------------
    # STEP: LOAD
    # ---------------------------------------------------------------------
    def _run_load(
        self,
        session: Session,
        module,
        ds: ETLDataSource,
        processed_path: Optional[str],
        force_steps: Sequence[str],
        use_conflict_csv: bool,
    ) -> None:
        last_transform_ok = self._find_last_package(
            session=session,
            ds_id=ds.id,
            operation_type="transform",
            ok_statuses=["completed", "up-to-date"],
            order_field=ETLPackage.transform_end,
        )

        if not last_transform_ok:
            msg = f"⚠️ No successful transform found for '{ds.name}' — cannot run load."
            self.logger.log(msg, "WARNING")

            pkg = self._create_package(session, ds)
            if not pkg:
                return
            pkg.operation_type = "load"
            pkg.status = "failed"
            pkg.extract_status = "not-applicable"
            pkg.transform_status = "not-applicable"
            pkg.load_status = "failed"
            pkg.load_start = datetime.now()
            pkg.load_end = datetime.now()
            pkg.stats = {"error": msg, "step": "load"}
            session.commit()
            return

        last_load = self._find_last_package(
            session=session,
            ds_id=ds.id,
            operation_type="load",
            ok_statuses=["completed", "up-to-date"],
            order_field=ETLPackage.load_end,
            extra_filters=[ETLPackage.load_hash == last_transform_ok.transform_hash],
        )

        if last_load and "load" not in force_steps:
            pkg = self._create_package(session, ds)
            if not pkg:
                return
            pkg.operation_type = "load"
            pkg.status = "not-applicable"  # keep your current semantics
            pkg.load_status = "not-applicable"
            pkg.extract_status = "not-applicable"
            pkg.transform_status = "not-applicable"
            pkg.load_start = datetime.now()
            pkg.load_end = datetime.now()
            pkg.load_hash = last_transform_ok.transform_hash
            pkg.stats = {
                "note": "load skipped (already completed for this hash)",
                "source_transform_package_id": last_transform_ok.id,
                "previous_load_package_id": last_load.id,
                "hash": last_transform_ok.transform_hash,
            }
            session.commit()
            self.logger.log(
                f"🚚 [Load] Up-to-date for '{ds.name}' (package_id={pkg.id})", "INFO"
            )
            return

        pkg = self._create_package(session, ds)
        if not pkg:
            return

        pkg.operation_type = "load"
        pkg.status = "running"
        pkg.load_status = "running"
        pkg.load_start = datetime.now()
        pkg.load_hash = last_transform_ok.transform_hash
        pkg.extract_status = "not-applicable"
        pkg.transform_status = "not-applicable"
        session.commit()

        self.logger.log(
            f"🚚 [Load] Running for '{ds.name}' (package_id={pkg.id})", "INFO"
        )

        dtp = module.DTP(
            logger=self.logger,
            debug_mode=self.debug_mode,
            datasource=ds,
            package=pkg,
            session=session,
            db=self.db,
            use_conflict_csv=use_conflict_csv,
        )

        ok, message = dtp.load(processed_path)

        pkg.load_end = datetime.now()

        if ok:
            pkg.status = "completed"
            pkg.load_status = "completed"
            self.logger.log(f"✅ [Load] Completed for '{ds.name}'", "INFO")
        else:
            pkg.status = "failed"
            pkg.load_status = "failed"
            pkg.stats = {"error": message, "step": "load"}
            self.logger.log(message, "ERROR")
            self.logger.log(f"❌ [Load] Failed for '{ds.name}'", "ERROR")

        session.commit()

    # ---------------------------------------------------------------------
    # UTILS
    # ---------------------------------------------------------------------
    def _delete_matching_files(self, path_pattern: str):
        for file_path in glob.glob(path_pattern):
            try:
                if os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    self.logger.log(f"🧹 Deleted directory: {file_path}", "DEBUG")
                else:
                    os.remove(file_path)
                    self.logger.log(f"🗑️ Deleted file: {file_path}", "DEBUG")
            except Exception as e:
                self.logger.log(f"⚠️ Could not delete {file_path}: {e}", "WARNING")

    def _simple_purge_by_data_source(self, session: Session, ds_id: int) -> None:
        engine = session.get_bind()
        metadata = MetaData()
        metadata.reflect(bind=engine)

        candidates = []
        for tname, table in metadata.tables.items():
            if _is_etl_table(tname):
                continue
            if "data_source_id" in table.columns:
                candidates.append(table)

        if not candidates:
            self.logger.log("ℹ️ No non-ETL tables with `data_source_id` found.", "INFO")
            return

        ordered = self._order_for_delete(candidates, metadata)

        total = 0
        for table in ordered:
            # Optional: skip count unless debug
            if self.debug_mode:
                cnt = (
                    session.execute(
                        select(func.count())
                        .select_from(table)
                        .where(table.c.data_source_id == ds_id)
                    ).scalar()
                    or 0
                )
                if cnt == 0:
                    continue
                self.logger.log(
                    f"🗑️  Deleting {cnt} rows from {table.name} (data_source_id={ds_id})",
                    "INFO",
                )

            session.execute(table.delete().where(table.c.data_source_id == ds_id))
            # We can't easily know affected rowcount reliably across DBs; commit at end.

        session.commit()
        self.logger.log(f"✅ Simple purge complete for data_source_id={ds_id}.", "INFO")

    def _order_for_delete(self, candidates, metadata):
        cand_by_name = {t.name: t for t in candidates}

        override = [cand_by_name[n] for n in PURGE_ORDER_OVERRIDE if n in cand_by_name]
        rest = [t for t in candidates if t.name not in PURGE_ORDER_OVERRIDE]

        if rest:
            graph = {t.name: set() for t in rest}
            names = set(graph.keys())

            for t in rest:
                for fk in t.foreign_keys:
                    parent = fk.column.table.name
                    if parent in names:
                        graph[parent].add(t.name)

            ordered_names = []
            no_incoming = [
                n for n in graph if not any(n in cs for cs in graph.values())
            ]

            while no_incoming:
                n = no_incoming.pop()
                ordered_names.append(n)
                for m in list(graph[n]):
                    graph[n].remove(m)
                    if not any(m in cs for cs in graph.values()):
                        no_incoming.append(m)

            remaining = [n for n, cs in graph.items() if cs]
            ordered_rest = [cand_by_name[n] for n in ordered_names] + [
                cand_by_name[n] for n in remaining
            ]
        else:
            ordered_rest = []

        ordered = override + ordered_rest
        if not ordered:
            sorted_all = list(metadata.sorted_tables)
            ordered = [t for t in reversed(sorted_all) if t in candidates]

        return ordered
