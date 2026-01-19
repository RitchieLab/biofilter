from __future__ import annotations

import os
import shutil
import glob
import json
import importlib
from typing import Callable, Iterable, Sequence, Any, Union, List, Optional
from datetime import datetime


from sqlalchemy import MetaData, func, select  # inspect
from biofilter.modules.db.database import Database
from sqlalchemy.exc import SQLAlchemyError
from biofilter.utils.logger import Logger
from biofilter.modules.db.models import (
    ETLPackage,
    ETLDataSource,
    ETLSourceSystem,
    # ETLLog,
)  # noqa: E501
from biofilter.modules.etl.mixins.base_dtp_turning import DBTuningMixin

# IndexSpecFn = Callable[[], list[tuple[str, list[str]]]]  # seu formato atual

ETL_TABLE_PREFIX = "etl_"
PURGE_ORDER_OVERRIDE = [
    "VariantLocus",
    "VariantMaster",
    "EntityRelationship",
    "EntityAlias",
    "Entity",
    # ...
]

def _is_etl_table(table_name: str) -> bool:
    return table_name.lower().startswith(ETL_TABLE_PREFIX)


class ETLManager:
    # def __init__(self, debug_mode: bool, session: Session):
    def __init__(self, debug_mode: bool, db: Database):
        self.debug_mode = debug_mode
        self.session = db.get_session()
        self.db = db
        self.logger = Logger()

    # ----------------------------------
    # CREATE AND DROP TABLE INDEXES
    # ----------------------------------
    def rebuild_indexes(
        self,
        index_group: Optional[Iterable[str]] = None,
        drop_only: bool = False,
        drop_first: bool = True,
        set_write_mode: bool = True,
        set_read_mode: bool = True,
    ) -> tuple[bool, str]:
        
        """
        Use the DTP Mixin to access methods to run these functions
        """

        tuning = DBTuningMixin()._bind_db_tuning(self.session, self.logger)

        INDEX_GROUP_CATALOG = {
            "entity": tuning.get_entity_index_specs,
            "entity_relationship": tuning.get_entity_relationship_index_specs,
            "entity_location": tuning.get_entity_location_index_specs,
            "gene": tuning.get_gene_index_specs,
            "variant": tuning.get_snp_index_specs,
            "protein": tuning.get_protein_index_specs,
            "go": tuning.get_go_index_specs,
            "pathway": tuning.get_pathway_index_specs,
            "gwas": tuning.get_variant_gwas_index_specs,
            "disease": tuning.get_disease_index_specs,
            "chemical": tuning.get_chemical_index_specs,
        }

        ALIASES = {
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

        # Resolve requested groups
        if not index_group:
            selected = INDEX_GROUP_CATALOG
        else:
            selected = {}
            invalid = []
            for g in index_group:
                key = ALIASES.get(str(g).strip().lower())
                if key and key in INDEX_GROUP_CATALOG:
                    selected[key] = INDEX_GROUP_CATALOG[key]
                else:
                    invalid.append(g)

            if invalid:
                self.logger.log(
                    f"⚠️ Unknown index groups ignored: {invalid}. "
                    f"Valid groups: {sorted(INDEX_GROUP_CATALOG.keys())}",
                    "WARNING",
                )

        if not selected:
            msg = "❌ No valid index groups selected. Nothing to do."
            self.logger.log(msg, "ERROR")
            return False, msg

        total_warnings = 0
        msg = "OK"

        if set_write_mode:
            tuning.db_write_mode()

        # Drop phase
        if drop_only or drop_first:
            self.logger.log("🧹 Dropping indexes...", "INFO")
            for group_name, spec_fn in selected.items():
                specs = spec_fn
                if not specs:
                    continue
                try:
                    tuning.drop_indexes(specs)
                except Exception as e:
                    total_warnings += 1
                    msg = f"⚠️ Failed to drop indexes for {group_name}: {e}"
                    self.logger.log(msg, "WARNING")

            if drop_only:
                if set_read_mode:
                    tuning.db_read_mode()
                final = f"✅ Dropped indexes with {total_warnings} warning(s)."
                level = "WARNING" if total_warnings else "INFO"
                self.logger.log(final, level)
                return (total_warnings == 0), final

        # Create phase
        self.logger.log("🏗️ Creating indexes...", "INFO")
        for group_name, spec_fn in selected.items():
            specs = spec_fn
            if not specs:
                continue
            try:
                self.logger.log(f"🏗️ Creating indexes for {group_name}...", "INFO")
                tuning.create_indexes(specs)
            except Exception as e:
                total_warnings += 1
                msg = f"⚠️ Failed to create indexes for {group_name}: {e}"
                self.logger.log(msg, "WARNING")

        if set_read_mode:
            tuning.db_read_mode()

        final = f"✅ Index rebuild finished with {total_warnings} warning(s)."
        level = "WARNING" if total_warnings else "INFO"
        self.logger.log(final, level)

        return True, final

    def _delete_matching_files(self, path_pattern: str):
        for file_path in glob.glob(path_pattern):
            try:
                if os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    self.logger.log(
                        f"🧹 Deleted directory: {file_path}", "DEBUG"
                    )  # noqa E501
                else:
                    os.remove(file_path)
                    self.logger.log(f"🗑️ Deleted file: {file_path}", "DEBUG")
            except Exception as e:
                self.logger.log(
                    f"⚠️ Could not delete {file_path}: {e}", "WARNING"
                )  # noqa E501

    def _create_package(self, data_source: ETLDataSource) -> Optional[ETLPackage]:
        try:
            package = ETLPackage(
                data_source_id=data_source.id,
                status="running",
                operation_type="running",
                version_tag=None,
                note=None,
                active=True,
            )
            self.session.add(package)
            self.session.commit()

            self.logger.log(
                f"📦 Created ETLPackage ID={package.id} for data source '{data_source.name}'",
                "DEBUG",
            )
            return package
        except Exception as e:
            self.logger.log(f"❌ Error creating ETLPackage: {e}", "ERROR")
            self.session.rollback()
            return None

    def start_process(
        self,
        source_system: list = None,
        data_sources: list = None,
        download_path: str = None,
        processed_path: str = None,
        run_steps: list = None,
        force_steps: list = None,
        use_conflict_csv: bool = False,
    ) -> None:
        """
        Runs ETL pipeline Extract → Transform → Load for specified data sources.

        Key rule (v3.2+): ONE ETLPackage PER PHASE.
        - Extract creates an "extract" package
        - Transform creates a "transform" package (depends on last extract)
        - Load creates a "load" package (depends on last transform)

        Notes:
        - Only active ETLDataSources are processed.
        - Each stage is skipped if the previous one fails/missing.
        - Status updates and errors are recorded in the database.
        """

        if run_steps is None:
            run_steps = ["extract", "transform", "load"]
        if force_steps is None:
            force_steps = []

        # Normalize inputs
        if isinstance(source_system, str):
            source_system = [source_system]
        if isinstance(data_sources, str):
            data_sources = [data_sources]

        # Validate input
        if not source_system and not data_sources:
            self.logger.log("❌ No source_system or data_sources provided. Aborting.", "ERROR")
            return

        # Build base query
        query = self.session.query(ETLDataSource).filter(ETLDataSource.active.is_(True))

        if source_system:
            query = (
                query.join(ETLSourceSystem)
                .filter(ETLSourceSystem.name.in_(source_system))
            )

        if data_sources:
            query = query.filter(ETLDataSource.name.in_(data_sources))

        datasources_to_process = query.all()

        if not datasources_to_process:
            self.logger.log("⚠️ No matching active DataSources found.", "WARNING")
            return

        for ds in datasources_to_process:
            self.logger.log(f"🔁 Starting ETL for '{ds.name}' (source_system_id={ds.source_system_id}, data_source_id={ds.id})", "INFO")

            try:
                # Load DTP module once per datasource
                script_module = importlib.import_module(f"biofilter.modules.etl.dtps.{ds.dtp_script.lower()}")

                # -----------------------------
                # 1) EXTRACT
                # -----------------------------
                if "extract" in run_steps:
                    extract_pkg = self._create_package(ds)
                    extract_pkg.operation_type = "extract"
                    extract_pkg.status = "running"
                    extract_pkg.extract_status = "running"
                    extract_pkg.extract_start = datetime.now()
                    self.session.commit()

                    self.logger.log(f"📦 [Extract] Running for '{ds.name}' (package_id={extract_pkg.id})", "INFO")

                    dtp_extract = script_module.DTP(
                        logger=self.logger,
                        debug_mode=self.debug_mode,
                        datasource=ds,
                        package=extract_pkg,
                        session=self.session,
                        db=self.db,
                        use_conflict_csv=use_conflict_csv,
                    )

                    ok, message, file_hash = dtp_extract.extract(raw_dir=download_path)

                    extract_pkg.extract_end = datetime.now()
                    extract_pkg.extract_hash = file_hash

                    if ok:
                        # Check if same hash already completed previously
                        last_same_hash = (
                            self.session.query(ETLPackage)
                            .filter(
                                ETLPackage.data_source_id == ds.id,
                                ETLPackage.operation_type == "extract",
                                ETLPackage.status.in_(["completed", "up-to-date"]),
                                ETLPackage.extract_hash == file_hash,
                            )
                            .order_by(ETLPackage.extract_end.desc())
                            .first()
                        )

                        if last_same_hash and "extract" not in force_steps:
                            extract_pkg.status = "up-to-date"
                            extract_pkg.extract_status = "up-to-date"
                            extract_pkg.stats = {
                                "note": "extract up-to-date (hash already processed)",
                                "previous_package_id": last_same_hash.id,
                                "hash": file_hash,
                            }
                            self.logger.log(
                                f"✅ [Extract] Up-to-date for '{ds.name}' (hash={file_hash})",
                                "INFO",
                            )
                        else:
                            extract_pkg.status = "completed"
                            extract_pkg.extract_status = "completed"
                            extract_pkg.stats = {"hash": file_hash}
                            self.logger.log(
                                f"✅ [Extract] Completed for '{ds.name}' (hash={file_hash})",
                                "INFO",
                            )

                    else:
                        extract_pkg.status = "failed"
                        extract_pkg.extract_status = "failed"
                        extract_pkg.stats = {"error": message, "step": "extract"}
                        self.session.commit()

                        self.logger.log(message, "ERROR")
                        self.logger.log(f"⛔️ ETL halted for '{ds.name}' due to extract failure", "WARNING")
                        continue

                    self.session.commit()

                # -----------------------------
                # 2) TRANSFORM
                # -----------------------------
                if "transform" in run_steps:
                    # Find latest successful extract package for downstream steps
                    last_extract = (
                        self.session.query(ETLPackage)
                        .filter(
                            ETLPackage.data_source_id == ds.id,
                            ETLPackage.operation_type == "extract",
                            ETLPackage.status.in_(["completed", "up-to-date"]),
                        )
                        .order_by(ETLPackage.extract_end.desc())
                        .first()
                    )

                    if not last_extract:
                        msg = f"⚠️ No successful extract found for '{ds.name}' — cannot run transform."
                        self.logger.log(msg, "WARNING")

                        transform_pkg = self._create_package(ds)
                        transform_pkg.operation_type = "transform"
                        transform_pkg.status = "failed"
                        transform_pkg.extract_status = "not-applicable"
                        transform_pkg.transform_status = "failed"
                        transform_pkg.load_status = "not-applicable"
                        transform_pkg.transform_start = datetime.now()
                        transform_pkg.transform_end = datetime.now()
                        transform_pkg.stats = {"error": msg, "step": "transform"}
                        self.session.commit()
                        continue

                    # Skip if already transformed for this extract hash (unless forced)
                    last_transform = (
                        self.session.query(ETLPackage)
                        .filter(
                            ETLPackage.data_source_id == ds.id,
                            ETLPackage.operation_type == "transform",
                            ETLPackage.status.in_(["completed", "up-to-date"]),
                            ETLPackage.transform_hash == last_extract.extract_hash,
                        )
                        .order_by(ETLPackage.transform_end.desc())
                        .first()
                    )

                    if last_transform and "transform" not in force_steps:
                        transform_pkg = self._create_package(ds)
                        transform_pkg.operation_type = "transform"
                        transform_pkg.status = "not-applicable"
                        transform_pkg.transform_status = "not-applicable"
                        transform_pkg.extract_status = "not-applicable"
                        transform_pkg.load_status = "not-applicable"
                        transform_pkg.transform_start = datetime.now()
                        transform_pkg.transform_end = datetime.now()
                        transform_pkg.transform_hash = last_extract.extract_hash
                        transform_pkg.stats = {
                            "note": "transform skipped (already completed for this hash)",
                            "source_extract_package_id": last_extract.id,
                            "previous_transform_package_id": last_transform.id,
                            "hash": last_extract.extract_hash,
                        }
                        self.session.commit()
                        self.logger.log(f"⚙️ [Transform] Up-to-date for '{ds.name}' (package_id={transform_pkg.id})", "WARNING")
                    else:
                        transform_pkg = self._create_package(ds)
                        transform_pkg.operation_type = "transform"
                        transform_pkg.status = "running"
                        transform_pkg.transform_status = "running"
                        transform_pkg.transform_start = datetime.now()
                        transform_pkg.transform_hash = last_extract.extract_hash
                        transform_pkg.extract_status = "not-applicable"
                        self.session.commit()

                        self.logger.log(f"⚙️ [Transform] Running for '{ds.name}' (package_id={transform_pkg.id})", "INFO")

                        dtp_transform = script_module.DTP(
                            logger=self.logger,
                            debug_mode=self.debug_mode,
                            datasource=ds,
                            package=transform_pkg,
                            session=self.session,
                            db=self.db,
                            use_conflict_csv=use_conflict_csv,
                        )

                        ok, message = dtp_transform.transform(download_path, processed_path)

                        transform_pkg.transform_end = datetime.now()

                        if ok:
                            transform_pkg.status = "completed"
                            transform_pkg.transform_status = "completed"
                            self.logger.log(f"✅ [Transform] Completed for '{ds.name}'", "INFO")
                        else:
                            transform_pkg.status = "failed"
                            transform_pkg.transform_status = "failed"
                            transform_pkg.stats = {"error": message, "step": "transform"}
                            self.logger.log(message, "ERROR")
                            self.logger.log(f"❌ [Transform] Failed for '{ds.name}'", "WARNING")
                            self.session.commit()
                            continue

                        self.session.commit()

                # -----------------------------
                # 3) LOAD
                # -----------------------------
                if "load" in run_steps:

                    # Find latest successful transform for downstream steps
                    last_transform_ok = (
                        self.session.query(ETLPackage)
                        .filter(
                            ETLPackage.data_source_id == ds.id,
                            ETLPackage.operation_type == "transform",
                            ETLPackage.status.in_(["completed", "up-to-date"]),
                        )
                        .order_by(ETLPackage.transform_end.desc())
                        .first()
                    )

                    if not last_transform_ok:
                        msg = f"⚠️ No successful transform found for '{ds.name}' — cannot run load."
                        self.logger.log(msg, "WARNING")

                        load_pkg = self._create_package(ds)
                        load_pkg.operation_type = "load"
                        load_pkg.status = "failed"
                        load_pkg.extract_status = "not-applicable"
                        load_pkg.transform_status = "not-applicable"
                        load_pkg.load_status = "failed"
                        load_pkg.load_start = datetime.now()
                        load_pkg.load_end = datetime.now()
                        load_pkg.stats = {"error": msg, "step": "load"}
                        self.session.commit()
                        continue

                    # Skip if already loaded for this transform hash (unless forced)
                    last_load = (
                        self.session.query(ETLPackage)
                        .filter(
                            ETLPackage.data_source_id == ds.id,
                            ETLPackage.operation_type == "load",
                            ETLPackage.status.in_(["completed", "up-to-date"]),
                            ETLPackage.load_hash == last_transform_ok.transform_hash,
                        )
                        .order_by(ETLPackage.load_end.desc())
                        .first()
                    )

                    if last_load and "load" not in force_steps:
                        load_pkg = self._create_package(ds)
                        load_pkg.operation_type = "load"
                        load_pkg.status = "not-applicable"
                        load_pkg.load_status = "not-applicable"
                        load_pkg.extract_status = "not-applicable"
                        load_pkg.transform_status = "not-applicable"
                        load_pkg.load_start = datetime.now()
                        load_pkg.load_end = datetime.now()
                        load_pkg.load_hash = last_transform_ok.transform_hash
                        load_pkg.stats = {
                            "note": "load skipped (already completed for this hash)",
                            "source_transform_package_id": last_transform_ok.id,
                            "previous_load_package_id": last_load.id,
                            "hash": last_transform_ok.transform_hash,
                        }
                        self.session.commit()
                        self.logger.log(f"🚚 [Load] Up-to-date for '{ds.name}' (package_id={load_pkg.id})", "WARNING")

                    else:
                        load_pkg = self._create_package(ds)
                        load_pkg.operation_type = "load"
                        load_pkg.status = "running"
                        load_pkg.load_status = "running"
                        load_pkg.load_start = datetime.now()
                        load_pkg.load_hash = last_transform_ok.transform_hash
                        load_pkg.extract_status = "not-applicable"
                        load_pkg.transform_status = "not-applicable"
                        self.session.commit()

                        self.logger.log(f"🚚 [Load] Running for '{ds.name}' (package_id={load_pkg.id})", "INFO")

                        dtp_load = script_module.DTP(
                            logger=self.logger,
                            debug_mode=self.debug_mode,
                            datasource=ds,
                            package=load_pkg,
                            session=self.session,
                            db=self.db,
                            use_conflict_csv=use_conflict_csv,
                        )

                        ok, message = dtp_load.load(processed_path)

                        load_pkg.load_end = datetime.now()

                        if ok:
                            load_pkg.status = "completed"
                            load_pkg.load_status = "completed"
                            self.logger.log(f"✅ [Load] Completed for '{ds.name}'", "INFO")
                        else:
                            load_pkg.status = "failed"
                            load_pkg.load_status = "failed"
                            load_pkg.stats = {"error": message, "step": "load"}
                            self.logger.log(message, "ERROR")
                            self.logger.log(f"❌ [Load] Failed for '{ds.name}'", "WARNING")
                            self.session.commit()
                            continue

                        self.session.commit()

                self.logger.log(f"🎉 ETL pipeline finished for '{ds.name}'", "INFO")

            except (SQLAlchemyError, Exception) as e:
                self.logger.log(f"❌ ETL failed for '{ds.name}': {e}", "ERROR")
                try:
                    self.session.rollback()
                except Exception:
                    pass

    def _simple_purge_by_data_source(self, ds_id: int) -> None:
        """
        Very simple rollback: delete rows where data_source_id = :ds_id
        across all non-ETL tables, in FK-safe order.

        - Auto-discovers tables with a `data_source_id` column
        - Excludes ETL tables (etl_*)
        - Uses dependency-aware order (children first). Falls back to reverse
        sorted_tables.
        """
        engine = self.session.get_bind()
        # insp = inspect(engine)
        metadata = MetaData()
        metadata.reflect(bind=engine)

        # 1) Pick candidate tables (have `data_source_id` and are NOT ETL)
        candidates = []
        for tname, table in metadata.tables.items():
            if _is_etl_table(tname):
                continue
            if "data_source_id" in table.columns:
                candidates.append(table)

        if not candidates:
            self.logger.log(
                "ℹ️ No non-ETL tables with `data_source_id` found.", "INFO"
            )  # noqa E501
            return

        # 2) Order tables: override > FK topo (children->parents) > reverse of metadata.sorted_tables # noqa E501
        ordered = self._order_for_delete(candidates, metadata)

        # 3) Delete per table
        total = 0
        for table in ordered:
            # Count (for log)
            cnt = (
                self.session.execute(
                    select(func.count())
                    .select_from(table)
                    .where(table.c.data_source_id == ds_id)
                ).scalar()
                or 0
            )
            if cnt == 0:
                continue

            self.logger.log(
                f"🗑️  Deleting {cnt} rows from {table.name} (data_source_id={ds_id})",  # noqa E501
                "INFO",
            )
            self.session.execute(
                table.delete().where(table.c.data_source_id == ds_id)
            )  # noqa E501
            total += cnt

        self.session.commit()
        self.logger.log(
            f"✅ Simple purge complete for data_source_id={ds_id}. Total rows: {total}",  # noqa E501
            "INFO",
        )

    def _order_for_delete(self, candidates, metadata):
        """
        Compute delete order: children -> parents.
        Priority:
        1) PURGE_ORDER_OVERRIDE if it fully matches subset of candidates
        2) Graph-based order using FKs among candidates
        3) Fallback: reverse of metadata.sorted_tables
        """
        cand_by_name = {t.name: t for t in candidates}

        # 1) Manual override (only include tables actually present)
        if PURGE_ORDER_OVERRIDE:
            override = [
                cand_by_name[n]
                for n in PURGE_ORDER_OVERRIDE
                if n in cand_by_name  # noqa E501
            ]
            # Add any remaining not covered by override at the end (still children->parents via FK) # noqa E501
            rest = [
                t for t in candidates if t.name not in PURGE_ORDER_OVERRIDE
            ]  # noqa E501
        else:
            override, rest = [], list(candidates)

        # 2) Graph-based order for 'rest'
        if rest:
            graph = {t.name: set() for t in rest}  # parent -> set(children)
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

            # Any remaining -> append (cycle or missing FK metadata)
            remaining = [n for n, cs in graph.items() if cs]
            ordered_rest = [cand_by_name[n] for n in ordered_names] + [
                cand_by_name[n] for n in remaining
            ]
        else:
            ordered_rest = []

        # 3) Fallback if nothing computed
        ordered = override + ordered_rest
        if not ordered:
            # reverse creation order ≈ children->parents; acceptable fallback
            sorted_all = list(metadata.sorted_tables)
            fallback = [t for t in reversed(sorted_all) if t in candidates]
            ordered = fallback

        return ordered
