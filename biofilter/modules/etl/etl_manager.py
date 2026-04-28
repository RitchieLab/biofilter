from __future__ import annotations

import glob
import importlib
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Optional, Sequence

from sqlalchemy import MetaData, func, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

from biofilter.modules.db.database import Database
from biofilter.modules.db.models import (
    Entity,
    EntityRelationship,
    ETLDataSource,
    ETLPackage,
    ETLSourceSystem,
)
from biofilter.modules.etl.mixins.base_dtp_turning import DBTuningMixin
from biofilter.utils.logger import Logger

ETL_TABLE_PREFIX = "etl_"
PURGE_ORDER_OVERRIDE = [
    "variant_masters",
    "variant_molecular_effects",
    "entity_relationships",
    "entity_aliases",
    "entities",
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
    - DTPs receive BOTH: `session` (for ORM) and `db`
        (for engine/dialect/mappings).
    """

    def __init__(self, debug_mode: bool, db: Database, logger: Optional[Logger] = None):  # noqa E501
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
        Rebuild (drop/create) indexes for selected groups using DBTuningMixin
        specs. Uses a short-lived session since this is admin-only.
        """
        with self.db.get_session() as session:
            tuning = DBTuningMixin()._bind_db_tuning(session, self.logger)

            index_catalog = {
                "entity": tuning.get_entity_index_specs,
                "entity_relationship": tuning.get_entity_relationship_index_specs,  # noqa E501
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

            selected = self._select_index_groups(index_group, index_catalog, aliases)  # noqa E501
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
                        msg = f"⚠️ Failed to drop indexes in {group_name}: {e}"
                        self.logger.log(msg, "WARNING")

                if drop_only:
                    if set_read_mode:
                        tuning.db_read_mode()
                    final = f"✅ Dropped indexes with {total_warnings} warning(s)."  # noqa E501
                    self.logger.log(final, "WARNING" if total_warnings else "INFO")  # noqa E501
                    return (total_warnings == 0), final

            # Create
            self.logger.log("🏗️ Creating indexes...", "INFO")
            for group_name, spec_fn in selected.items():
                try:
                    specs = spec_fn
                    # specs = spec_fn()  # ✅ call it
                    if not specs:
                        continue
                    msg = f"✅ Found {len(specs)} index specs for {group_name}."
                    self.logger.log(msg, "INFO")
                    tuning.create_indexes(specs)
                except Exception as e:
                    total_warnings += 1
                    msg = f"⚠️ Failed to create indexes for {group_name}: {e}"
                    self.logger.log(msg, "WARNING")

            if set_read_mode:
                tuning.db_read_mode()

            final = f"✅ Index rebuild finished with {total_warnings} warning(s)."  # noqa E501
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
                f"⚠️ Unknown index groups ignored: {invalid}. Valid groups: {sorted(catalog.keys())}",  # noqa E501
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
            msg = "❌ No source_system or data_sources provided. Aborting."
            self.logger.log(msg, "ERROR")
            return

        # Query DataSources in a short-lived session
        with self.db.get_session() as session:
            ds_ids = self._resolve_datasource_ids(session, source_system, data_sources)  # noqa E501

        if not ds_ids:
            msg = "⚠️ No matching active DataSources found."
            self.logger.log(msg, "WARNING")
            return

        # Run each datasource with its OWN session
        # (keeps package updates consistent per ds)
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
                )

    def start_process_all(
        self,
        source_system: Optional[Sequence[str]] = None,
        data_sources: Optional[Sequence[str]] = None,
        download_path: Optional[str] = None,
        processed_path: Optional[str] = None,
        drop_files_on_success: bool = False,
        only_active: bool = True,
        stop_on_error: bool = False,
    ) -> dict[str, int]:
        """
        Resume-friendly ETL for many data sources:
        - resolves targets in deterministic order (data_source_id asc)
        - skips data sources whose latest LOAD is already successful
        - runs extract/transform/load for pending ones
        - optionally drops raw/processed files after successful load
        """
        if isinstance(source_system, str):
            source_system = [source_system]
        if isinstance(data_sources, str):
            data_sources = [data_sources]

        with self.db.get_session() as session:
            ds_ids = self._resolve_datasource_ids(
                session,
                source_system,
                data_sources,
                only_active=only_active,
            )

        if not ds_ids:
            msg = "⚠️ No matching DataSources found for update-all."
            self.logger.log(msg, "WARNING")
            return {
                "selected": 0,
                "skipped": 0,
                "processed": 0,
                "succeeded": 0,
                "failed": 0,
            }

        summary = {
            "selected": len(ds_ids),
            "skipped": 0,
            "processed": 0,
            "succeeded": 0,
            "failed": 0,
        }
        success_statuses = {"completed", "up-to-date", "not-applicable"}

        for ds_id in ds_ids:
            with self.db.get_session() as session:
                ds = self._load_datasource(session, ds_id)

                latest_before = self._latest_load_status(session, ds.id)
                if latest_before in success_statuses:
                    summary["skipped"] += 1
                    msg = f"⏭️ Skipping '{ds.name}' (latest load already {latest_before})."  # noqa E501
                    self.logger.log(msg, "INFO")
                    continue

                summary["processed"] += 1
                self._run_one_datasource(
                    session=session,
                    ds=ds,
                    download_path=download_path,
                    processed_path=processed_path,
                    run_steps=["extract", "transform", "load"],
                    force_steps=[],
                )

                latest_after = self._latest_load_status(session, ds.id)
                if latest_after in success_statuses:
                    summary["succeeded"] += 1
                    self.logger.log(
                        f"✅ update-all succeeded for '{ds.name}' (load={latest_after}).",  # noqa E501
                        "INFO",
                    )

                    if drop_files_on_success:
                        if download_path:
                            raw_base = os.path.join(
                                str(download_path), ds.source_system.name, ds.name  # noqa E501
                            )
                            self._delete_matching_files(f"{raw_base}*")
                        if processed_path:
                            proc_base = os.path.join(
                                str(processed_path), ds.source_system.name, ds.name  # noqa E501
                            )
                            self._delete_matching_files(f"{proc_base}*")
                else:
                    summary["failed"] += 1
                    self.logger.log(
                        (
                            f"❌ update-all failed for '{ds.name}' "
                            f"(latest load={latest_after or 'none'})."
                        ),
                        "ERROR",
                    )
                    if stop_on_error:
                        break

        self.logger.log(
            (
                "📊 update-all summary "
                f"(selected={summary['selected']}, skipped={summary['skipped']}, "  # noqa E501
                f"processed={summary['processed']}, succeeded={summary['succeeded']}, "  # noqa E501
                f"failed={summary['failed']})"
            ),
            "INFO",
        )
        return summary

    def restart_etl_process(
        self,
        data_source: Optional[Sequence[str]] = None,
        source_system: Optional[Sequence[str]] = None,
        download_path: Optional[str] = None,
        processed_path: Optional[str] = None,
        delete_files: bool = False,
    ) -> bool:
        """
        Restart ETL for selected data sources by:
        1) rolling back non-ETL rows linked by data_source_id
        2) optionally deleting raw/processed files
        3) re-running extract/transform/load with forced steps
        """
        if isinstance(source_system, str):
            source_system = [source_system]
        if isinstance(data_source, str):
            data_source = [data_source]

        if not source_system and not data_source:
            self.logger.log(
                "❌ No source_system or data_source provided. Aborting restart.",  # noqa E501
                "ERROR",
            )
            return False

        with self.db.get_session() as session:
            ds_ids = self._resolve_datasource_ids(session, source_system, data_source)  # noqa E501

        if not ds_ids:
            self.logger.log("⚠️ No matching active DataSources found.", "WARNING")  # noqa E501
            return False

        all_ok = True
        for ds_id in ds_ids:
            with self.db.get_session() as session:
                ds = self._load_datasource(session, ds_id)
                self.logger.log(f"♻️  Restarting ETL for '{ds.name}'", "INFO")

                rollback_ok, _ = self._rollback_data_source(
                    session=session,
                    ds=ds,
                    note="rollback before restart",
                )
                if not rollback_ok:
                    all_ok = False
                    continue

                if delete_files:
                    if download_path:
                        raw_base = os.path.join(
                            str(download_path), ds.source_system.name, ds.name
                        )
                        self._delete_matching_files(f"{raw_base}*")

                    if processed_path:
                        proc_base = os.path.join(
                            str(processed_path), ds.source_system.name, ds.name
                        )
                        self._delete_matching_files(f"{proc_base}*")

                self._run_one_datasource(
                    session=session,
                    ds=ds,
                    download_path=download_path,
                    processed_path=processed_path,
                    run_steps=["extract", "transform", "load"],
                    force_steps=["extract", "transform", "load"],
                )

        return all_ok

    def rollback_etl_process(
        self,
        package_ids: Optional[Sequence[int]] = None,
        data_source: Optional[Sequence[str]] = None,
        source_system: Optional[Sequence[str]] = None,
        delete_files: bool = False,
        download_path: Optional[str] = None,
        processed_path: Optional[str] = None,
    ) -> bool:
        """
        Rollback ETL loads without rerunning ETL.

        Supported modes:
        - by package_ids (targeted rollback)
        - by data_source/source_system filters (full data-source rollback)
        """
        normalized_pkg_ids = self._normalize_package_ids(package_ids)

        if normalized_pkg_ids and (data_source or source_system):
            self.logger.log(
                "❌ Use either package_ids OR data_source/source_system filters, not both.",  # noqa E501
                "ERROR",
            )
            return False

        if not normalized_pkg_ids and not data_source and not source_system:
            self.logger.log(
                "❌ No rollback target provided. Use package_ids or data_source/source_system.",  # noqa E501
                "ERROR",
            )
            return False

        all_ok = True

        if normalized_pkg_ids:
            if delete_files:
                self.logger.log(
                    "⚠️ delete_files ignored for package rollback (ambiguous scope).",  # noqa E501
                    "WARNING",
                )

            for pkg_id in normalized_pkg_ids:
                with self.db.get_session() as session:
                    pkg = self._load_package(session, pkg_id)
                    if not pkg:
                        all_ok = False
                        self.logger.log(
                            f"❌ Package id={pkg_id} not found. Skipping.", "ERROR"  # noqa E501
                        )
                        continue
                    if str(pkg.operation_type or "").lower() == "rollback":
                        all_ok = False
                        self.logger.log(
                            f"❌ Package id={pkg_id} is already a rollback package. Skipping.",  # noqa E501
                            "ERROR",
                        )
                        continue

                    ds = self._load_datasource(session, int(pkg.data_source_id))  # noqa E501
                    ok, _ = self._rollback_package(
                        session=session,
                        ds=ds,
                        target_package=pkg,
                        note="manual package rollback",
                    )
                    if not ok:
                        all_ok = False

            return all_ok

        if isinstance(source_system, str):
            source_system = [source_system]
        if isinstance(data_source, str):
            data_source = [data_source]

        with self.db.get_session() as session:
            ds_ids = self._resolve_datasource_ids(session, source_system, data_source)  # noqa E501

        if not ds_ids:
            self.logger.log("⚠️ No matching active DataSources found.", "WARNING")  # noqa E501
            return False

        for ds_id in ds_ids:
            with self.db.get_session() as session:
                ds = self._load_datasource(session, ds_id)
                ok, _ = self._rollback_data_source(
                    session=session,
                    ds=ds,
                    note="manual data-source rollback",
                )
                if not ok:
                    all_ok = False
                    continue

                if delete_files:
                    if download_path:
                        raw_base = os.path.join(
                            str(download_path), ds.source_system.name, ds.name
                        )
                        self._delete_matching_files(f"{raw_base}*")

                    if processed_path:
                        proc_base = os.path.join(
                            str(processed_path), ds.source_system.name, ds.name
                        )
                        self._delete_matching_files(f"{proc_base}*")

        return all_ok

    def _resolve_datasource_ids(
        self,
        session: Session,
        source_system: Optional[Sequence[str]],
        data_sources: Optional[Sequence[str]],
        only_active: bool = True,
    ) -> list[int]:
        q = session.query(ETLDataSource.id)

        if only_active:
            q = q.filter(ETLDataSource.active.is_(True))

        if source_system or only_active:
            q = q.join(ETLSourceSystem)

        if only_active:
            q = q.filter(ETLSourceSystem.active.is_(True))

        if source_system:
            q = q.filter(ETLSourceSystem.name.in_(list(source_system)))

        if data_sources:
            q = q.filter(ETLDataSource.name.in_(list(data_sources)))

        return [row[0] for row in q.order_by(ETLDataSource.id.asc()).all()]

    def _latest_load_status(self, session: Session, ds_id: int) -> Optional[str]:  # noqa E501
        row = (
            session.query(ETLPackage.load_status)
            .filter(
                ETLPackage.data_source_id == int(ds_id),
                ETLPackage.operation_type == "load",
            )
            .order_by(ETLPackage.created_at.desc(), ETLPackage.id.desc())
            .first()
        )
        if not row:
            return None
        status = row[0]
        if status is None:
            return None
        return str(status).strip().lower()

    @staticmethod
    def _normalize_package_ids(
        package_ids: Optional[Sequence[int]],
    ) -> list[int]:
        if package_ids is None:
            return []
        if isinstance(package_ids, int):
            package_ids = [package_ids]
        out = []
        for value in package_ids:
            try:
                out.append(int(value))
            except Exception:
                continue
        return sorted(set(out))

    def _load_datasource(self, session: Session, ds_id: int) -> ETLDataSource:
        ds = (
            session.query(ETLDataSource)
            .options(selectinload(ETLDataSource.source_system))
            .filter(ETLDataSource.id == ds_id)
            .one()
        )
        return ds

    def _load_package(self, session: Session, package_id: int) -> Optional[ETLPackage]:  # noqa E501
        return (
            session.query(ETLPackage)
            .filter(ETLPackage.id == int(package_id))
            .one_or_none()
        )

    def _create_rollback_package(
        self,
        session: Session,
        ds: ETLDataSource,
        note: str,
        target: dict[str, Any],
    ) -> Optional[ETLPackage]:
        try:
            pkg = ETLPackage(
                data_source_id=ds.id,
                status="running",
                operation_type="rollback",
                note=note,
                active=True,
                extract_status="not-applicable",
                transform_status="not-applicable",
                load_status="running",
                load_start=datetime.now(),
                stats=target,
            )
            session.add(pkg)
            session.commit()
            self.logger.log(
                (
                    "📦 Created rollback ETLPackage "
                    f"ID={pkg.id} for data source '{ds.name}'"
                ),
                "INFO",
            )
            return pkg
        except Exception as e:
            self.logger.log(f"❌ Error creating rollback ETLPackage: {e}", "ERROR")  # noqa E501
            try:
                session.rollback()
            except Exception:
                pass
            return None

    def _mark_rollback_failed(
        self,
        session: Session,
        rollback_pkg_id: int,
        message: str,
        extra_stats: Optional[dict[str, Any]] = None,
    ) -> None:
        try:
            pkg = self._load_package(session, rollback_pkg_id)
            if not pkg:
                return
            pkg.status = "failed"
            pkg.load_status = "failed"
            pkg.load_end = datetime.now()
            stats = dict(pkg.stats or {})
            stats.update({"error": message, "step": "rollback"})
            if extra_stats:
                stats.update(extra_stats)
            pkg.stats = stats
            session.commit()
        except Exception:
            try:
                session.rollback()
            except Exception:
                pass

    def _find_relationship_conflicts(
        self,
        session: Session,
        *,
        target_data_source_id: Optional[int] = None,
        target_package_id: Optional[int] = None,
        sample_limit: int = 20,
    ) -> dict[str, Any]:
        if target_data_source_id is None and target_package_id is None:
            return {"entities_to_rollback": 0, "conflict_count": 0, "samples": []}  # noqa E501

        if target_package_id is not None:
            entity_filter = Entity.etl_package_id == int(target_package_id)
        else:
            entity_filter = Entity.data_source_id == int(target_data_source_id)

        entity_ids_subq = session.query(Entity.id).filter(entity_filter).subquery()  # noqa E501
        entity_count = int(
            session.execute(select(func.count()).select_from(entity_ids_subq)).scalar()  # noqa E501
            or 0
        )
        if entity_count == 0:
            return {"entities_to_rollback": 0, "conflict_count": 0, "samples": []}  # noqa E501

        relationship_uses_target_entities = or_(
            EntityRelationship.entity_1_id.in_(select(entity_ids_subq.c.id)),
            EntityRelationship.entity_2_id.in_(select(entity_ids_subq.c.id)),
        )

        if target_package_id is not None:
            mismatch_filter = or_(
                EntityRelationship.etl_package_id.is_(None),
                EntityRelationship.etl_package_id != int(target_package_id),
                EntityRelationship.data_source_id.is_(None),
                EntityRelationship.data_source_id != int(target_data_source_id),  # noqa E501
            )
        else:
            mismatch_filter = or_(
                EntityRelationship.data_source_id.is_(None),
                EntityRelationship.data_source_id != int(target_data_source_id),  # noqa E501
            )

        count_stmt = (
            select(func.count(EntityRelationship.id))
            .select_from(EntityRelationship)
            .where(relationship_uses_target_entities)
            .where(mismatch_filter)
        )
        conflict_count = int(session.execute(count_stmt).scalar() or 0)
        if conflict_count == 0:
            return {
                "entities_to_rollback": entity_count,
                "conflict_count": 0,
                "samples": [],
            }

        sample_rows = (
            session.query(
                EntityRelationship.id.label("relationship_id"),
                EntityRelationship.entity_1_id.label("entity_1_id"),
                EntityRelationship.entity_2_id.label("entity_2_id"),
                EntityRelationship.data_source_id.label("relationship_data_source_id"),  # noqa E501
                EntityRelationship.etl_package_id.label("relationship_package_id"),  # noqa E501
            )
            .filter(relationship_uses_target_entities)
            .filter(mismatch_filter)
            .order_by(EntityRelationship.id.asc())
            .limit(sample_limit)
            .all()
        )

        samples = [
            {
                "relationship_id": int(r.relationship_id),
                "entity_1_id": int(r.entity_1_id),
                "entity_2_id": int(r.entity_2_id),
                "relationship_data_source_id": r.relationship_data_source_id,
                "relationship_package_id": r.relationship_package_id,
            }
            for r in sample_rows
        ]
        return {
            "entities_to_rollback": entity_count,
            "conflict_count": conflict_count,
            "samples": samples,
        }

    def _rollback_data_source(
        self,
        session: Session,
        ds: ETLDataSource,
        note: str,
    ) -> tuple[bool, str]:
        rollback_pkg = self._create_rollback_package(
            session=session,
            ds=ds,
            note=note,
            target={"mode": "data_source", "target_data_source_id": ds.id},
        )
        if not rollback_pkg:
            return False, "Could not create rollback package."

        conflicts = self._find_relationship_conflicts(
            session,
            target_data_source_id=ds.id,
        )
        if conflicts["conflict_count"] > 0:
            msg = (
                "❌ Rollback blocked for data source "
                f"'{ds.name}': found {conflicts['conflict_count']} "
                "entity_relationship rows from different package/source that use "  # noqa E501
                "entities targeted for rollback. Rollback newer dependent loads first."  # noqa E501
            )
            self.logger.log(msg, "ERROR")
            self._mark_rollback_failed(
                session=session,
                rollback_pkg_id=rollback_pkg.id,
                message=msg,
                extra_stats={
                    "dependency_conflict": True,
                    "entities_to_rollback": conflicts["entities_to_rollback"],
                    "conflict_count": conflicts["conflict_count"],
                    "conflict_samples": conflicts["samples"],
                },
            )
            return False, msg

        try:
            deleted_rows_by_table = self._simple_purge_by_data_source(
                session,
                ds_id=ds.id,
                commit=False,
            )
            deleted_total = int(sum(deleted_rows_by_table.values()))

            rollback_pkg = self._load_package(session, rollback_pkg.id)
            if rollback_pkg:
                rollback_pkg.status = "completed"
                rollback_pkg.load_status = "completed"
                rollback_pkg.load_end = datetime.now()
                rollback_pkg.stats = {
                    "mode": "data_source",
                    "target_data_source_id": ds.id,
                    "deleted_rows_total": deleted_total,
                    "deleted_rows_by_table": deleted_rows_by_table,
                }
            session.commit()
            msg = (
                f"✅ Rollback completed for data_source '{ds.name}' "
                f"(deleted_rows={deleted_total}, rollback_package_id={rollback_pkg.id})"  # noqa E501
            )
            self.logger.log(msg, "INFO")
            return True, msg
        except Exception as e:
            try:
                session.rollback()
            except Exception:
                pass
            msg = f"❌ Rollback failed for data_source '{ds.name}': {e}"
            self.logger.log(msg, "ERROR")
            self._mark_rollback_failed(
                session=session,
                rollback_pkg_id=rollback_pkg.id,
                message=msg,
            )
            return False, msg

    def _rollback_package(
        self,
        session: Session,
        ds: ETLDataSource,
        target_package: ETLPackage,
        note: str,
    ) -> tuple[bool, str]:
        rollback_pkg = self._create_rollback_package(
            session=session,
            ds=ds,
            note=note,
            target={
                "mode": "package",
                "target_package_id": target_package.id,
                "target_data_source_id": ds.id,
            },
        )
        if not rollback_pkg:
            return False, "Could not create rollback package."

        conflicts = self._find_relationship_conflicts(
            session,
            target_data_source_id=ds.id,
            target_package_id=target_package.id,
        )
        if conflicts["conflict_count"] > 0:
            msg = (
                "❌ Rollback blocked for package "
                f"id={target_package.id} ({ds.name}): found {conflicts['conflict_count']} "  # noqa E501
                "entity_relationship rows from different package/source that use "  # noqa E501
                "entities targeted for rollback. Rollback newer dependent loads first."  # noqa E501
            )
            self.logger.log(msg, "ERROR")
            self._mark_rollback_failed(
                session=session,
                rollback_pkg_id=rollback_pkg.id,
                message=msg,
                extra_stats={
                    "dependency_conflict": True,
                    "entities_to_rollback": conflicts["entities_to_rollback"],
                    "conflict_count": conflicts["conflict_count"],
                    "conflict_samples": conflicts["samples"],
                    "target_package_id": target_package.id,
                },
            )
            return False, msg

        try:
            deleted_rows_by_table = self._simple_purge_by_package(
                session=session,
                package_id=target_package.id,
                commit=False,
            )
            deleted_total = int(sum(deleted_rows_by_table.values()))

            rollback_pkg = self._load_package(session, rollback_pkg.id)
            if rollback_pkg:
                rollback_pkg.status = "completed"
                rollback_pkg.load_status = "completed"
                rollback_pkg.load_end = datetime.now()
                rollback_pkg.stats = {
                    "mode": "package",
                    "target_package_id": target_package.id,
                    "target_data_source_id": ds.id,
                    "deleted_rows_total": deleted_total,
                    "deleted_rows_by_table": deleted_rows_by_table,
                }
            session.commit()
            msg = (
                f"✅ Rollback completed for package id={target_package.id} "
                f"(data_source='{ds.name}', deleted_rows={deleted_total}, "
                f"rollback_package_id={rollback_pkg.id})"
            )
            self.logger.log(msg, "INFO")
            return True, msg
        except Exception as e:
            try:
                session.rollback()
            except Exception:
                pass
            msg = f"❌ Rollback failed for package id={target_package.id}: {e}"
            self.logger.log(msg, "ERROR")
            self._mark_rollback_failed(
                session=session,
                rollback_pkg_id=rollback_pkg.id,
                message=msg,
                extra_stats={"target_package_id": target_package.id},
            )
            return False, msg

    def _run_one_datasource(
        self,
        session: Session,
        ds: ETLDataSource,
        download_path: Optional[str],
        processed_path: Optional[str],
        run_steps: Sequence[str],
        force_steps: Sequence[str],
    ) -> None:
        self.logger.log(
            f"🔁 Starting ETL for '{ds.name}' (source_system_id={ds.source_system_id}, data_source_id={ds.id})",  # noqa E501
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
                )

            # ---- Load
            if "load" in run_steps:
                self._run_load(
                    session=session,
                    module=module,
                    ds=ds,
                    processed_path=processed_path,
                    force_steps=force_steps,
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
                f"📦 Created ETLPackage ID={pkg.id} for data source '{data_source.name}'",  # noqa E501
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
                    f"✅ [Extract] Up-to-date for '{ds.name}' (hash={file_hash})",  # noqa E501
                    "INFO",
                )
            else:
                pkg.status = "completed"
                pkg.extract_status = "completed"
                pkg.stats = {"hash": file_hash}
                self.logger.log(
                    f"✅ [Extract] Completed for '{ds.name}' (hash={file_hash})", "INFO"  # noqa E501
                )
        else:
            pkg.status = "failed"
            pkg.extract_status = "failed"
            pkg.stats = {"error": message, "step": "extract"}
            self.logger.log(message, "ERROR")
            self.logger.log(
                f"⛔️ ETL halted for '{ds.name}' due to extract failure", "ERROR"  # noqa E501
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
                f"⚠️ No successful extract found for '{ds.name}' — cannot run transform."  # noqa E501
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
            extra_filters=[ETLPackage.transform_hash == last_extract.extract_hash],  # noqa E501
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
                f"⚙️  [Transform] Up-to-date for '{ds.name}' (package_id={pkg.id})",  # noqa E501
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
            f"⚙️  [Transform] Running for '{ds.name}' (package_id={pkg.id})", "INFO"  # noqa E501
        )

        dtp = module.DTP(
            logger=self.logger,
            debug_mode=self.debug_mode,
            datasource=ds,
            package=pkg,
            session=session,
            db=self.db,
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
    ) -> None:
        last_transform_ok = self._find_last_package(
            session=session,
            ds_id=ds.id,
            operation_type="transform",
            ok_statuses=["completed", "up-to-date"],
            order_field=ETLPackage.transform_end,
        )

        if not last_transform_ok:
            msg = f"⚠️ No successful transform found for '{ds.name}' — cannot run load."  # noqa E501
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
            extra_filters=[ETLPackage.load_hash == last_transform_ok.transform_hash],  # noqa E501
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
                f"🚚 [Load] Up-to-date for '{ds.name}' (package_id={pkg.id})", "INFO"  # noqa E501
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
                    self.logger.log(f"🧹 Deleted directory: {file_path}", "DEBUG")  # noqa E501
                else:
                    os.remove(file_path)
                    self.logger.log(f"🗑️ Deleted file: {file_path}", "DEBUG")
            except Exception as e:
                self.logger.log(f"⚠️ Could not delete {file_path}: {e}", "WARNING")  # noqa E501

    def _collect_purge_candidates(self, metadata: MetaData, key_name: str):
        candidates = []
        for tname, table in metadata.tables.items():
            if _is_etl_table(tname):
                continue
            if key_name in table.columns:
                candidates.append(table)
        return candidates

    def _simple_purge_by_data_source(
        self,
        session: Session,
        ds_id: int,
        *,
        commit: bool = True,
    ) -> dict[str, int]:
        engine = session.get_bind()
        metadata = MetaData()
        metadata.reflect(bind=engine)

        candidates = self._collect_purge_candidates(metadata, "data_source_id")

        if not candidates:
            self.logger.log("ℹ️ No non-ETL tables with `data_source_id` found.", "INFO")  # noqa E501
            return {}

        ordered = self._order_for_delete(candidates, metadata)
        deleted_rows_by_table: dict[str, int] = {}

        for table in ordered:
            # Cheap probe: stops at first match, with or without index.
            # Avoids issuing a DELETE on tables that have no rows for
            # this data source (saves planning, locks, WAL overhead).
            has_rows = session.execute(
                select(table.c.data_source_id)
                .where(table.c.data_source_id == ds_id)
                .limit(1)
            ).first()
            if not has_rows:
                continue

            if self.debug_mode:
                self.logger.log(
                    f"🗑️  Deleting rows from {table.name} "
                    f"(data_source_id={ds_id})",
                    "INFO",
                )

            result = session.execute(
                table.delete().where(table.c.data_source_id == ds_id)
            )
            affected = int(result.rowcount or 0)
            if affected > 0:
                deleted_rows_by_table[table.name] = (
                    deleted_rows_by_table.get(table.name, 0) + affected
                )

        if commit:
            session.commit()
        self.logger.log(f"✅ Simple purge complete for data_source_id={ds_id}.", "INFO")  # noqa E501
        return deleted_rows_by_table

    def _simple_purge_by_package(
        self,
        session: Session,
        package_id: int,
        *,
        commit: bool = True,
    ) -> dict[str, int]:
        engine = session.get_bind()
        metadata = MetaData()
        metadata.reflect(bind=engine)

        candidates = self._collect_purge_candidates(metadata, "etl_package_id")
        if not candidates:
            self.logger.log("ℹ️ No non-ETL tables with `etl_package_id` found.", "INFO")  # noqa E501
            return {}

        ordered = self._order_for_delete(candidates, metadata)
        deleted_rows_by_table: dict[str, int] = {}

        for table in ordered:
            has_rows = session.execute(
                select(table.c.etl_package_id)
                .where(table.c.etl_package_id == package_id)
                .limit(1)
            ).first()
            if not has_rows:
                continue

            if self.debug_mode:
                self.logger.log(
                    f"🗑️  Deleting rows from {table.name} "
                    f"(etl_package_id={package_id})",
                    "INFO",
                )

            result = session.execute(
                table.delete().where(table.c.etl_package_id == package_id)
            )
            affected = int(result.rowcount or 0)
            if affected > 0:
                deleted_rows_by_table[table.name] = (
                    deleted_rows_by_table.get(table.name, 0) + affected
                )

        if commit:
            session.commit()
        self.logger.log(f"✅ Simple purge complete for etl_package_id={package_id}.", "INFO")  # noqa E501
        return deleted_rows_by_table

    def _order_for_delete(self, candidates, metadata):
        cand_by_name = {t.name: t for t in candidates}
        override = [cand_by_name[n] for n in PURGE_ORDER_OVERRIDE if n in cand_by_name]  # noqa E501

        sorted_all = list(metadata.sorted_tables)
        sorted_candidates_child_first = [
            t
            for t in reversed(sorted_all)
            if t.name in cand_by_name and t.name not in PURGE_ORDER_OVERRIDE
        ]
        # reflected tables not present in metadata.sorted_tables
        missing = [
            t
            for t in candidates
            if t.name not in {x.name for x in override + sorted_candidates_child_first}  # noqa E501
        ]

        return override + sorted_candidates_child_first + missing
