import os
import shutil
import glob
import importlib
from typing import Union, List
from datetime import datetime

# from collections.abc import Iterable
from sqlalchemy import MetaData, func, select  # inspect
from sqlalchemy.orm import Session
from biofilter.utils.logger import Logger
from biofilter.db.models import (
    ETLProcess,
    DataSource,
    SourceSystem,
    ETLLog,
)  # noqa: E501

ETL_TABLE_PREFIX = "etl_"
PURGE_ORDER_OVERRIDE = [
    "VariantLocus",
    "VariantMaster",
    "EntityRelationship",
    "EntityName",
    "Entity",
    # ...
]


def _is_etl_table(table_name: str) -> bool:
    return table_name.lower().startswith(ETL_TABLE_PREFIX)


class ETLManager:
    def __init__(self, session: Session):
        self.session = session
        self.logger = Logger()

    def restart_etl_process(
        self,
        data_source: Union[str, List[str]] = None,
        source_system: Union[str, List[str]] = None,
        download_path: str = None,
        processed_path: str = None,
        delete_files: bool = True,
    ) -> bool:

        """
        Restarts one or more ETL processes, optionally filtering by DataSource
        name(s) or SourceSystem name(s).

        This will:
        - Reset ETLProcess statuses to "pending"
        - Clear hashes
        - Optionally remove files from disk
        - Log the restart in ETLLog

        Parameters:
        ----------
        data_source : str or list of str, optional
            Filter by one or more specific DataSource names.

        source_system : str or list of str, optional
            Filter by SourceSystem name(s) instead of DataSource.

        download_path : str, optional
            Directory containing raw data files to delete.

        processed_path : str, optional
            Directory containing transformed data files to delete.
        """

        # Normalize parameters
        if isinstance(data_source, str):
            data_source = [data_source]
        if isinstance(source_system, str):
            source_system = [source_system]

        # Base query
        query = self.session.query(DataSource)
        if data_source:
            query = query.filter(DataSource.name.in_(data_source))
        elif source_system:
            query = query.join(SourceSystem).filter(
                SourceSystem.name.in_(source_system)
            )  # noqa: E501
        data_sources = query.all()

        if not data_sources:
            self.logger.log("❌ No matching DataSources found.", "ERROR")
            return False

        for ds in data_sources:

            # PURGE simples (sem batch, sem LIMIT)
            self._simple_purge_by_data_source(ds_id=ds.id)

            process = (
                self.session.query(ETLProcess)
                .filter_by(data_source_id=ds.id)
                .first()  # noqa: E501
            )

            if not process:
                msg = f"🚀 Creating new ETLProcess for '{ds.name}'"
                self.logger.log(msg, "INFO")

                process = ETLProcess(
                    data_source_id=ds.id,
                    global_status="pending",
                    extract_status="pending",
                    transform_status="pending",
                    load_status="pending",
                    dtp_script=ds.dtp_script,
                )
                self.session.add(process)
                self.session.commit()
            else:
                msg = f"🔄 Restarting ETLProcess for '{ds.name}'"
                self.logger.log(msg, "INFO")
                # self._reset_etl_process_fields(process)
                process.global_status = "pending"
                process.extract_start = None
                process.extract_end = None
                process.extract_status = "pending"
                process.transform_start = None
                process.transform_end = None
                process.transform_status = "pending"
                process.load_start = None
                process.load_end = None
                process.load_status = "pending"
                process.raw_data_hash = None
                process.process_data_hash = None

            # Optional file deletion
            if download_path and delete_files:
                raw_path_data_source = os.path.join(
                    download_path, f"{ds.name.lower()}/"
                )  # noqa: E501
                self._delete_matching_files(raw_path_data_source)

            if processed_path and delete_files:
                processed_file = os.path.join(
                    processed_path, f"{ds.name.lower()}/"
                )  # noqa: E501
                self._delete_matching_files(processed_file)

            # Log to persistent log table
            log = ETLLog(
                etl_process_id=process.id,
                phase="global",
                action="restart",
                message=f"ETL process for '{ds.name}' restarted manually",
            )
            self.session.add(log)

        self.session.commit()
        return True

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

    def get_etl_process(
        self,
        data_source: DataSource,
    ) -> ETLProcess:  # noqa E501
        """
        Retrieves or initializes an ETLProcess object for the given DataSource.
        """
        process = (
            self.session.query(ETLProcess)
            .filter_by(data_source_id=data_source.id)
            .first()
        )

        if not process:
            msg = f"Creating ETLProcess for {data_source.name}"
            self.logger.log(msg, "INFO")
            process = ETLProcess(
                data_source_id=data_source.id,
                global_status="running",
                extract_start=None,
                extract_end=None,
                extract_status="pending",
                transform_start=None,
                transform_end=None,
                transform_status="pending",
                load_start=None,
                load_end=None,
                load_status="pending",
                dtp_script=data_source.dtp_script,
            )
            self.session.add(process)

        self.session.commit()
        return process

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
        Runs ETL pipeline Extract→Transform→Load for specified data sources.

        Parameters:
        - source_system (str or list[str], optional): Filter by source system.
        - data_sources (str or list[str], optional): Filter by specific ds.
        - download_path (str, optional): Path for raw file downloads.
        - processed_path (str, optional): Path for processed files.
        - run_steps (list[str], optional): Which steps to run. Default: all.
        - force_steps (list[str], optional): Steps to force rerun.
        - use_conflict_csv (bool, optional): Use conflict CSV during load.

        Notes:
        - Only active DataSources are processed.
        - Each stage is skipped if the previous one fails.
        - Status updates and errors are recorded in the database.
        """

        if run_steps is None:
            run_steps = ["extract", "transform", "load"]

        # Normalize inputs
        if isinstance(source_system, str):
            source_system = [source_system]
        if isinstance(data_sources, str):
            data_sources = [data_sources]

        # Validate input
        if not source_system and not data_sources:
            msg = "❌ No source_system or data_sources provided. Aborting."
            self.logger.log(msg, "ERROR")
            return

        # Build base query
        query = self.session.query(DataSource).filter_by(active=True)

        if source_system:
            query = query.join(SourceSystem).filter(
                SourceSystem.name.in_(source_system)
            )  # noqa: E501

        if data_sources:
            query = query.filter(DataSource.name.in_(data_sources))

        datasources_to_process = query.all()

        # Finish if no DataSources are found
        if not datasources_to_process:
            self.logger.log(
                "⚠️ No matching active DataSources found.", "WARNING"
            )  # noqa: E501
            return

        for ds in datasources_to_process:
            # Get or Create ETLProcess
            process = self.get_etl_process(ds)

            try:
                # Get DTP module
                script_module = importlib.import_module(
                    f"biofilter.etl.dtps.{ds.dtp_script.lower()}"
                )

                # Instantiate DTP class
                dtp_instance = script_module.DTP(
                    logger=self.logger,
                    datasource=ds,
                    etl_process=process,
                    session=self.session,
                    use_conflict_csv=use_conflict_csv,
                )

                # Open df to hosting data after transformation
                transform_df = None

                # Status options: pending, running, completed, failed

                # EXTRACT PHASE
                # if process.extract_status == "pending":
                if "extract" in run_steps and (
                    process.extract_status
                    in ("pending", "completed", "failed")  # noqa E501
                    or "extract" in force_steps
                ):  # noqa E501

                    if force_steps:
                        force_flag = True
                    else:
                        force_flag = False

                    process.extract_start = datetime.now()
                    process.extract_status = "running"
                    self.session.commit()
                    self.logger.log(
                        f"📦 [Extract] Running for {ds.name}", "INFO"
                    )  # noqa E501
                    status, message, file_hash = dtp_instance.extract(
                        raw_dir=download_path,
                        force_steps=force_flag,
                        # source_url=ds.source_url, # TODO: Add this to the DTP class. # noqa E501
                        # last_hash=process.raw_data_hash, # TODO Use internal class call # noqa E501
                    )  # noqa E501
                    process.extract_end = datetime.now()

                    # Check if the file_hash is new
                    if status:
                        if process.raw_data_hash != file_hash:
                            # New data detected, Restart transform/load phases
                            process.raw_data_hash = file_hash
                            process.extract_status = "completed"
                            process.transform_status = "pending"
                            process.load_status = "pending"
                            msg = f"Data changed for {ds.name}, reprocessing transform/load steps"  # noqa E501
                        else:
                            # No change detected, set transform/load phases to completed                        # noqa E501
                            process.extract_status = "completed"
                            process.transform_status = "completed"
                            process.load_status = "completed"
                            msg = f"No changes in data for {ds.name} (hash unchanged), skipping transform/load"  # noqa E501
                        self.logger.log(msg, "INFO")
                        log = ETLLog(
                            etl_process_id=process.id,
                            phase="extract",
                            action="info",
                            message=str(msg),
                        )
                        self.session.add(log)
                        self.session.commit()
                    else:
                        process.extract_status = "failed"
                        process.global_status = "failed"
                        self.logger.log(
                            f"❌ [Extract] Failed for {ds.name}", "ERROR"
                        )  # noqa E501
                        # Record the error in the persistent log
                        log = ETLLog(
                            etl_process_id=process.id,
                            phase="extract",
                            action="error",
                            message=str(message),
                        )
                        self.session.add(log)
                        self.session.commit()
                        continue  # Leving the process to "failed" for now

                # TRANSFORM PHASE
                if "transform" in run_steps and (
                    process.transform_status == "pending"
                    and process.extract_status == "completed"
                    or "transform" in force_steps
                ):  # noqa E501
                    process.transform_start = datetime.now()
                    process.transform_status = "running"
                    self.session.commit()
                    self.logger.log(
                        f"📦 [Transform] Running for {ds.name}", "INFO"
                    )  # noqa E501
                    transform_df, status, message = dtp_instance.transform(
                        download_path, processed_path
                    )  # noqa E501

                    if transform_df is None or transform_df.empty:
                        # status = False
                        # message = "Transform returned an empty DataFrame"
                        transform_df = None

                    process.transform_end = datetime.now()
                    if status:
                        process.transform_status = "completed"
                        self.session.commit()
                    else:
                        process.transform_status = "failed"
                        process.global_status = "failed"
                        self.logger.log(
                            f"❌ [Transform] Failed for {ds.name}", "ERROR"
                        )  # noqa E501
                        # Record the error in the persistent log
                        log = ETLLog(
                            etl_process_id=process.id,
                            phase="Transform",
                            action="error",
                            message=str(message),
                        )
                        self.session.add(log)
                        self.session.commit()
                        continue  # Leving the process to "failed" for now

                # LOAD PHASE
                if "load" in run_steps and (
                    process.load_status == "pending"
                    and process.transform_status == "completed"
                    or "load" in force_steps
                ):  # noqa E501
                    process.load_start = datetime.now()
                    process.load_status = "running"
                    self.session.commit()
                    self.logger.log(
                        f"📦 [Load] Running for {ds.name}", "INFO"
                    )  # noqa E501
                    records, status, message = dtp_instance.load(
                        transform_df, processed_path
                    )  # noqa E501
                    # TODO: na funcao essa como processed_dir

                    process.load_end = datetime.now()
                    if status:
                        process.load_status = "completed"
                        self.session.commit()
                    else:
                        process.load_status = "failed"
                        process.global_status = "failed"
                        self.logger.log(
                            f"❌ [Load] Failed for {ds.name}", "ERROR"
                        )  # noqa E501
                        # Record the error in the persistent log
                        log = ETLLog(
                            etl_process_id=process.id,
                            phase="load",
                            action="error",
                            message=str(message),
                        )
                        self.session.add(log)
                        self.session.commit()
                        continue  # Leving the process to "failed" for now

                # COMPLETE PROCESS
                if (
                    process.extract_status == "completed"
                    and process.transform_status == "completed"
                    and process.load_status == "completed"
                ):
                    process.global_status = "completed"
                    self.session.commit()
                    self.logger.log(f"✅ ETL completed for {ds.name}", "INFO")

            except Exception as e:
                self.logger.log(f"❌ ETL failed for {ds.name}: {e}", "ERROR")
                process.global_status = "failed"

                # Record the error in the persistent log
                log = ETLLog(
                    etl_process_id=process.id,
                    phase="global",
                    action="error",
                    message=str(e),
                )
                self.session.add(log)

                self.session.commit()

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
            self.logger.log("ℹ️ No non-ETL tables with `data_source_id` found.", "INFO") # noqa E501
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
                f"🗑️  Deleting {cnt} rows from {table.name} (data_source_id={ds_id})", # noqa E501
                "INFO",
            )
            self.session.execute(table.delete().where(table.c.data_source_id == ds_id)) # noqa E501
            total += cnt

        self.session.commit()
        self.logger.log(
            f"✅ Simple purge complete for data_source_id={ds_id}. Total rows: {total}", # noqa E501
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
                cand_by_name[n] for n in PURGE_ORDER_OVERRIDE if n in cand_by_name # noqa E501
            ]
            # Add any remaining not covered by override at the end (still children->parents via FK) # noqa E501
            rest = [t for t in candidates if t.name not in PURGE_ORDER_OVERRIDE] # noqa E501
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
