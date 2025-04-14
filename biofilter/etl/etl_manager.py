import importlib
from datetime import datetime
from collections.abc import Iterable
from sqlalchemy.orm import Session
from biofilter.utils.logger import Logger
from biofilter.db.models.etl_models import ETLProcess, DataSource, SourceSystem, ETLLog                     # noqa: E501


class ETLManager():
    def __init__(self, session: Session):
        self.session = session
        self.logger = Logger()

    """
    TODO: NEW METHOD TO:
        - Delete DB Data from Sources
        - Delete files
        - Receive data_source_name as list
        - Receive Source System as list
        - Reset ETLProcess Records
        - Write ETLLog Records
    """

    def restart_etl_process(self, data_source_name: str) -> bool:
        """
        Restart ETL process for a given data source.
        """
        try:
            ds = (
                self.session.query(DataSource)
                .join(SourceSystem)
                .filter(DataSource.name == data_source_name)
                .first()
            )

            if not ds:
                self.logger.log(
                    f"❌ DataSource '{data_source_name}' not found.", "ERROR"
                )
                return False

            process = (
                self.session.query(ETLProcess)
                .filter_by(data_source_id=ds.id)
                .first()
            )                                                                                               # noqa: E501
            if not process:
                msg = f"⚠️ ETLProcess for '{data_source_name}' not found."
                self.logger.log(msg, "WARNING")
                msg = f"🚀 Creating a new ETLProcess for '{data_source_name}'."
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
            else:
                msg = f"🔄 Restarting ETLProcess for '{data_source_name}'"
                self.logger.log(msg, "INFO")

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

            self.session.commit()

            msg = f"ETLProcess for '{data_source_name}' has been restarted."
            self.logger.log(msg, "INFO")
            return True

        except Exception as e:
            msg = f"❌ Error restarting ETLProcess for '{data_source_name}': {e}"                           # noqa: E501
            self.logger.log(msg, "ERROR")
            return False

    def get_etl_process(
            self,
            data_source: DataSource,
    ) -> ETLProcess:                                                                                       # noqa E501
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
        download_path: str = None,
        processed_path: str = None,
    ) -> None:
        """
        Runs ETL pipeline Extract→Transform→Load for all active data sources.

        Parameters:
        - source_system (str or list[str], optional): Filter by source system.
        - download_path (str, optional): Path for raw file downloads.
        - processed_path (str, optional): Path for processed CSV files.

        Notes:
        - Only active DataSources are processed.
        - Each stage is skipped if the previous one fails.
        - Status updates and errors are recorded in the database.
        """

        # Prepare DTPs to process
        query = self.session.query(DataSource).filter_by(active=True)
        if source_system is not None:
            if isinstance(source_system, str) or not isinstance(source_system, Iterable):       # noqa E501
                source_system = [source_system]
        if source_system:
            query = query.join(SourceSystem).filter(SourceSystem.name.in_(source_system))       # noqa E501
        datasources_to_process = query.all()

        # Finish if no DataSources are found
        if not datasources_to_process:
            self.logger.log("⚠️ No active DataSources found.", "WARNING")
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
                )

                # Open df to hosting data after transformation
                transform_df = None

                # Status options: pending, running, completed, failed

                # EXTRACT PHASE
                # TODO: Pensar em como reprocessar com status completed
                if process.extract_status == "pending":
                    process.extract_start = datetime.now()
                    process.extract_status = "running"
                    self.session.commit()
                    self.logger.log(f"📦 [Extract] Running for {ds.name}", "INFO")                          # noqa E501
                    status, message, file_hash = dtp_instance.extract(download_path)                        # noqa E501
                    process.extract_end = datetime.now()

                    # Check if the file_hash is new
                    if process.raw_data_hash != file_hash and status:
                        process.extract_status = "completed"
                        process.raw_data_hash = file_hash
                        self.session.commit()
                    elif process.raw_data_hash == file_hash and status:
                        process.extract_status = "completed"
                        process.global_status = "completed"
                        message = f"[Extract] No new data for {ds.name}"
                        self.logger.log(message, "INFO")
                        log = ETLLog(
                            etl_process_id=process.id,
                            phase="extract",
                            action="info",
                            message=str(message),
                        )
                        self.session.add(log)
                        continue  # Go to the next process
                    else:
                        process.extract_status = "failed"
                        process.global_status = "failed"
                        self.logger.log(f"❌ [Extract] Failed for {ds.name}", "ERROR")                      # noqa E501
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
                if process.transform_status == "pending" and process.extract_status == "completed":         # noqa E501
                    process.transform_start = datetime.now()
                    process.transform_status = "running"
                    self.session.commit()
                    self.logger.log(f"📦 [Transform] Running for {ds.name}", "INFO")                        # noqa E501
                    transform_df, status, message = dtp_instance.transform(download_path, processed_path)   # noqa E501
                    # No data returned set status to False
                    if transform_df is None or transform_df.empty:
                        status = False
                        message = "Transform returned an empty DataFrame"
                    process.transform_end = datetime.now()
                    if status:
                        process.transform_status = "completed"
                        self.session.commit()
                    else:
                        process.transform_status = "failed"
                        process.global_status = "failed"
                        self.logger.log(f"❌ [Transform] Failed for {ds.name}", "ERROR")                    # noqa E501
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
                if process.load_status == "pending" and process.transform_status == "completed":           # noqa E501 
                    process.load_start = datetime.now()
                    process.load_status = "running"
                    self.session.commit()
                    self.logger.log(f"📦 [Load] Running for {ds.name}", "INFO")                             # noqa E501
                    records, status, message = dtp_instance.load(transform_df, processed_path)              # noqa E501

                    process.load_end = datetime.now()
                    if status:
                        process.load_status = "completed"
                        self.session.commit()
                    else:
                        process.load_status = "failed"
                        process.global_status = "failed"
                        self.logger.log(f"❌ [Load] Failed for {ds.name}", "ERROR")                         # noqa E501
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
