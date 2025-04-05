import os
import shutil
import datetime
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy.orm import Session
from sqlalchemy import select, update
from omics_modules.models import DataSource, WorkProcess


class UpdaterWorkflowMixin:

    def workflow(self):
        """
        Orchestrates the entire ETL process for available data sources.
        1. Synchronizes available sources with the database.
        2. Determines which sources to process (user-specified or all active).
        3. Loads and instantiates source classes.
        4. Executes the download phase.
        5. Updates download status in the database.
        6. Executes the processing phase.
        7. Updates final status and cleans up files if necessary.
        """

        self.logger.log("[INFO] Starting workprocess...")

        # 1️⃣ SYNCHRONIZE DATA SOURCES
        # =====================================================================
        self.sync_data_sources()  # Ensure DB is up to date with sources

        # 2️⃣ DETERMINE WHICH SOURCES TO PROCESS
        # =====================================================================
        self.logger.log("[INFO] Checking sources to process...")

        # Attach sources will fill the attrs:
        # _objectSources, _sourceOptions, _sourceVersions
        self.attachSourceModules(self.source_list)

        with Session(self._engine) as session:
            # ✅ Fetch only active DataSource records
            qryset_datasource = session.scalars(
                select(DataSource).where(
                    DataSource.name.in_(list(self._sourceObjects.keys())),
                    DataSource.active == True,
                )
            ).all()

        if not qryset_datasource:
            self.logger.log(
                "No valid data sources found in the database!", level="ERROR"
            )
            return

        # 3️⃣ DOWNLOAD PHASE
        # =====================================================================
        self.logger.log("[INFO] Starting download phase...")

        if self.skip_download:
            self.logger.log("[INFO] Skipping download phase.")
            successful_sources = [ds.name for ds in qryset_datasource]
        else:
            downloadStatus = {}  # Stores download results (True/False)

            def execute_download(data_source):
                """Executes download for a single data source and updates its status."""
                srcName = data_source.name

                self.set_datasource_status(qryset_datasource, srcName, "downloading")

                success, error_message = self.workflow_download(
                    self.dir_download, srcName, self._sourceOptions.get(srcName, {})
                )

                downloadStatus[srcName] = success
                # TODO Adicionar a mensagem de erro no log doDB?
                if not success:
                    self.logger.log(f"[ERROR] {error_message}", level="ERROR")

            # 🚨 Using ThreadPoolExecutor for parallel downloads
            with ThreadPoolExecutor(max_workers=5) as executor:
                executor.map(execute_download, qryset_datasource)

            # 4️⃣ UPDATE STATUS AFTER DOWNLOAD
            # =====================================================================
            for srcName, success in downloadStatus.items():
                new_status = "downloaded" if success else "failed_download"
                self.set_datasource_status(
                    qryset_datasource, srcName, new_status
                )  # noqa E501

            # Keep only the successful sources for processing
            successful_sources = [
                ds.name
                for ds in qryset_datasource
                if downloadStatus.get(ds.name, False)  # noqa E501
            ]

        # 5️⃣ PROCESSING PHASE
        # =====================================================================
        self.logger.log("[INFO] Starting processing phase...")

        for srcName in successful_sources:
            # TODO We can check if the source is already downloaded!
            self.set_datasource_status(
                qryset_datasource, srcName, "processing"
            )  # noqa E501

            try:
                # Setting up the work process
                srcObj = self._sourceObjects[srcName]
                srcID = next(
                    (ds.id for ds in qryset_datasource if ds.name == srcName), None
                )  # noqa E501
                srcObj.datasource_id = srcID
                options = self._sourceOptions.get(srcName, {})
                path = os.path.join(self.dir_download, srcName)

                if srcID is None:
                    error = f"[ERROR] Data Source ID not found for {srcName}"
                    raise ValueError(error)

                # Register work process in the database
                with Session(self._engine) as session:
                    work_process = WorkProcess(
                        data_source_id=srcID,
                        status="running",
                        dtp_script=f"omics_source_{srcName}.py",
                    )
                    session.add(work_process)
                    session.commit()
                    work_id = work_process.id

                # 🚨 Call the update method from the source system
                srcObj.update(options, path)

                status = "completed"
                error = None
                self.logger.log(f"[INFO] {srcName} processed successfully.")

            except Exception as e:
                status = "failed"
                error = f"[ERROR] Processing failed for {srcName}: {str(e)}"
                self.logger.log(error, level="ERROR")

            finally:
                # Update work process status
                with Session(self._engine) as session:
                    session.execute(
                        update(WorkProcess)
                        .where(WorkProcess.id == work_id)
                        .values(
                            status=status,
                            error_message=error,
                            end_time=datetime.datetime.now(
                                datetime.timezone.utc
                            ),  # noqa E501
                        )
                    )
                    session.commit()

            # 6️⃣ CLEANUP PHASE
            # =====================================================================
            if not self.keep_download:
                shutil.rmtree(path)
                self.logger.log(
                    f"[INFO] Removed downloaded files for {srcName}"
                )  # noqa E501

        self.logger.log("[INFO] Workprocess completed.")
