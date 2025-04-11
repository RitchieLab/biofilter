from biofilter.db.models.etl_models import ETLProcess, DataSource, SourceSystem
from datetime import datetime
from sqlalchemy.orm import Session
from biofilter.utils.logger import Logger
import importlib
from collections.abc import Iterable
import pandas as pd


class ETLManager:
    def __init__(self, session: Session):
        self.session = session
        self.logger = Logger()

    def restart_etl_process(self, data_source_name: str) -> bool:
        """
        Restart ETL process for a given data source.

        How to use it:
            manager = ETLManager(session)
            manager.restart_etl_process("HGNC")

        TODO:
        - Delete data
        - Receive data_source_name as list
        - Receive Source System as list
        - Delete ETLProcess
        - Delete files
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
                self.session.query(ETLProcess).filter_by(data_source_id=ds.id).first()
            )

            if not process:
                self.logger.log(
                    f"⚠️ ETLProcess for '{data_source_name}' not found.", "WARNING"
                )
                self.logger.log(
                    f"🚀 Creating a new '{data_source_name}' ETLProcess.", "INFO"
                )

                process = ETLProcess(
                    data_source_id=ds.id,
                    global_status="pending",
                    extract_status="pending",
                    transform_status="pending",
                    load_status="pending",
                    dtp_script=ds.dtp_version,
                )
                self.session.add(process)
            else:
                self.logger.log(
                    f"🔄 Restarting ETLProcess for '{data_source_name}'", "INFO"
                )

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

                # NOTE: We keep original `dtp_script` to maintain reproducibility

            self.session.commit()
            self.logger.log(
                f"✅ ETLProcess for '{data_source_name}' has been restarted.", "INFO"
            )
            return True

        except Exception as e:
            self.logger.log(
                f"❌ Error restarting ETLProcess for '{data_source_name}': {e}", "ERROR"
            )
            return False

    def start_process(
        self,
        dtp_script: str = None,
        source_system: list = None,
        download_path: str = None,
        processed_path: str = None,
    ) -> None:
        """
        Inicia o(s) processo(s) ETL para todos os DataSources ativos.
        - Se source_systems for fornecido, filtra pelos nomes especificados.
        - Caso contrário, executa para todos os DataSources ativos.
        """
        if source_system is not None:
            if isinstance(source_system, str) or not isinstance(
                source_system, Iterable
            ):
                source_system = [source_system]

        query = self.session.query(DataSource).filter_by(active=True)

        if source_system:
            # filtrar os SourceSystems da lista
            query = query.join(SourceSystem).filter(
                SourceSystem.name.in_(source_system)
            )

        data_sources = query.all()

        if not data_sources:
            self.logger.log("No active DataSources found.", "WARNING")
            return

        for ds in data_sources:
            process = self._init_or_restart_etl(
                ds, dtp_script or ds.dtp_version
            )  # noqa: E501

            try:
                script_module = importlib.import_module(
                    f"biofilter.etl.sources.{ds.dtp_version.lower()}"
                )

                dtp_instance = script_module.DTP(
                    logger=self.logger,
                    datasource=ds,
                    etl_process=process,
                    session=self.session,
                )

                # NOTE: Pensar em como pular etapas se já foram feitas
                # # RUN EXTRACT FASE
                # self.logger.log(f"Running extract() for {ds.name}", "INFO")
                # extract_status = dtp_instance.extract(
                #     download_path,
                # )

                # # if ERROR, go to next DataSource
                # if not extract_status:
                #     continue

                # # RUN TRANSFORM FASE
                # self.logger.log(f"Running transform() for {ds.name}", "INFO")
                # transform_df, transform_status = dtp_instance.transform(
                #     download_path, processed_path
                # )

                # # if ERROR, go to next DataSource
                # if not transform_status:
                #     continue

                # # Check if the DataFrame is empty
                # if transform_df is None or transform_df.empty:
                #     self.logger.log(
                #         f"❌ Transform returned an empty DataFrame for {ds.name}",
                #         "ERROR",
                #     )
                #     continue

                transform_df = None  # Placeholder for the actual DataFrame

                # RUN LOAD FASE
                self.logger.log(f"Running load() for {ds.name}", "INFO")
                records, load_status = dtp_instance.load(transform_df, processed_path)

                if load_status:
                    process.load_end = datetime.now()
                    process.load_status = "completed"
                    msg = f"ETL completed for {ds.name}"
                    self.logger.log(msg, "INFO")
                    self.session.commit()
                else:
                    process.load_end = datetime.now()
                    process.load_status = "failed"
                    process.global_status = "failed"
                    msg = f"ETL failed for {ds.name}"
                    self.logger.log(msg, "ERROR")
                    self.session.commit()
                    continue

                # # CLOSING PROCESS
                # self.finish_process(
                #     process,
                #     status="completed",
                #     records_processed=records,
                #     # tables_updated=ds.data_type,
                # )

            except Exception as e:
                self.finish_process(process, status="failed", error_message=str(e))
                self.logger.log(f"❌ ETL failed for {ds.name}: {e}", "ERROR")

    def _init_or_restart_etl(
        self, data_source: DataSource, dtp_script: str
    ) -> ETLProcess:
        """
        Cria ou reinicia o processo ETL associado a um único DataSource.
        """
        process = (
            self.session.query(ETLProcess)
            .filter_by(data_source_id=data_source.id)
            .first()
        )

        # now = datetime.now()

        if process:
            self.logger.log(f"🔄 Updating ETLProcess for {data_source.name}", "INFO")
            process.global_status = "running"

            process.extract_start = None
            process.extract_end = None
            process.extract_status = "pending"

            process.transform_start = None
            process.transform_end = None
            process.transform_status = "pending"

            process.load_start = None
            process.load_end = None
            process.load_status = "pending"

            process.dtp_script = dtp_script
        else:
            self.logger.log(f"🚀 Creating ETLProcess for {data_source.name}", "INFO")
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
                dtp_script=dtp_script,
            )
            self.session.add(process)

        self.session.commit()
        return process

    def finish_process(
        self,
        process: ETLProcess,
        status="completed",
        error_message=None,
        records_processed=None,
    ):

        process.global_status = status

        if error_message:
            # process.error_message = error_message
            pass

        if records_processed is not None:
            # process.records_processed = records_processed
            pass

        self.session.commit()
        self.logger.log(
            f"ETLProcess {process.id} finished with status: {status}", "INFO"
        )
