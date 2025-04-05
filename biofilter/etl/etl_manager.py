from biofilter.db.models.etl_models import ETLProcess, DataSource, SourceSystem
from datetime import datetime
from sqlalchemy.orm import Session
from biofilter.utils.logger import Logger
import importlib
from collections.abc import Iterable


class ETLManager:
    def __init__(self, session: Session):
        self.session = session
        self.logger = Logger()

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
            if isinstance(source_system, str) or not isinstance(source_system, Iterable):
                source_system = [source_system]

        query = self.session.query(DataSource).filter_by(active=True)

        if source_system:
            # filtrar os SourceSystems da lista
            query = query.join(SourceSystem).filter(SourceSystem.name.in_(source_system))

        data_sources = query.all()

        if not data_sources:
            self.logger.log("No active DataSources found.", "WARNING")
            return

        for ds in data_sources:
            process = self._init_or_restart_etl(ds, dtp_script or ds.dtp_version)

            try:
                script_module = importlib.import_module(f"etl.sources.{ds.dtp_version.lower()}")

                dtp_instance = script_module.DTP(
                    logger=self.logger,
                    datasource=ds,
                    etl_process=process,
                    session=self.session,
                    
                )

                # RUN EXTRACT FASE
                self.logger.log(f"Running extract() for {ds.name}", "INFO")
                extract_result = dtp_instance.extract(
                    download_path,
                    )

                # RUN TRANSFORM FASE
                self.logger.log(f"Running transform() for {ds.name}", "INFO")
                transform_df, transform_status = dtp_instance.transform(
                    download_path,
                    processed_path
                )

                # RUN LOAD FASE
                self.logger.log(f"Running load() for {ds.name}", "INFO")
                records = dtp_instance.load(transform_df, processed_path)

                # CLOCE PROCESS
                self.finish_process(
                    process,
                    status="completed",
                    records_processed=records,
                    tables_updated=ds.data_type  # ou outra info mais específica
                )

            except Exception as e:
                self.finish_process(
                    process,
                    status="failed",
                    error_message=str(e)
                )
                self.logger.log(f"❌ ETL failed for {ds.name}: {e}", "ERROR")

    def _init_or_restart_etl(self, data_source: DataSource, dtp_script: str) -> ETLProcess:
        """
        Cria ou reinicia o processo ETL associado a um único DataSource.
        """
        process = (
            self.session.query(ETLProcess)
            .filter_by(data_source_id=data_source.id)
            .first()
        )

        now = datetime.now()

        if process:
            self.logger.log(f"🔄 Updating ETLProcess for {data_source.name}", "INFO")
            process.start_time = now
            process.end_time = None
            process.status = "running"
            process.extract_status = None
            process.transform_status = None
            process.load_status = None
            process.error_message = None
            process.records_processed = 0
            process.tables_updated = None
            process.dtp_script = dtp_script
        else:
            self.logger.log(f"🚀 Creating ETLProcess for {data_source.name}", "INFO")
            process = ETLProcess(
                data_source_id=data_source.id,
                start_time=now,
                status="running",
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
        tables_updated=None,
    ):
        process.end_time = datetime.now()
        process.status = status

        if error_message:
            process.error_message = error_message

        if records_processed is not None:
            process.records_processed = records_processed

        if tables_updated:
            process.tables_updated = tables_updated

        self.session.commit()
        self.logger.log(
            f"ETLProcess {process.id} finished with status: {status}", "INFO"
        )











# # etl/etl_manager.py

# from biofilter.db.models.etl_models import ETLProcess, DataSource
# from sqlalchemy.orm import Session
# from utils.logger import Logger
# from datetime import datetime


# class ETLManager:
#     def __init__(self, db_session: Session):
#         self.session = db_session
#         self.logger = Logger()

#     def start_process(self, data_source_id: int, dtp_script: str) -> ETLProcess:
#         """
#         Cria um novo ETLProcess com status "running" e salva no banco.
#         """
#         process = ETLProcess(
#             data_source_id=data_source_id,
#             status="running",
#             dtp_script=dtp_script,
#             start_time=datetime.now()
#         )
#         self.session.add(process)
#         self.session.commit()
#         self.logger.log(f"🔁 ETLProcess iniciado para DataSource {data_source_id}.", "INFO")
#         return process

#     def update_status(self, process: ETLProcess, step: str, status: str):
#         """
#         Atualiza o status de um passo (extract, transform, load)
#         """
#         if step == "extract":
#             process.extract_status = status
#         elif step == "transform":
#             process.transform_status = status
#         elif step == "load":
#             process.load_status = status
#         else:
#             raise ValueError(f"Passo ETL desconhecido: {step}")

#         self.session.commit()
#         self.logger.log(f"🔄 ETL step '{step}' atualizado para '{status}'.", "DEBUG")

#     def complete_process(self, process: ETLProcess, records_processed: int, tables: list[str]):
#         process.end_time = datetime.now()
#         process.status = "completed"
#         process.records_processed = records_processed
#         process.tables_updated = ", ".join(tables)
#         self.session.commit()
#         self.logger.log(f"✅ ETLProcess {process.id} finalizado.", "INFO")

#     def fail_process(self, process: ETLProcess, message: str):
#         process.end_time = datetime.now()
#         process.status = "failed"
#         process.error_message = message
#         self.session.commit()
#         self.logger.log(f"❌ ETLProcess {process.id} falhou: {message}", "ERROR")
