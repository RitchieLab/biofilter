import pandas as pd
from biofilter.report.reports.base_report import ReportBase

# from biofilter.db.models import SourceSystem, DataSource, ETLProcess


class ETLStatusReport(ReportBase):
    name = "qry_etl_status"
    description = "Summarizes the ETL status for each data source"

    def run(self):
        # Subquery: obter o ETLProcess mais recente por data_source_id (pelo ID máximo)
        # subq = (
        #     self.session.query(ETLProcess.data_source_id, ETLProcess.id.label("etl_id"))
        #     .group_by(ETLProcess.data_source_id)
        #     .order_by(ETLProcess.data_source_id, ETLProcess.id.desc())
        #     .subquery()
        # )

        # # Join com DataSource, SourceSystem e o último ETLProcess
        # query = (
        #     self.session.query(
        #         DataSource.name.label("data_source"),
        #         DataSource.data_type,
        #         DataSource.format,
        #         DataSource.grch_version,
        #         DataSource.ucschg_version,
        #         DataSource.schema_version,
        #         DataSource.dtp_version,
        #         DataSource.last_update,
        #         SourceSystem.name.label("source_system"),
        #         ETLProcess.global_status,
        #         ETLProcess.extract_start,
        #         ETLProcess.extract_end,
        #         ETLProcess.transform_start,
        #         ETLProcess.transform_end,
        #         ETLProcess.load_start,
        #         ETLProcess.load_end,
        #     )
        #     .join(SourceSystem, DataSource.source_system_id == SourceSystem.id)
        #     .outerjoin(subq, subq.c.data_source_id == DataSource.id)
        #     .outerjoin(ETLProcess, ETLProcess.id == subq.c.etl_id)
        #     .order_by(SourceSystem.name, DataSource.name)
        # )

        # results = query.all()

        # if not results:
        #     self.logger.warning("No ETL process data found.")
        #     return pd.DataFrame()

        # df = pd.DataFrame(results)

        # return df
        return None
