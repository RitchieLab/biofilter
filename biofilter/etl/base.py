# class BaseETL:
#     def __init__(self, biofilter, datasource, etl_process):
#         self.biofilter = biofilter
#         self.datasource = datasource
#         self.etl_process = etl_process
#         self.logger = biofilter.logger
#         self.session = biofilter.biofilter.db.get_session()
#         self.download_path = biofilter.settings.get("download_path")
#         self.processed_path = biofilter.settings.get("processed_path")
