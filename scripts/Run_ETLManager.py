from biofilter import Biofilter

db_uri = "sqlite:///dev_biofilter.db"

bf = Biofilter(db_uri)

# bf.update(source_system=["HGNC"])
# bf.update(source_system=["HGNC"], run_steps=["load"], force_steps=["load"])
# bf.update(data_sources=["dbSNP_chrY"], run_steps=["load"], force_steps=["load"])
# bf.update(data_sources=["dbSNP_chr22"])
# bf.update(source_system=["dbSNP"], run_steps=["extract"], force_steps=["extract"])
# bf.update(
#     data_sources=["dbSNP_chrY"], run_steps=["load"], force_steps=["load"]
# )
bf.update(
    data_sources=["dbSNP_chr22"], run_steps=["load"], force_steps=["load"]
)
# bf.update(
#     data_sources=["dbSNP_SAMPLE"], run_steps=["load"], force_steps=["load"]
# )


print("Database updated successfully.")


# def run_etl_step(self, data_source_name: str, step: str, path_raw=None, path_processed=None):  # noqa: E501
#     ds = self.session.query(DataSource).filter_by(name=data_source_name).first()  # noqa: E501
#     if not ds:
#         self.logger.log(f"DataSource '{data_source_name}' not found.", "ERROR")  # noqa: E501
#         return

#     process = self.get_etl_process(ds)

#     script_module = importlib.import_module(f"biofilter.etl.dtps.{ds.dtp_script.lower()}")  # noqa: E501
#     dtp_instance = script_module.DTP(
#         logger=self.logger,
#         datasource=ds,
#         etl_process=process,
#         session=self.session,
#     )

#     if step == "extract":
#         return dtp_instance.extract(path_raw)
#     elif step == "transform":
#         return dtp_instance.transform(path_raw, path_processed)
#     elif step == "load":
#         return dtp_instance.load(None, path_processed)
#     else:
#         self.logger.log(f"Unknown ETL step '{step}'", "ERROR")

# manager.run_etl_step("HGNC", step="load", path_processed="biofilter_data/processed")  # noqa: E501

# manager.start_process(
#     source_system=["HGNC"],
#     download_path="biofilter_data/raw",
#     processed_path="biofilter_data/processed",
#     run_steps=["load"]
# )
