from biofilter import Biofilter

bf = Biofilter("postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_dev")

in_path = "/opt/biofilter/dev/biofilter/tests/outputs/export_ps/"

bf.db.import_(
    in_dir=in_path
)

# bf.db.import_(
#     in_dir="/data/exports/biofilter_4_0_0",
#     fmt="parquet",
#     rebuild_indexes=True,
#     reset_postgres_sequences=True,
# )


# bf.db.import_(
#     in_dir="/mnt/artifacts/biofilter_ci_bundle",
#     fmt="parquet",
#     rebuild_indexes=False,
#     reset_postgres_sequences=True,
# )
# # rebuild indexes em etapa separada
# bf.rebuild_indexes()



# # 1. create schema
# bf.create_new_project(db_uri, overwrite=True)
# bf.migrate()

# # 2. import data
# bf.db.import_(
#     in_dir="/data/releases/biofilter_4_0_0",
#     fmt="parquet",
# )

# # 3. sanity checks (opcional)
# bf.report.run_report("qry_etl_status")


# biofilter db import --in ./exports/biofilter_4_0_0
# biofilter db import --in ./exports --no-rebuild-indexes
