from biofilter import Biofilter

bf = Biofilter("postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_dev")

out_path = "/opt/biofilter/dev/biofilter/tests/outputs/export_ps/"

bundle_path = bf.db.export(out_dir=out_path)

# bundle_path = bf.db.export(
#     out_dir=out_path,
#     fmt="parquet",
#     biofilter_version="4.0.0",
#     schema_version="4.0",
# )

# bundle_path = bf.db.export(
#     out_dir="out_path",
#     fmt="csv",
#     chunksize=50_000,
# )

# biofilter db export --out ./exports --fmt parquet