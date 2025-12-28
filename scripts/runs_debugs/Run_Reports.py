from biofilter import Biofilter


# from urllib.parse import quote_plus
# user = "bioadmin"
# pwd  = quote_plus("xxx@1111")  # -> 'xxx%401111'
# host = "<SERVER_IP>"
# uri  = f"postgresql+psycopg2://{user}:{pwd}@{host}:5432/biofilter?sslmode=require"
# # engine = create_engine(uri)


# db_uri = "sqlite:///dev_biofilter.db"
# db_uri = "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter"
# db_uri = "postgresql+psycopg2://bioadmin:Penn%402025@109.199.114.191:5432/biofilter?sslmode=require"

db_uri = "postgresql+psycopg://bioadmin:bioadmin@109.199.114.191:5432/biofilter?sslmode=require"

bf = Biofilter(db_uri)

# print(bf.metadata.schema_version)

# List reports
# print(bf.report.list_reports())

# Run specific report
# df = bf.report.run_report("report_etl_status")

# ----= GENE TO VARIANTS =----
# ----------------------------
# result = bf.report.run_report(
#     "report_gene_to_snp",
#     assembly='38',
#     input_data=[
#         "TXLNGY",
#         "HGNC:18473",
#         "246126",
#         "ENSG00000131002",
#         "HGNC:5"
#     ],
# )

# def flatten_allele(val):
#     if isinstance(val, list):
#         return ";".join(val)
#     return val

# result["Ref Allele"] = result["Ref Allele"].apply(flatten_allele)
# result["Alt Allele"] = result["Alt Allele"].apply(flatten_allele)

# print(result)

# ----= POSITIONS TO GENES =----
# ------------------------------

# Run Report
result = bf.report.run_example("gene_to_snp")

print(result)
