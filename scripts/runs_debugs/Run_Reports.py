from biofilter import Biofilter

# from urllib.parse import quote_plus
# user = "bioadmin"
# pwd  = quote_plus("xxx@1111")  # -> 'xxx%401111'
# host = "<SERVER_IP>"
# uri  = f"postgresql+psycopg2://{user}:{pwd}@{host}:5432/biofilter?sslmode=require"
# # engine = create_engine(uri)


# db_uri = "sqlite:///dev_biofilter.db"
# db_uri = "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_dev_2"
# db_uri = "postgresql+psycopg2://bioadmin:Penn%402025@109.199.114.191:5432/biofilter?sslmode=require"

# db_uri = "postgresql+psycopg://bioadmin:bioadmin@109.199.114.191:5432/biofilter?sslmode=require"

bf = Biofilter()

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
# result = bf.report.run("db_pg_index_stats")
# result = bf.report.explain("db_pg_table_stats")


# ------------------------------------------------------------------
# Report Variant Consequences
# ------------------------------------------------------------------
# items = [
#     "chrY:2781644:2781644",
#     "chrY:3579294:3579294",
#     "chr1:55516888:55516888",
#     "chr7:55019017:55019017",
# ]

# # Optional: input file instead of inline items
# input_path = "/opt/biofilter/dev/biofilter/scripts/runs_debugs/regions.txt"
# # df = bf.report.run(
# #     "variant_consequences",
# #     items=items,
# #     range_up=1,
# #     range_down=1,
# #     emit_not_found_rows=True,
# #     include_variant_only_rows=True,
# #     limit_variants_per_input=1000,
# # )

# # Alternative using input file:
# df = bf.report.run(
#     "variant_consequences",
#     input_path=input_path,
#     range_up=1000,
#     range_down=1000,
#     emit_not_found_rows=True,
#     include_variant_only_rows=True,
#     limit_variants_per_input=1000,
# )


df = bf.report.run(
    'snp_snp_model',
    input_data=['chr19:44904604', 'chr1:13259', 'chr15:63279422'],
    build=38,
    window_bp=0,
    group_entity_groups=['Pathways'],
    # relationship_types=['in_pathway'],
    gene_pair_scope='both_from_seed',
    # snp_pair_scope='at_least_one_from_seed',
)

print('rows:', len(df))
print(df)
