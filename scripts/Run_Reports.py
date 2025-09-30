from biofilter import Biofilter

# db_uri = "sqlite:///dev_biofilter.db"
db_uri = "postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter"

if __name__ == "__main__":

    bf = Biofilter(db_uri)

    print(bf.metadata.schema_version)

    # List reports
    # print(bf.report.list_reports())

    # Run specific report
    # df = bf.report.run_report("report_etl_status")

    result = bf.report.run_report(
        "report_gene_to_snp",
        assembly='38',
        input_data=[
            "TXLNGY",
            "HGNC:18473",
            "246126",
            "ENSG00000131002",
            "HGNC:5"
        ],
    )

    # def flatten_allele(val):
    #     if isinstance(val, list):
    #         return ";".join(val)
    #     return val

    # result["Ref Allele"] = result["Ref Allele"].apply(flatten_allele)
    # result["Alt Allele"] = result["Alt Allele"].apply(flatten_allele)


    print(result)
