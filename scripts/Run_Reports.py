from biofilter import Biofilter

db_uri = "sqlite:///dev_biofilter.db"

if __name__ == "__main__":

    bf = Biofilter(db_uri)

    print(bf.metadata.schema_version)

    # List reports
    # print(bf.report.list_reports())

    # Run specific report
    # df = bf.report.run_report("report_etl_status")

    df = bf.report.run_report(
        "report_entity_filter",
        input_data=["A0A087X1C5", "A0AV02", "rs456", "Q68D04", "LOC130057800"],
    )

    print(df)
