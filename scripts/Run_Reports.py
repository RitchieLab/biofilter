from biofilter import Biofilter

db_uri = "sqlite:///dev_biofilter.db"

if __name__ == "__main__":

    bf = Biofilter(db_uri)

    print(bf.metadata.schema_version)

    # List reports
    print(bf.list_reports())

    # Run specific report
    df = bf.run_report("qry_etl_status")

    print(df)