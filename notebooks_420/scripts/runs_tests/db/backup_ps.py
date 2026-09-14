from biofilter import Biofilter

bf = Biofilter()

# bf.db.create("sqlite:///biofilter_dev.db", overwrite=True)
bf.db.backup(
    # db_uri="postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_dev",
    output_path="/opt/biofilter/dev/biofilter/tests/outputs/bkp_db_dev.dump"
)
