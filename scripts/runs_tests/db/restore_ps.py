from biofilter import Biofilter

bf = Biofilter("postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_test")

# bf.db.create("sqlite:///biofilter_dev.db", overwrite=True)
bf.db.restore(
    # db_uri="postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_dev",
    input_path="/opt/biofilter/dev/biofilter/tests/outputs/bkp_db_dev.dump"
    )
