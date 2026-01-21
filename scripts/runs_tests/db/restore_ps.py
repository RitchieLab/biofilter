from biofilter import Biofilter

bf = Biofilter()

# bf.db.create("sqlite:///biofilter_dev.db", overwrite=True)
bf.db.restore(
    # db_uri="postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_dev",
    input_path="/opt/biofilter/dev/biofilter/tests/outputs/blp_2.dump"
    )