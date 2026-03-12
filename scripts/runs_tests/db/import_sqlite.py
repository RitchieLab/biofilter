from biofilter import Biofilter

bf = Biofilter("sqlite:///biofilter_dev.db")

in_path = "/opt/biofilter/dev/biofilter/tests/outputs/export_sqlite/"

bf.db.import_(in_dir=in_path)

# TODO: get error from time stamp
