from biofilter import Biofilter

bf = Biofilter("sqlite:///biofilter_dev.db")

out_path = "/opt/biofilter/dev/biofilter/tests/outputs/export_sqlite/"

bundle_path = bf.db.export(out_dir=out_path)
