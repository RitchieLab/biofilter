from biofilter import Biofilter

bf = Biofilter("sqlite:///biofilter_dev.db")
bf.db.restore(
    input_path="/opt/biofilter/dev/biofilter/tests/outputs/backup_sqlite_scrip.sql"
    )