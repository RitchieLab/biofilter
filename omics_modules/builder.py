# from sqlalchemy import text
from omics_modules.omics_db import Database
from omics_modules.omics_updater import Updater


def main():
    """
    Instantiates OmicsDB and runs a basic test to verify database connection
    and integrity.
    """

    # Step 1: Initialize the OmicsDB instance
    # Step 2: Initialize the Updater instance
    # Step 3: Run Updater.workprocess() with source

    # ARGUMENTS
    DATABASE_FILE = "/Users/andrerico/Works/Sys/biofilter/data/omic.db",
    SOURCES_LIST = ['dbsnp']
    DOWNLOAD_FOLDER = "/Users/andrerico/Works/Sys/biofilter/data/downloads"
    UPDATER_PROCESS = True
    ONLY_DOWNLOAD = False
    SKIP_DOWNLOAD = False
    KEEP_DOWNLOAD = True

    # try:
    # Initialize the OmicsDB instance
    db = Database(
        dbFile=DATABASE_FILE,
        updating=UPDATER_PROCESS
    )
    print("✅ [SUCCESS] OmicsDB instance created successfully!")

    # Initialize the Updater instance
    updater = Updater(db)

    # Setting Updater options
    updater.keep_download = KEEP_DOWNLOAD
    updater.only_download = ONLY_DOWNLOAD
    updater.skip_download = SKIP_DOWNLOAD
    updater.dir_download = DOWNLOAD_FOLDER
    updater.source_list = SOURCES_LIST

    print("✅ [SUCCESS] Updater instance created successfully!")

    # Run Updater.workprocess() with sources
    updater.workflow()
    print("✅ [SUCCESS] Updater.workprocess() executed successfully!")


    # Testing session creation
    # with db.get_session() as session:
    #     result = session.execute(text("SELECT name FROM sqlite_master WHERE type='table';"))
    #     tables = result.fetchall()
    #     print(f"📋 [INFO] Tables in database: {tables}")

    # print("🎉 [TEST COMPLETED] Database connection and integrity verified successfully!")

    # except Exception as e:
    #     print(f"❌ [ERROR] An error occurred during OmicsDB initialization: {e}")


if __name__ == "__main__":
    main()
