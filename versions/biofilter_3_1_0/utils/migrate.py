import os
from pathlib import Path
from alembic import command
from alembic.config import Config
from packaging.version import parse as parse_version
from biofilter.db.models import BiofilterMetadata
from biofilter.utils.version import __version__ as current_version


def run_migration(session_factory, db_uri: str):
    """
    Run Alembic migrations with dynamic config.
    """
    session = session_factory()

    try:
        metadata = session.query(BiofilterMetadata).first()
        print("[INFO] BiofilterMetadata found:", metadata)

        if not metadata:
            print("⚠️  No metadata found. Cannot determine schema version.")
            return

        os.environ["BIOFILTER_DB_URI"] = db_uri

        db_version = metadata.schema_version
        print(
            f"📦 Current schema: {db_version} | Target version: {current_version}"
        )  # noqa E501

        if parse_version(current_version) > parse_version(db_version):
            print("🚀 Running Alembic migrations...")

            # Absolute path to alembic folder
            base_dir = Path(__file__).resolve().parent.parent
            script_location = base_dir / "alembic"

            # Create a programatic config file
            config = Config()
            config.set_main_option("script_location", str(script_location))
            config.set_main_option("sqlalchemy.url", db_uri)  # ← IMPORTANT

            # Executa upgrade
            command.upgrade(config, "head")

            metadata.schema_version = current_version
            session.commit()
            print(f"✅ Migration completed: {db_version} → {current_version}")
        else:
            print("✅ Schema already up-to-date. No migration needed.")

    except Exception as e:
        print(f"❌ Migration failed: {str(e)}")
        session.rollback()
