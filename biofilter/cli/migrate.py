import subprocess
from packaging.version import parse as parse_version

from biofilter import __version__ as current_version
from biofilter.db.database import SessionLocal
from biofilter.db.models import BiofilterMetadata


def run_migration():
    session = SessionLocal()

    try:
        # Load schema version from database
        metadata = session.query(BiofilterMetadata).first()
        if not metadata:
            print("⚠️  No metadata found. Cannot determine schema version.")
            return

        db_version = metadata.schema_version

        print(f"📦 Current schema: {db_version} | Target version: {current_version}")

        if parse_version(current_version) > parse_version(db_version):
            print("🚀 Running Alembic migrations...")
            subprocess.run(["alembic", "upgrade", "head"], check=True)

            # Update metadata version
            metadata.schema_version = current_version
            session.commit()

            print(f"✅ Migration completed: {db_version} → {current_version}")
        else:
            print("✅ Schema already up-to-date. No migration needed.")

    except Exception as e:
        print(f"❌ Migration failed: {str(e)}")
        session.rollback()

    finally:
        session.close()
