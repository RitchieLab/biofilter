# import subprocess
# from pathlib import Path
from packaging.version import parse as parse_version

# from biofilter import __version__ as current_version
from biofilter.utils.version import __version__ as current_version

# from biofilter.db.database import SessionLocal
from biofilter.db.models import BiofilterMetadata


# def run_migration(session_factory):
#     """
#     Run alembic migrations on the current database.
#     Accepts a sessionmaker (not an active session).
#     """
#     session = session_factory()  # Create a new active session

#     try:
#         # exemplo: verificar se a tabela de metadados já existe
#         metadata = session.query(BiofilterMetadata).first()
#         print("[INFO] BiofilterMetadata found:", metadata)

#         if not metadata:
#             print("⚠️  No metadata found. Cannot determine schema version.")
#             return

#         db_version = metadata.schema_version

#         print(f"📦 Current schema: {db_version} | Target version: {current_version}")

#         if parse_version(current_version) > parse_version(db_version):
#             print("🚀 Running Alembic migrations...")

#             import subprocess
#             subprocess.run(["alembic", "upgrade", "head"], check=True)
#             # subprocess.run(
#             #     ["alembic", "-c", "biofilter/alembic.ini", "upgrade", "head"],
#             #     check=True
#             # ) s

#             base_path = Path(__file__).resolve().parent.parent  # points to biofilter/
#             ini_path = base_path / "alembic.ini"

#             subprocess.run(
#                 ["alembic", "-c", str(ini_path), "upgrade", "head"],
#                 check=True
#             )

#             metadata.schema_version = current_version
#             session.commit()
#             print(f"✅ Migration completed: {db_version} → {current_version}")

#             print("✅ Schema already up-to-date. No migration needed.")

#     except Exception as e:
#         session.rollback()
#         print(f"❌ Migration failed: {str(e)}")

from alembic.config import Config
from alembic import command
from pathlib import Path
import os

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
        print(f"📦 Current schema: {db_version} | Target version: {current_version}")

        if parse_version(current_version) > parse_version(db_version):
            print("🚀 Running Alembic migrations...")

            # Caminho absoluto até a pasta alembic (dentro do pacote)
            base_dir = Path(__file__).resolve().parent.parent
            script_location = base_dir / "alembic"

            # Monta o config programaticamente
            config = Config()
            config.set_main_option("script_location", str(script_location))
            # config.set_main_option("sqlalchemy.url", session.bind.url.render_as_string(hide_password=False))
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


