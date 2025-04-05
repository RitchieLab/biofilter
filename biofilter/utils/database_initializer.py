# biofilter/init/init_biofilter.db.py

from sqlalchemy import create_engine
from biofilter.db.models import Base  # Importa declarative_base
import os


def init_db(db_path="biofilter.sqlite", overwrite=False):
    if os.path.exists(db_path):
        if overwrite:
            os.remove(db_path)
        else:
            raise FileExistsError(
                f"{db_path} already exists. Use overwrite=True to replace it."
            )

    engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(engine)
    print(f"✅ Database created at: {db_path}")


"""
from biofilter.init.database_initializer import init_db
init_db(overwrite=True)
"""
