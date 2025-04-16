# import os
# import tempfile
import pytest
from biofilter import Biofilter
from biofilter.db.models.config_models import SystemConfig
from sqlalchemy import inspect


# Global Test
def test_create_new_project_creates_db(tmp_path):
    db_path = tmp_path / "test_biofilter.sqlite"
    db_uri = f"sqlite:///{db_path}"

    # Start a Biofilter instance
    bf = Biofilter()
    # Start a new project
    bf.create_new_project(db_uri=db_uri, overwrite=True)

    # Insure that the database file was created
    assert db_path.exists(), "Database file should exist"

    # Check if the connection is active
    assert bf.db.engine is not None
    assert bf.db.session is not None

    # Check if the expected tables were created
    inspector = inspect(bf.db.engine)
    tables = inspector.get_table_names()
    assert "system_config" in tables
    assert "etl_data_sources" in tables
    assert "etl_source_systems" in tables

    # Check if the database was seeded
    with bf.db.get_session() as session:
        config_count = session.query(SystemConfig).count()
        assert config_count > 0, "SystemConfig should be seeded"


def test_create_db_without_uri_raises_error():
    bf = Biofilter()
    with pytest.raises(ValueError, match="Database URI must be set"):
        bf._create_db()
