# # tests/db/test_database_errors.py

# import pytest
# from biofilter.biofilter.db.database import Database


# def test_database_connection_invalid_path():
#     invalid_path = "tests/data/missing.sqlite"

#     with pytest.raises(ValueError) as exc_info:
#         db = Database(invalid_path)

#     assert "Database not found" in str(exc_info.value)
