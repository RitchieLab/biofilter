# tests/test_import.py

def test_import_gene():
    from biofilter.db.models.omics_models import Gene
    assert Gene.__tablename__ == "genes"
