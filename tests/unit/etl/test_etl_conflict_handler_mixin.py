from biofilter.db.models.omics_models import Gene
from biofilter.db.models.entity_models import Entity
from biofilter.db.models.curation_models import (
    CurationConflict,
    ConflictStatus,
)  # noqa: E501
from biofilter.etl.mixins.conflict_handler_mixin import ConflictHandlerMixin
from biofilter.utils.logger import Logger
from unittest.mock import MagicMock


# Instance of the test class with injected session
class ConflictHandler(ConflictHandlerMixin):
    def __init__(self, session):
        self.session = session
        self.logger = Logger()


def test_detect_gene_conflict_creates_conflict_record(db_session):
    handler = ConflictHandler(db_session)

    # Create existing entity with associated Gene
    entity = Entity()
    db_session.add(entity)
    db_session.flush()

    gene = Gene(
        hgnc_id="HGNC:12345",
        entrez_id="54321",
        ensembl_id="ENSG000001234",
        entity_id=entity.id,
    )
    db_session.add(gene)
    db_session.commit()

    # Create a new entity for the gene that will conflict
    new_entity = Entity()
    db_session.add(new_entity)
    db_session.flush()

    # New gene with same entrez_id but different hgnc_id → CONFLICT
    result = handler.detect_gene_conflict(
        hgnc_id="HGNC:67890",
        entrez_id="54321",  # same entrez_id
        ensembl_id=None,
        entity_id=new_entity.id,
        symbol="NEWGENE",
    )

    # Chacks if the conflict was logged
    assert result == "CONFLICT"  # conflict detected
    conflict = db_session.query(CurationConflict).first()
    assert conflict is not None

    assert conflict.identifier == "HGNC:67890"
    assert conflict.existing_identifier == "HGNC:12345"
    assert "entrez_id=54321" in conflict.description

    updated_entity = (
        db_session.query(Entity).filter_by(id=new_entity.id).first()
    )  # noqa: E501
    assert updated_entity.has_conflict is True


def test_detect_gene_conflict_returns_existing_when_no_conflict(db_session):
    handler = ConflictHandler(db_session)

    # Create existing entity with associated Gene
    entity = Entity()
    db_session.add(entity)
    db_session.flush()

    gene = Gene(
        hgnc_id="HGNC:12345",
        entrez_id="54321",
        ensembl_id="ENSG000001234",
        entity_id=entity.id,
    )
    db_session.add(gene)
    db_session.commit()

    # Same hgnc_id and entrez_id → no conflict
    result = handler.detect_gene_conflict(
        hgnc_id="HGNC:12345",
        entrez_id="54321",
        ensembl_id=None,
        entity_id=entity.id,
        symbol="GENE_A",
    )

    assert result == gene
    assert db_session.query(CurationConflict).count() == 0


def test_detect_gene_conflict_does_not_duplicate_conflict(db_session):
    handler = ConflictHandler(db_session)

    entity = Entity()
    db_session.add(entity)
    db_session.flush()

    gene = Gene(
        hgnc_id="HGNC:OLD",
        entrez_id="111",
        ensembl_id="ENSG000001111",
        entity_id=entity.id,
    )
    db_session.add(gene)
    db_session.commit()

    # Create conflict with existing gene
    new_entity = Entity()
    db_session.add(new_entity)
    db_session.flush()

    existing_conflict = CurationConflict(
        entity_type="gene",
        identifier="HGNC:NEW",
        existing_identifier="HGNC:OLD",
        status=ConflictStatus.pending,
        description="existing conflict",
        entity_id=new_entity.id,
    )
    db_session.add(existing_conflict)
    db_session.commit()

    # Same conflict is tested again
    result = handler.detect_gene_conflict(
        hgnc_id="HGNC:NEW",
        entrez_id="111",  # same entrez_id
        ensembl_id=None,
        entity_id=new_entity.id,
        symbol="DUPLICATE",
    )

    assert result == "CONFLICT"
    assert db_session.query(CurationConflict).count() == 1


def test_normalize_gene_identifiers_returns_clean_ids():
    handler = ConflictHandler(MagicMock())

    hgnc_id, entrez_id, ensembl_id = handler.normalize_gene_identifiers(
        " hgnc:1234 ", " 54321 ", "ensg000001111 "
    )

    assert hgnc_id == "HGNC:1234"
    assert entrez_id == "54321"
    assert ensembl_id == "ENSG000001111"


def test_normalize_gene_identifiers_handles_nan_and_none():
    handler = ConflictHandler(MagicMock())

    hgnc_id, entrez_id, ensembl_id = handler.normalize_gene_identifiers(
        "NaN", None, " nan "
    )

    assert hgnc_id is None
    assert entrez_id is None
    assert ensembl_id is None
