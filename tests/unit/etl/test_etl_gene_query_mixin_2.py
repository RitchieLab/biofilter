# import pytest
from biofilter.etl.mixins.gene_query_mixin import GeneQueryMixin
from db.models.model_genes import (
    Gene,
    GeneGroupMembership,
    GenomicRegion,
)  # noqa: E501
from db.models.model_entities import Entity
from db.models.model_curation import (
    CurationConflict,
    ConflictStatus,
)  # noqa: E501


class DummyLogger:
    def log(self, msg, level="INFO"):
        print(f"[{level}] {msg}")


class GeneQueryTester(GeneQueryMixin):
    def __init__(self, session):
        self.session = session
        self.logger = DummyLogger()


def test_create_new_gene_without_conflict(db_session):
    tester = GeneQueryTester(db_session)

    # Create a base entity
    entity = Entity(group_id=1)
    db_session.add(entity)
    db_session.commit()

    gene = tester.get_or_create_gene(
        symbol="TP53",
        hgnc_id="HGNC:11998",
        entrez_id="7157",
        ensembl_id="ENSG00000141510",
        entity_id=entity.id,
        data_source_id=1,
        hgnc_status="Approved",
    )

    # Checks
    assert gene is not None
    assert gene.hgnc_id == "HGNC:11998"
    assert gene.entrez_id == "7157"
    assert gene.ensembl_id == "ENSG00000141510"
    assert gene.entity_id == entity.id

    # Check if the gene was saved in the database
    saved_gene = db_session.query(Gene).filter_by(hgnc_id="HGNC:11998").first()
    assert saved_gene is not None


def test_create_gene_when_already_exists_returns_existing_gene(db_session):
    tester = GeneQueryTester(db_session)

    # Fist gene go to the database
    gene_1 = tester.get_or_create_gene(
        symbol="TP53",
        hgnc_id="HGNC:11998",
        entrez_id="7157",
        ensembl_id="ENSG00000141510",
        entity_id=1,
        data_source_id=1,
    )

    # Second gene with the same IDs
    gene_2 = tester.get_or_create_gene(
        symbol="TP53",
        hgnc_id="HGNC:11998",
        entrez_id="7157",
        ensembl_id="ENSG00000141510",
        entity_id=1,
        data_source_id=1,
    )

    # Must return the same object
    assert gene_1.id == gene_2.id

    # Confirm that only one gene was created
    genes = db_session.query(Gene).all()
    assert len(genes) == 1


def test_create_duplicate_gene_should_return_existing(db_session):
    tester = GeneQueryTester(db_session)

    entity = Entity()
    db_session.add(entity)
    db_session.commit()

    # Gene original
    existing_gene = Gene(
        entity_id=entity.id,
        hgnc_id="HGNC:1234",
        entrez_id="201",
        ensembl_id="ENSG00000111111",
    )
    db_session.add(existing_gene)
    db_session.commit()

    # Tentativa de criar exatamente o mesmo gene
    gene = tester.get_or_create_gene(
        symbol="GeneDuplicate",
        hgnc_id="HGNC:1234",
        entrez_id="201",
        ensembl_id="ENSG00000111111",
        entity_id=entity.id,
    )

    # Deve retornar o mesmo gene, sem conflito
    assert gene is not None
    assert gene.id == existing_gene.id

    # Não deve haver conflitos registrados
    conflicts = db_session.query(CurationConflict).all()
    assert len(conflicts) == 0


def test_create_gene_with_conflict_entrez(db_session):
    tester = GeneQueryTester(db_session)

    entity1 = Entity()
    db_session.add(entity1)
    db_session.commit()

    gene1 = tester.get_or_create_gene(
        symbol="GENE1",
        hgnc_id="HGNC:001",
        entrez_id="123",
        ensembl_id="ENSG0001",
        entity_id=entity1.id,
        data_source_id=1,
    )
    assert gene1 is not None

    entity2 = Entity()
    db_session.add(entity2)
    db_session.commit()

    # Try to create a gene with the same entrez_id but different hgnc_id
    gene2 = tester.get_or_create_gene(
        symbol="GENE2",
        hgnc_id="HGNC:002",
        entrez_id="123",  # 👈 Conflict
        ensembl_id="ENSG0002",
        entity_id=entity2.id,
        data_source_id=1,
    )

    # Must return None due to the conflict
    assert gene2 is None

    # Must have a CurationConflict registered
    conflict = (
        db_session.query(CurationConflict)
        .filter_by(
            identifier="HGNC:002",
            existing_identifier="HGNC:001",
            status=ConflictStatus.pending,
        )
        .first()
    )
    assert conflict is not None
    assert "entrez_id=123" in conflict.description

    # New entity should be marked with has_conflict
    entity2 = db_session.query(Entity).get(entity2.id)
    assert entity2.has_conflict


def test_create_gene_with_conflict_ensembl(db_session):
    tester = GeneQueryTester(db_session)

    entity = Entity()
    db_session.add(entity)
    db_session.commit()

    existing_gene = Gene(
        entity_id=entity.id,
        hgnc_id="HGNC:0001",
        entrez_id="101",
        ensembl_id="ENSG00000123456",
    )
    db_session.add(existing_gene)
    db_session.commit()

    gene = tester.get_or_create_gene(
        symbol="GeneB",
        hgnc_id="HGNC:9999",
        entrez_id="102",
        ensembl_id="ENSG00000123456",  # 👈 Conflict
        entity_id=entity.id,
    )

    assert gene is None

    conflict = (
        db_session.query(CurationConflict)
        .filter_by(
            identifier="HGNC:9999",
            existing_identifier="HGNC:0001",
            status=ConflictStatus.pending,
        )
        .first()
    )
    assert conflict is not None
    assert "ensembl_id" in conflict.description


def test_create_gene_with_empty_symbol_returns_none(db_session):
    tester = GeneQueryTester(db_session)

    entity = Entity()
    db_session.add(entity)
    db_session.commit()

    gene = tester.get_or_create_gene(
        symbol="",
        hgnc_id="HGNC:9999",
        entrez_id="9999",
        ensembl_id="ENSG00000999999",
        entity_id=entity.id,
    )

    assert gene is None


def test_gene_group_association(db_session):
    tester = GeneQueryTester(db_session)

    entity = Entity()
    db_session.add(entity)
    db_session.commit()

    gene = tester.get_or_create_gene(
        symbol="GroupGene",
        hgnc_id="HGNC:2222",
        entrez_id="202",
        ensembl_id="ENSG00000222222",
        entity_id=entity.id,
        gene_group_names=["Pathway1", "Pathway2"],
    )

    assert gene is not None
    assert len(gene.groups) == 2

    group_names = {group.name for group in gene.groups}
    assert "Pathway1" in group_names
    assert "Pathway2" in group_names

    memberships = (
        db_session.query(GeneGroupMembership).filter_by(gene_id=gene.id).all()
    )  # noqa: E501
    assert len(memberships) == 2


def test_create_gene_location_success(db_session):
    tester = GeneQueryTester(db_session)

    entity = Entity()
    db_session.add(entity)

    gene = Gene(entity_id=1)
    db_session.add(gene)

    region = GenomicRegion(
        label="Region1", chromosome="12", start=1000, end=2000
    )  # noqa: E501
    db_session.add(region)
    db_session.commit()

    location = tester.create_gene_location(
        gene=gene,
        chromosome="12",
        start=1000,
        end=2000,
        strand="+",
        region=region,
        assembly="GRCh38",
        data_source_id=1,
    )

    assert location is not None
    assert location.chromosome == "12"
    assert location.start == 1000
    assert location.end == 2000
    assert location.strand == "+"
    assert location.region_id == region.id
    assert location.assembly == "GRCh38"
    assert location.data_source_id == 1


def test_create_gene_location_without_gene_returns_none(db_session):
    tester = GeneQueryTester(db_session)

    location = tester.create_gene_location(
        gene=None,
        chromosome="1",
        start=100,
        end=200,
        strand="+",
        region=None,
        assembly="GRCh38",
        data_source_id=1,
    )

    assert location is None
