import pytest
from biofilter.db.models import Gene, GeneGroup


def test_create_gene_group(db_session):
    group = GeneGroup(
        name="Protein-Coding Genes",
        description="Main coding genes"
    )  # noqa: E501
    db_session.add(group)
    db_session.commit()

    result = db_session.query(GeneGroup).filter_by(name="Protein-Coding Genes").first()  # noqa: E501
    assert result is not None
    assert result.name == "Protein-Coding Genes"
    assert result.description == "Main coding genes"


def test_create_gene_with_location(db_session):
    gene = Gene(
        entity_id=1,
        hgnc_id="HGNC:1100",
        entrez_id="1234",
        ensembl_id="ENSG000001234",
        chromosome="17",
        start=43044295,
        end=43125483,
        strand="+",
        locus_group="protein-coding",
        locus_type="gene with protein product",
        gene_group_id=None,
        data_source_id=1,
    )
    db_session.add(gene)
    db_session.commit()

    result = db_session.query(Gene).filter_by(hgnc_id="HGNC:1100").first()
    assert result is not None
    assert result.chromosome == "17"
    assert result.strand == "+"
    assert result.start < result.end


def test_unique_hgnc_id_constraint(db_session):
    gene1 = Gene(entity_id=1, hgnc_id="HGNC:9999")
    gene2 = Gene(entity_id=2, hgnc_id="HGNC:9999")  # Same HGNC ID

    db_session.add(gene1)
    db_session.commit()

    db_session.add(gene2)
    with pytest.raises(Exception):
        db_session.commit()


def test_gene_without_locus_data(db_session):
    gene = Gene(
        entity_id=3,
        hgnc_id="HGNC:3000",
        ensembl_id="ENSG000003000"
    )
    db_session.add(gene)
    db_session.commit()

    result = db_session.query(Gene).filter_by(entity_id=3).first()
    assert result is not None
    assert result.chromosome is None
    assert result.start is None
    assert result.end is None
