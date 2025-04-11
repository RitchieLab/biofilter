import pytest
from sqlalchemy.exc import IntegrityError
from biofilter.db.models.omics_models import (
    Gene,
    GeneGroup,
    LocusGroup,
    LocusType,
    GeneGroupMembership,
    GeneLocation,
    GenomicRegion,
)


def test_create_gene_basic(db_session):
    gene = Gene(entity_id=1, hgnc_id="HGNC:1")
    db_session.add(gene)
    db_session.commit()

    assert gene.id is not None
    assert gene.hgnc_id == "HGNC:1"


def test_gene_with_locus_group_and_type(db_session):
    locus_group = LocusGroup(name="protein-coding gene")
    locus_type = LocusType(name="gene with protein product")

    gene = Gene(
        entity_id=1, hgnc_id="HGNC:2", locus_group=locus_group, locus_type=locus_type
    )  # noqa E501
    db_session.add(gene)
    db_session.commit()

    assert gene.locus_group.name == "protein-coding gene"
    assert gene.locus_type.name == "gene with protein product"


def test_gene_group_membership(db_session):
    group = GeneGroup(name="RNA-binding")
    gene = Gene(entity_id=1, hgnc_id="HGNC:3")
    group.genes.append(gene)

    db_session.commit()

    assert gene.groups[0].name == "RNA-binding"
    assert group.genes[0].hgnc_id == "HGNC:3"


def test_gene_group_membership_direct(db_session):
    group = GeneGroup(name="Immunoglobulin Family")
    gene = Gene(entity_id=1, hgnc_id="HGNC:6")
    db_session.add_all([group, gene])
    db_session.commit()

    # Cria explicitamente o vínculo via GeneGroupMembership
    link = GeneGroupMembership(gene_id=gene.id, group_id=group.id)
    db_session.add(link)
    db_session.commit()

    # Confirma existência do vínculo
    result = (
        db_session.query(GeneGroupMembership)
        .filter_by(gene_id=gene.id, group_id=group.id)
        .first()
    )
    assert result is not None


def test_gene_group_membership_uniqueness(db_session):
    group = GeneGroup(name="Transporters")
    gene = Gene(entity_id=1, hgnc_id="HGNC:7")
    db_session.add_all([group, gene])
    db_session.commit()

    link1 = GeneGroupMembership(gene_id=gene.id, group_id=group.id)
    link2 = GeneGroupMembership(gene_id=gene.id, group_id=group.id)

    db_session.add(link1)
    db_session.commit()

    db_session.add(link2)
    with pytest.raises(IntegrityError):
        db_session.commit()


def test_gene_location_with_region(db_session):
    region = GenomicRegion(
        label="12p13.31", chromosome="12", start=1000, end=5000
    )  # noqa E501
    gene = Gene(entity_id=1, hgnc_id="HGNC:4")

    location = GeneLocation(
        gene=gene, chromosome="12", start=1200, end=1300, strand="+", region=region
    )

    db_session.add(location)
    db_session.commit()

    assert location.region.label == "12p13.31"
    assert location.gene.hgnc_id == "HGNC:4"


def test_gene_hgnc_id_uniqueness(db_session):
    gene1 = Gene(entity_id=1, hgnc_id="HGNC:5")
    gene2 = Gene(entity_id=2, hgnc_id="HGNC:5")  # mesma HGNC

    db_session.add(gene1)
    db_session.commit()

    db_session.add(gene2)
    with pytest.raises(IntegrityError):
        db_session.commit()
