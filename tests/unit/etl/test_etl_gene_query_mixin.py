import pytest
import pandas as pd
from biofilter.etl.mixins.gene_query_mixin import GeneQueryMixin
from biofilter.db.models.omics_models import (
    LocusGroup,
    LocusType,
    GenomicRegion,
    Gene,
    GeneLocation,
)  # noqa: E501


class DummyLogger:
    def log(self, msg, level="INFO"):
        print(f"[{level}] {msg}")


class GeneQueryTester(GeneQueryMixin):
    def __init__(self, session):
        self.session = session
        self.logger = DummyLogger()


class DummyGeneMixin(GeneQueryMixin):
    def __init__(self):
        pass


def test_get_or_create_locus_group(db_session):
    mixin = GeneQueryTester(db_session)

    # Test creation
    group = mixin.get_or_create_locus_group("TestGroup")
    assert isinstance(group, LocusGroup)
    assert group.name == "TestGroup"

    # Test retrieval
    same_group = mixin.get_or_create_locus_group("TestGroup")
    assert same_group.id == group.id

    # Test with empty string
    assert mixin.get_or_create_locus_group("") is None

    # Test with None
    assert mixin.get_or_create_locus_group(None) is None


def test_get_or_create_locus_type(db_session):
    mixin = GeneQueryTester(db_session)

    # Test creation
    locus = mixin.get_or_create_locus_type("protein-coding")
    assert isinstance(locus, LocusType)
    assert locus.name == "protein-coding"

    # Test retrieval
    same_locus = mixin.get_or_create_locus_type("protein-coding")
    assert same_locus.id == locus.id

    # Test with empty string
    assert mixin.get_or_create_locus_type("") is None

    # Test with None
    assert mixin.get_or_create_locus_type(None) is None


def test_get_or_create_genomic_region(db_session):
    mixin = GeneQueryTester(db_session)

    # Test creation
    region = mixin.get_or_create_genomic_region(
        label="12q13.2",
        chromosome="12",
        start=56370000,
        end=56420000,
        description="test region",
    )  # noqa: E501
    assert isinstance(region, GenomicRegion)
    assert region.label == "12q13.2"
    assert region.chromosome == "12"
    assert region.start == 56370000
    assert region.end == 56420000
    assert region.description == "test region"

    # Test retrieval
    same_region = mixin.get_or_create_genomic_region(label="12q13.2")
    assert same_region.id == region.id

    # Test empty label
    assert mixin.get_or_create_genomic_region(label="") is None

    # Test None label
    assert mixin.get_or_create_genomic_region(label=None) is None


def test_create_gene_location(db_session):
    mixin = GeneQueryTester(db_session)

    # Gene com entity_id fictício
    gene = Gene(entity_id=999)
    db_session.add(gene)
    db_session.commit()

    # GenomicRegion dummy
    region = GenomicRegion(label="TestRegion")
    db_session.add(region)
    db_session.commit()

    # Create a location test
    location = mixin.create_gene_location(
        gene=gene,
        chromosome="17",
        start=43044295,
        end=43125483,
        strand="+",
        region=region,
        assembly="GRCh38",
        data_source_id=1,
    )

    assert isinstance(location, GeneLocation)
    assert location.gene_id == gene.id
    assert location.chromosome == "17"
    assert location.start == 43044295
    assert location.end == 43125483
    assert location.strand == "+"
    assert location.region_id == region.id
    assert location.assembly == "GRCh38"
    assert location.data_source_id == 1

    result = mixin.create_gene_location(gene=None)
    assert result is None


@pytest.mark.parametrize(
    "input_value,expected_output",
    [
        ("1:1000-2000", "1"),
        ("X:100-200", "X"),
        ("MT:400-500", "MT"),
        ("Y:1000", "Y"),
        ("chr12:300-500", None),
        ("abc:100-200", None),
        ("", None),
        (None, None),
        (pd.NA, None),
    ],
)
def test_extract_chromosome(input_value, expected_output):
    mixin = DummyGeneMixin()
    result = mixin.extract_chromosome(input_value)
    assert result == expected_output
