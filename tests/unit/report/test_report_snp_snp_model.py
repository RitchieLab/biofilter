from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.orm import sessionmaker

from biofilter.modules.db.base import Base
from biofilter.modules.db.models import (
    Entity,
    EntityAlias,
    EntityGroup,
    EntityLocation,
    EntityRelationship,
    EntityRelationshipType,
    GenomeAssembly,
)
from biofilter.modules.report.reports.report_snp_snp_model import SNPSNPModelReport


class DummyLogger:
    def log(self, message, level="INFO"):
        pass


def _report(session, **kwargs):
    return SNPSNPModelReport(
        session=session,
        db=SimpleNamespace(engine=getattr(session, "bind", None)),
        logger=DummyLogger(),
        **kwargs,
    )


def _create_variant_masters_table(engine):
    metadata = MetaData()
    vm = Table(
        "variant_masters",
        metadata,
        Column("variant_id", Integer, primary_key=True, autoincrement=True),
        Column("chromosome", Integer, nullable=False),
        Column("position_start", BigInteger, nullable=False),
        Column("position_end", BigInteger, nullable=False),
        Column("reference_allele", String(64), nullable=False),
        Column("alternate_allele", String(256), nullable=False),
        Column("rsid", String(32), nullable=True),
    )
    metadata.create_all(engine)
    return vm


def _make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    vm = _create_variant_masters_table(engine)
    Session = sessionmaker(bind=engine, future=True)
    return Session(), vm


def _seed(session):
    session.add(
        GenomeAssembly(
            id=1,
            accession="NC_000017.11",
            assembly_name="GRCh38.p14",
            chromosome="17",
        )
    )

    gene = EntityGroup(name="Gene")
    pathway = EntityGroup(name="Pathway")
    session.add_all([gene, pathway])
    session.flush()

    tp53 = Entity(group_id=gene.id, is_active=True)
    brca1 = Entity(group_id=gene.id, is_active=True)
    pten = Entity(group_id=gene.id, is_active=True)
    dna_repair = Entity(group_id=pathway.id, is_active=True)
    session.add_all([tp53, brca1, pten, dna_repair])
    session.flush()

    session.add_all(
        [
            EntityAlias(
                entity_id=tp53.id,
                group_id=gene.id,
                alias_value="TP53",
                alias_norm="tp53",
                alias_type="preferred",
                is_primary=True,
            ),
            EntityAlias(
                entity_id=brca1.id,
                group_id=gene.id,
                alias_value="BRCA1",
                alias_norm="brca1",
                alias_type="preferred",
                is_primary=True,
            ),
            EntityAlias(
                entity_id=pten.id,
                group_id=gene.id,
                alias_value="PTEN",
                alias_norm="pten",
                alias_type="preferred",
                is_primary=True,
            ),
            EntityAlias(
                entity_id=dna_repair.id,
                group_id=pathway.id,
                alias_value="DNA_REPAIR_PATHWAY",
                alias_norm="dna_repair_pathway",
                alias_type="preferred",
                is_primary=True,
            ),
        ]
    )

    session.add_all(
        [
            EntityLocation(
                entity_id=tp53.id,
                entity_group_id=gene.id,
                assembly_id=1,
                build=38,
                chromosome=17,
                start_pos=100,
                end_pos=200,
            ),
            EntityLocation(
                entity_id=brca1.id,
                entity_group_id=gene.id,
                assembly_id=1,
                build=38,
                chromosome=17,
                start_pos=260,
                end_pos=320,
            ),
            EntityLocation(
                entity_id=pten.id,
                entity_group_id=gene.id,
                assembly_id=1,
                build=38,
                chromosome=17,
                start_pos=360,
                end_pos=430,
            ),
        ]
    )

    in_pathway = EntityRelationshipType(code="in_pathway", description="in pathway")
    session.add(in_pathway)
    session.flush()

    session.add_all(
        [
            EntityRelationship(
                entity_1_id=tp53.id,
                entity_1_group_id=gene.id,
                entity_2_id=dna_repair.id,
                entity_2_group_id=pathway.id,
                relationship_type_id=in_pathway.id,
            ),
            EntityRelationship(
                entity_1_id=brca1.id,
                entity_1_group_id=gene.id,
                entity_2_id=dna_repair.id,
                entity_2_group_id=pathway.id,
                relationship_type_id=in_pathway.id,
            ),
            EntityRelationship(
                entity_1_id=pten.id,
                entity_1_group_id=gene.id,
                entity_2_id=dna_repair.id,
                entity_2_group_id=pathway.id,
                relationship_type_id=in_pathway.id,
            ),
        ]
    )
    session.commit()


def _seed_variants(session, vm):
    session.execute(
        vm.insert(),
        [
            {
                "variant_id": 1,
                "chromosome": 17,
                "position_start": 150,
                "position_end": 150,
                "reference_allele": "A",
                "alternate_allele": "G",
                "rsid": "rs111",
            },
            {
                "variant_id": 2,
                "chromosome": 17,
                "position_start": 280,
                "position_end": 280,
                "reference_allele": "C",
                "alternate_allele": "T",
                "rsid": "rs222",
            },
            {
                "variant_id": 3,
                "chromosome": 17,
                "position_start": 380,
                "position_end": 380,
                "reference_allele": "G",
                "alternate_allele": "A",
                "rsid": "rs333",
            },
        ],
    )
    session.commit()


def test_default_pipeline_builds_gene_and_snp_pairs():
    session, vm = _make_session()
    with session:
        _seed(session)
        _seed_variants(session, vm)

        report = _report(
            session,
            input_data=["chr17:150"],
            group_entity_groups=["Pathway"],
            relationship_types=["in_pathway"],
        )
        df = report.run()

    gene_rows = df[df["row_type"] == "gene_pair"]
    snp_rows = df[df["row_type"] == "snp_pair"]

    assert len(gene_rows) == 2
    assert set(gene_rows["gene_1_name"].tolist()) == {"TP53"}
    assert set(gene_rows["gene_2_name"].tolist()) == {"BRCA1", "PTEN"}
    assert set(gene_rows["gene_pair_seed_scope"].tolist()) == {"one_from_seed"}

    assert len(snp_rows) == 2
    rs_pairs = {
        tuple(sorted((row["variant_1_rsid"], row["variant_2_rsid"])))
        for _, row in snp_rows.iterrows()
    }
    assert rs_pairs == {("rs111", "rs222"), ("rs111", "rs333")}


def test_scope_both_from_seed_keeps_only_seed_seed_pairs():
    session, vm = _make_session()
    with session:
        _seed(session)
        _seed_variants(session, vm)

        report = _report(
            session,
            input_data=["chr17:150", "chr17:280"],
            group_entity_groups=["Pathway"],
            relationship_types=["in_pathway"],
            gene_pair_scope="both_from_seed",
            snp_pair_scope="both_from_seed",
        )
        df = report.run()

    gene_rows = df[df["row_type"] == "gene_pair"]
    snp_rows = df[df["row_type"] == "snp_pair"]

    assert len(gene_rows) == 1
    grow = gene_rows.iloc[0]
    assert {grow["gene_1_name"], grow["gene_2_name"]} == {"TP53", "BRCA1"}
    assert grow["gene_pair_seed_scope"] == "both_from_seed"

    assert len(snp_rows) == 1
    srow = snp_rows.iloc[0]
    assert {srow["variant_1_rsid"], srow["variant_2_rsid"]} == {"rs111", "rs222"}
    assert srow["snp_pair_seed_scope"] == "both_from_seed"


def test_disable_variant_expansion_for_expanded_genes_removes_snp_pairs():
    session, vm = _make_session()
    with session:
        _seed(session)
        _seed_variants(session, vm)

        report = _report(
            session,
            input_data=["chr17:150"],
            group_entity_groups=["Pathway"],
            relationship_types=["in_pathway"],
            expand_variants_from_expanded_genes=False,
        )
        df = report.run()

    gene_rows = df[df["row_type"] == "gene_pair"]
    snp_rows = df[df["row_type"] == "snp_pair"]

    assert len(gene_rows) == 2
    assert len(snp_rows) == 0


def test_invalid_and_not_found_inputs_are_reported():
    session, vm = _make_session()
    with session:
        _seed(session)
        _seed_variants(session, vm)

        report = _report(
            session,
            input_data=["chr17:99999", "bad_input"],
        )
        df = report.run()

    input_rows = df[df["row_type"] == "input"]
    observations = set(input_rows["observation"].tolist())
    assert "invalid_input" in observations
    assert "not_found" in observations
