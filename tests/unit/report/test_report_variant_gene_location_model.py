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
    GenomeAssembly,
)
from biofilter.modules.report.reports.report_variant_gene_location_model import (
    VariantGeneLocationModelReport,
)


class DummyLogger:
    def log(self, message, level="INFO"):
        pass


def _report(session, **kwargs):
    return VariantGeneLocationModelReport(
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
    session.add(gene)
    session.flush()

    tp53 = Entity(group_id=gene.id, is_active=True)
    brca1 = Entity(group_id=gene.id, is_active=True)
    session.add_all([tp53, brca1])
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
        ]
    )

    # build=38, chromosome encoding as integer
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
                "position_start": 500,
                "position_end": 500,
                "reference_allele": "G",
                "alternate_allele": "A",
                "rsid": "rs333",
            },
        ],
    )
    session.commit()


def _make_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    vm = _create_variant_masters_table(engine)
    Session = sessionmaker(bind=engine, future=True)
    return Session(), vm


def test_gene_input_returns_overlapping_variants():
    session, vm = _make_session()
    with session:
        _seed(session)
        _seed_variants(session, vm)
        report = _report(
            session,
            input_mode="gene",
            input_data=["TP53"],
        )
        df = report.run()

    rows = df[df["observation"] == "ok"]
    assert len(rows) == 1
    row = rows.iloc[0]
    assert row["input_primary_name"] == "TP53"
    assert row["variant_rsid"] == "rs111"
    assert row["gene_primary_name"] == "TP53"
    assert row["overlap_bp"] == 1


def test_rsid_input_maps_variant_to_gene():
    session, vm = _make_session()
    with session:
        _seed(session)
        _seed_variants(session, vm)
        report = _report(
            session,
            input_mode="rsid",
            input_data=["rs222"],
        )
        df = report.run()

    rows = df[df["observation"] == "ok"]
    assert len(rows) == 1
    row = rows.iloc[0]
    assert row["variant_rsid"] == "rs222"
    assert row["gene_primary_name"] == "BRCA1"


def test_auto_mode_supports_position_and_region_inputs():
    session, vm = _make_session()
    with session:
        _seed(session)
        _seed_variants(session, vm)
        report = _report(
            session,
            input_mode="auto",
            input_data=["chr17:150", "chr17:260-320"],
        )
        df = report.run()

    ok_rows = df[df["observation"] == "ok"]
    assert len(ok_rows) >= 2
    assert set(ok_rows["variant_rsid"].tolist()) >= {"rs111", "rs222"}
    assert set(ok_rows["gene_primary_name"].tolist()) >= {"TP53", "BRCA1"}


def test_not_found_and_invalid_inputs_are_reported():
    session, vm = _make_session()
    with session:
        _seed(session)
        _seed_variants(session, vm)
        report = _report(
            session,
            input_mode="auto",
            input_data=["NOT_A_GENE", "chr17:XYZ", "rs999999999"],
        )
        df = report.run()

    observations = set(df["observation"].tolist())
    assert "not found" in observations
    assert "invalid_input" in observations


def test_gene_input_reports_when_chromosome_has_no_variant_rows():
    session, vm = _make_session()
    with session:
        _seed(session)
        _seed_variants(session, vm)

        gene_group = session.query(EntityGroup).filter_by(name="Gene").one()
        session.add(
            GenomeAssembly(
                id=2,
                accession="NC_000022.11",
                assembly_name="GRCh38.p14",
                chromosome="22",
            )
        )
        session.flush()

        gene_no_variants = Entity(group_id=gene_group.id, is_active=True)
        session.add(gene_no_variants)
        session.flush()
        session.add(
            EntityAlias(
                entity_id=gene_no_variants.id,
                group_id=gene_group.id,
                alias_value="NO_VAR_GENE",
                alias_norm="no_var_gene",
                alias_type="preferred",
                is_primary=True,
            )
        )
        session.add(
            EntityLocation(
                entity_id=gene_no_variants.id,
                entity_group_id=gene_group.id,
                assembly_id=2,
                build=38,
                chromosome=22,
                start_pos=100,
                end_pos=200,
            )
        )
        session.commit()

        report = _report(
            session,
            input_mode="gene",
            input_data=["NO_VAR_GENE"],
        )
        df = report.run()

    row = df.iloc[0]
    assert row["observation"] == "not found"
    assert row["input_primary_name"] == "NO_VAR_GENE"
    assert "variant_masters has no rows for chromosome chr22" in str(row["note"])
