from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Float,
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
    ETLDataSource,
    ETLSourceSystem,
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
        Column("allele_type", String(20), nullable=True),
        Column("rsid", String(32), nullable=True),
        Column("an", BigInteger, nullable=True),
        Column("grpmax", String(32), nullable=True),
        Column("cadd_raw_score", Float, nullable=True),
        Column("cadd_phred", Float, nullable=True),
    )
    metadata.create_all(engine)
    return vm


def _create_variant_annotation_tables(engine):
    metadata = MetaData()
    vcg = Table(
        "variant_consequence_groups",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("name", String(64), nullable=False),
    )
    vcc = Table(
        "variant_consequence_categories",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("name", String(64), nullable=False),
    )
    vc = Table(
        "variant_consequences",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("name", String(64), nullable=False),
        Column("severity_rank", Integer, nullable=False),
        Column("consequence_group_id", Integer, nullable=True),
        Column("consequence_category_id", Integer, nullable=True),
        Column("is_active", Boolean, nullable=False, default=True),
    )
    vme = Table(
        "variant_molecular_effects",
        metadata,
        Column("chromosome", Integer, nullable=False),
        Column("variant_id", Integer, nullable=False),
        Column("transcript_id", String(32), nullable=False),
        Column("consequence_id", Integer, nullable=False),
        Column("lof_confidence", String(8), nullable=True),
    )
    vep = Table(
        "variant_effect_predictions",
        metadata,
        Column("chromosome", Integer, nullable=False),
        Column("variant_id", Integer, nullable=False),
        Column("predictor_key", String(128), nullable=False),
        Column("predictor_name", String(64), nullable=False),
        Column("score", Float, nullable=True),
        Column("classification", String(64), nullable=True),
    )
    metadata.create_all(engine)
    return {"vcg": vcg, "vcc": vcc, "vc": vc, "vme": vme, "vep": vep}


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
    interacts_with = EntityRelationshipType(
        code="interacts_with",
        description="direct interaction",
    )
    source_system = ETLSourceSystem(name="UNIT_TEST_SOURCE", active=True)
    session.add(source_system)
    session.flush()

    reactome_ds = ETLDataSource(
        name="Reactome",
        source_system_id=source_system.id,
        data_type="Pathway",
        format="TSV",
        dtp_script="tests/reactome.py",
        active=True,
    )
    kegg_ds = ETLDataSource(
        name="KEGG",
        source_system_id=source_system.id,
        data_type="Pathway",
        format="TSV",
        dtp_script="tests/kegg.py",
        active=True,
    )
    session.add_all([in_pathway, interacts_with, reactome_ds, kegg_ds])
    session.flush()

    session.add_all(
        [
            EntityRelationship(
                entity_1_id=tp53.id,
                entity_1_group_id=gene.id,
                entity_2_id=dna_repair.id,
                entity_2_group_id=pathway.id,
                relationship_type_id=in_pathway.id,
                data_source_id=reactome_ds.id,
            ),
            EntityRelationship(
                entity_1_id=brca1.id,
                entity_1_group_id=gene.id,
                entity_2_id=dna_repair.id,
                entity_2_group_id=pathway.id,
                relationship_type_id=in_pathway.id,
                data_source_id=reactome_ds.id,
            ),
            EntityRelationship(
                entity_1_id=pten.id,
                entity_1_group_id=gene.id,
                entity_2_id=dna_repair.id,
                entity_2_group_id=pathway.id,
                relationship_type_id=in_pathway.id,
                data_source_id=kegg_ds.id,
            ),
            # Direct gene-gene edge for Direct Gene mode
            EntityRelationship(
                entity_1_id=tp53.id,
                entity_1_group_id=gene.id,
                entity_2_id=brca1.id,
                entity_2_group_id=gene.id,
                relationship_type_id=interacts_with.id,
                data_source_id=kegg_ds.id,
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
                "allele_type": "snv",
                "rsid": "rs111",
                "an": 100,
                "grpmax": "nfe",
                "cadd_raw_score": 2.1,
                "cadd_phred": 15.2,
            },
            {
                "variant_id": 2,
                "chromosome": 17,
                "position_start": 280,
                "position_end": 280,
                "reference_allele": "C",
                "alternate_allele": "T",
                "allele_type": "snv",
                "rsid": "rs222",
                "an": 120,
                "grpmax": "afr",
                "cadd_raw_score": 0.5,
                "cadd_phred": 7.0,
            },
            {
                "variant_id": 3,
                "chromosome": 17,
                "position_start": 380,
                "position_end": 380,
                "reference_allele": "G",
                "alternate_allele": "A",
                "allele_type": "snv",
                "rsid": "rs333",
                "an": None,
                "grpmax": None,
                "cadd_raw_score": None,
                "cadd_phred": None,
            },
            # Same logical variant (same rsid) with another ALT allele.
            # Report should keep only one row after variant dedupe.
            {
                "variant_id": 4,
                "chromosome": 17,
                "position_start": 280,
                "position_end": 280,
                "reference_allele": "C",
                "alternate_allele": "G",
                "allele_type": "snv",
                "rsid": "rs222",
                "an": None,
                "grpmax": None,
                "cadd_raw_score": None,
                "cadd_phred": None,
            },
            # Non-SNV rows must be ignored by the report.
            {
                "variant_id": 5,
                "chromosome": 17,
                "position_start": 150,
                "position_end": 150,
                "reference_allele": "A",
                "alternate_allele": "AG",
                "allele_type": "ins",
                "rsid": "rsINS150",
                "an": None,
                "grpmax": None,
                "cadd_raw_score": None,
                "cadd_phred": None,
            },
            {
                "variant_id": 6,
                "chromosome": 17,
                "position_start": 155,
                "position_end": 155,
                "reference_allele": "A",
                "alternate_allele": "AT",
                "allele_type": "ins",
                "rsid": "rsINS155",
                "an": None,
                "grpmax": None,
                "cadd_raw_score": None,
                "cadd_phred": None,
            },
        ],
    )
    session.commit()


def _seed_variant_annotations(session):
    tables = _create_variant_annotation_tables(getattr(session, "bind", None))

    session.execute(
        tables["vcg"].insert(),
        [
            {"id": 1, "name": "Coding"},
            {"id": 2, "name": "Regulatory"},
        ],
    )
    session.execute(
        tables["vcc"].insert(),
        [
            {"id": 1, "name": "Protein altering"},
            {"id": 2, "name": "Non-coding"},
        ],
    )
    session.execute(
        tables["vc"].insert(),
        [
            {
                "id": 10,
                "name": "missense_variant",
                "severity_rank": 4,
                "consequence_group_id": 1,
                "consequence_category_id": 1,
                "is_active": True,
            },
            {
                "id": 20,
                "name": "intron_variant",
                "severity_rank": 20,
                "consequence_group_id": 2,
                "consequence_category_id": 2,
                "is_active": True,
            },
        ],
    )
    session.execute(
        tables["vme"].insert(),
        [
            {
                "chromosome": 17,
                "variant_id": 1,
                "transcript_id": "ENST000001",
                "consequence_id": 10,
                "lof_confidence": "HC",
            },
            {
                "chromosome": 17,
                "variant_id": 2,
                "transcript_id": "ENST000002",
                "consequence_id": 20,
                "lof_confidence": "LC",
            },
        ],
    )
    session.execute(
        tables["vep"].insert(),
        [
            {
                "chromosome": 17,
                "variant_id": 1,
                "predictor_key": "alphamissense:v1:-",
                "predictor_name": "alphamissense",
                "score": 0.91,
                "classification": "likely_pathogenic",
            },
            {
                "chromosome": 17,
                "variant_id": 2,
                "predictor_key": "cadd:v1:-",
                "predictor_name": "cadd",
                "score": 7.0,
                "classification": "moderate",
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
    assert 5 not in set(snp_rows["variant_1_id"].tolist())
    assert 5 not in set(snp_rows["variant_2_id"].tolist())


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


def test_non_snv_seed_position_is_reported_as_not_found():
    session, vm = _make_session()
    with session:
        _seed(session)
        _seed_variants(session, vm)

        report = _report(
            session,
            input_data=["chr17:155"],
        )
        df = report.run()

    input_rows = df[df["row_type"] == "input"]
    assert len(input_rows) >= 1
    assert "not_found" in set(input_rows["observation"].tolist())


def test_direct_gene_mode_builds_pairs_without_intermediate_group_entity():
    session, vm = _make_session()
    with session:
        _seed(session)
        _seed_variants(session, vm)

        report = _report(
            session,
            input_data=["chr17:150", "chr17:280"],
            group_entity_groups=["Direct Gene"],
            gene_pair_scope="both_from_seed",
            snp_pair_scope="both_from_seed",
        )
        df = report.run()

    gene_rows = df[df["row_type"] == "gene_pair"]
    snp_rows = df[df["row_type"] == "snp_pair"]

    assert len(gene_rows) == 1
    assert set(gene_rows["group_support_names"].tolist()) == {"Direct Gene"}
    assert len(snp_rows) == 1
    assert set(snp_rows["group_support_names"].tolist()) == {"Direct Gene"}


def test_invalid_group_entity_groups_returns_helpful_error():
    session, vm = _make_session()
    with session:
        _seed(session)
        _seed_variants(session, vm)

        report = _report(
            session,
            input_data=["chr17:150"],
            group_entity_groups=["NOT_A_GROUP_TYPE"],
        )
        with pytest.raises(ValueError, match="group_entity_groups"):
            report.run()


def test_group_data_sources_filters_grouping_step_and_emits_ds_support():
    session, vm = _make_session()
    with session:
        _seed(session)
        _seed_variants(session, vm)

        report = _report(
            session,
            input_data=["chr17:150"],
            group_entity_groups=["Pathway"],
            relationship_types=["in_pathway"],
            group_data_sources=["Reactome"],
        )
        df = report.run()

    gene_rows = df[df["row_type"] == "gene_pair"]
    snp_rows = df[df["row_type"] == "snp_pair"]

    assert len(gene_rows) == 1
    grow = gene_rows.iloc[0]
    assert {grow["gene_1_name"], grow["gene_2_name"]} == {"TP53", "BRCA1"}
    assert grow["data_source_support_names"] == "Reactome"
    assert int(grow["data_source_support_count"]) == 1

    assert len(snp_rows) == 1
    srow = snp_rows.iloc[0]
    assert {srow["variant_1_rsid"], srow["variant_2_rsid"]} == {"rs111", "rs222"}
    assert srow["data_source_support_names"] == "Reactome"


# Return old version without extended fields
# def test_snp_pair_rows_include_variant_master_effect_and_prediction_fields():
#     session, vm = _make_session()
#     with session:
#         _seed(session)
#         _seed_variants(session, vm)
#         _seed_variant_annotations(session)

#         report = _report(
#             session,
#             input_data=["chr17:150", "chr17:280"],
#             group_entity_groups=["Pathway"],
#             relationship_types=["in_pathway"],
#             gene_pair_scope="both_from_seed",
#             snp_pair_scope="both_from_seed",
#         )
#         df = report.run()

#     snp_rows = df[df["row_type"] == "snp_pair"]
#     assert len(snp_rows) == 1
#     srow = snp_rows.iloc[0]

#     by_rsid = {
#         srow["variant_1_rsid"]: {
#             "an": srow["variant_1_an"],
#             "grpmax": srow["variant_1_grpmax"],
#             "cadd_raw_score": srow["variant_1_cadd_raw_score"],
#             "cadd_phred": srow["variant_1_cadd_phred"],
#             "consequence_ids": srow["variant_1_consequence_ids"],
#             "consequence_names": srow["variant_1_consequence_names"],
#             "consequence_groups": srow["variant_1_consequence_groups"],
#             "consequence_categories": srow["variant_1_consequence_categories"],
#             "lof_confidences": srow["variant_1_lof_confidences"],
#             "predictor_names": srow["variant_1_predictor_names"],
#             "prediction_scores": srow["variant_1_prediction_scores"],
#             "prediction_classifications": srow[
#                 "variant_1_prediction_classifications"
#             ],
#         },
#         srow["variant_2_rsid"]: {
#             "an": srow["variant_2_an"],
#             "grpmax": srow["variant_2_grpmax"],
#             "cadd_raw_score": srow["variant_2_cadd_raw_score"],
#             "cadd_phred": srow["variant_2_cadd_phred"],
#             "consequence_ids": srow["variant_2_consequence_ids"],
#             "consequence_names": srow["variant_2_consequence_names"],
#             "consequence_groups": srow["variant_2_consequence_groups"],
#             "consequence_categories": srow["variant_2_consequence_categories"],
#             "lof_confidences": srow["variant_2_lof_confidences"],
#             "predictor_names": srow["variant_2_predictor_names"],
#             "prediction_scores": srow["variant_2_prediction_scores"],
#             "prediction_classifications": srow[
#                 "variant_2_prediction_classifications"
#             ],
#         },
#     }

#     assert by_rsid["rs111"]["an"] == 100
#     assert by_rsid["rs111"]["grpmax"] == "nfe"
#     assert by_rsid["rs111"]["cadd_raw_score"] == 2.1
#     assert by_rsid["rs111"]["cadd_phred"] == 15.2
#     assert by_rsid["rs111"]["consequence_ids"] == "10"
#     assert by_rsid["rs111"]["consequence_names"] == "missense_variant"
#     assert by_rsid["rs111"]["consequence_groups"] == "Coding"
#     assert by_rsid["rs111"]["consequence_categories"] == "Protein altering"
#     assert by_rsid["rs111"]["lof_confidences"] == "HC"
#     assert by_rsid["rs111"]["predictor_names"] == "alphamissense"
#     assert by_rsid["rs111"]["prediction_scores"] == "0.91"
#     assert by_rsid["rs111"]["prediction_classifications"] == "likely_pathogenic"

#     assert by_rsid["rs222"]["an"] == 120
#     assert by_rsid["rs222"]["grpmax"] == "afr"
#     assert by_rsid["rs222"]["cadd_raw_score"] == 0.5
#     assert by_rsid["rs222"]["cadd_phred"] == 7.0
#     assert by_rsid["rs222"]["consequence_ids"] == "20"
#     assert by_rsid["rs222"]["consequence_names"] == "intron_variant"
#     assert by_rsid["rs222"]["consequence_groups"] == "Regulatory"
#     assert by_rsid["rs222"]["consequence_categories"] == "Non-coding"
#     assert by_rsid["rs222"]["lof_confidences"] == "LC"
#     assert by_rsid["rs222"]["predictor_names"] == "cadd"
#     assert by_rsid["rs222"]["prediction_scores"] == "7"
#     assert by_rsid["rs222"]["prediction_classifications"] == "moderate"
