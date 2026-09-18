"""
A bundle small enough to commit, shaped like a real one.

Every report migrated to the native module tests against this. It stays
deliberately small — three genes and a handful of variants — but it
carries the structure that makes gene annotation interesting: a gene
with no build-38 location, an alias that resolves case-insensitively,
relationships pointing both ways, and a gene whose range contains
variants.

It also reproduces on purpose the trap that cost us 2.4 billion
invisible rows: a partitioned table with an empty same-named parent file
beside it. A reader that scans the directory picks the wrong one; a
reader that reads the manifest cannot.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# Entity ids, grouped by what they are.
#
# DGENE exists so the disease fixtures can add relationships without
# changing the counts the gene tests assert on TP53 and BRCA1.
TP53, BRCA1, NOLOC, DGENE = 1, 2, 3, 4
PROTEIN, PROTEIN_ISO = 10, 11
PATHWAY = 20
DISEASE, DISEASE_BARE = 30, 31
GO_PARENT, GO_CHILD = 40, 41

GROUP_GENES, GROUP_PROTEINS, GROUP_PATHWAYS = 1, 2, 3
GROUP_DISEASES, GROUP_GO = 4, 5

# Data sources, so provenance and the ClinGen-only summary have something
# to distinguish.
DS_HGNC, DS_UNIPROT, DS_REACTOME, DS_MONDO, DS_GO, DS_CLINGEN = 1, 2, 3, 4, 5, 6
DS_VARIANT = 7


def _t(**columns) -> pa.Table:
    return pa.table(columns)


def _arrow_type(sa_type) -> pa.DataType:
    """A usable Arrow type for a SQLAlchemy column, for padding nulls."""
    try:
        python_type = sa_type.python_type
    except NotImplementedError:
        return pa.string()
    return {
        bool: pa.bool_(),
        int: pa.int64(),
        float: pa.float64(),
    }.get(python_type, pa.string())


def _conform_to_models(name: str, table: pa.Table) -> pa.Table:
    """
    Pad a fixture table with the columns the models declare.

    The tables above spell out only what the tests care about. Everything
    else — provenance ids, timestamps, descriptions — is filled with
    nulls from the model definition, so the fixture has a real bundle's
    shape without every test fixture restating it.

    Three reports have failed against this fixture for a column the real
    tables carry and the fixture omitted. Padding from the contract is
    what stops the fourth.
    """
    from biofilter.modules.db.base import Base

    declared = Base.metadata.tables.get(name)
    if declared is None:
        return table

    for column in declared.columns:
        if column.name not in table.column_names:
            table = table.append_column(
                column.name, pa.nulls(table.num_rows, _arrow_type(column.type))
            )
    return table.select([c.name for c in declared.columns])


def _tables() -> dict[str, pa.Table]:
    """The core tables, as a bundle spells them."""
    return {
        "entity_groups": _t(
            id=pa.array(
                [GROUP_GENES, GROUP_PROTEINS, GROUP_PATHWAYS, GROUP_DISEASES, GROUP_GO],
                pa.int64(),
            ),
            name=pa.array(
                ["Genes", "Proteins", "Pathways", "Diseases", "Gene Ontology"]
            ),
            description=pa.array([None] * 5, pa.string()),
        ),
        "entities": _t(
            id=pa.array(
                [
                    TP53, BRCA1, NOLOC, DGENE,
                    PROTEIN, PROTEIN_ISO, PATHWAY,
                    DISEASE, DISEASE_BARE, GO_PARENT, GO_CHILD,
                ],
                pa.int64(),
            ),
            group_id=pa.array(
                [
                    GROUP_GENES, GROUP_GENES, GROUP_GENES, GROUP_GENES,
                    GROUP_PROTEINS, GROUP_PROTEINS, GROUP_PATHWAYS,
                    GROUP_DISEASES, GROUP_DISEASES, GROUP_GO, GROUP_GO,
                ],
                pa.int64(),
            ),
            has_conflict=pa.array([False] * 11),
            is_active=pa.array([True] * 11),
        ),
        "etl_source_systems": _t(
            id=pa.array(
                [DS_HGNC, DS_UNIPROT, DS_REACTOME, DS_MONDO, DS_GO, DS_CLINGEN,
                 DS_VARIANT],
                pa.int64(),
            ),
            name=pa.array(
                ["HGNC", "UniProt", "Reactome", "MONDO", "GO", "ClinGen", "gnomAD"]
            ),
            active=pa.array([True] * 7),
        ),
        "etl_data_sources": _t(
            id=pa.array(
                [DS_HGNC, DS_UNIPROT, DS_REACTOME, DS_MONDO, DS_GO, DS_CLINGEN,
                 DS_VARIANT],
                pa.int64(),
            ),
            name=pa.array(
                ["hgnc", "uniprot", "reactome", "mondo", "gene_ontology", "clingen",
                 "gnomad_test"]
            ),
            source_system_id=pa.array(
                [DS_HGNC, DS_UNIPROT, DS_REACTOME, DS_MONDO, DS_GO, DS_CLINGEN,
                 DS_VARIANT],
                pa.int64(),
            ),
            data_type=pa.array(
                ["Gene", "Protein", "Pathway", "Disease", "GO",
                 "Disease Relationships", "Variant"]
            ),
            active=pa.array([True] * 7),
        ),
        # One data source per pipeline state the status report reports.
        #
        #   hgnc           extract -> transform -> load, one hash carried
        #                  through            ........................ ok
        #   gnomad_test    extract -> transform, no load: the variant
        #                  branch writes parquet straight ........... ok
        #   uniprot        every stage ran, no hashes anywhere . unverifiable
        #   gene_ontology  transform ran on a different extract . misaligned
        #   reactome       core source with no load ............. incomplete
        #   mondo          no packages at all .................... never_run
        #   clingen        failed once, then succeeded ..... ok + an error
        "etl_packages": _t(
            id=pa.array(list(range(1, 15)), pa.int64()),
            data_source_id=pa.array(
                [
                    DS_HGNC, DS_HGNC, DS_HGNC,
                    DS_VARIANT, DS_VARIANT,
                    DS_UNIPROT, DS_UNIPROT, DS_UNIPROT,
                    DS_GO, DS_GO, DS_GO,
                    DS_REACTOME,
                    DS_CLINGEN, DS_CLINGEN,
                ],
                pa.int64(),
            ),
            operation_type=pa.array(
                [
                    "extract", "transform", "load",
                    "extract", "transform",
                    "extract", "transform", "load",
                    "extract", "transform", "load",
                    "extract",
                    "extract", "extract",
                ]
            ),
            status=pa.array(
                [
                    "completed", "completed", "completed",
                    "completed", "completed",
                    "completed", "completed", "completed",
                    "completed", "completed", "completed",
                    "completed",
                    "failed", "completed",
                ]
            ),
            extract_status=pa.array(
                [
                    "completed", None, None,
                    "up-to-date", None,
                    "completed", None, None,
                    "completed", None, None,
                    "completed",
                    "failed", "completed",
                ]
            ),
            transform_status=pa.array(
                [
                    None, "completed", None,
                    None, "completed",
                    None, "completed", None,
                    None, "completed", None,
                    None,
                    None, None,
                ]
            ),
            load_status=pa.array(
                [
                    None, None, "completed",
                    None, None,
                    None, None, "completed",
                    None, None, "completed",
                    None,
                    None, None,
                ]
            ),
            extract_hash=pa.array(
                [
                    "aaa", None, None,
                    "vvv", None,
                    None, None, None,
                    "ggg", None, None,
                    "rrr",
                    None, None,
                ]
            ),
            transform_hash=pa.array(
                [
                    None, "aaa", None,
                    None, "vvv",
                    None, None, None,
                    None, "OTHER", None,
                    None,
                    None, None,
                ]
            ),
            load_hash=pa.array(
                [
                    None, None, "aaa",
                    None, None,
                    None, None, None,
                    None, None, "OTHER",
                    None,
                    None, None,
                ]
            ),
        ),
        "omic_status": _t(
            id=pa.array([1], pa.int64()),
            name=pa.array(["active"]),
            description=pa.array([None], pa.string()),
        ),
        "gene_locus_groups": _t(
            id=pa.array([1], pa.int64()),
            name=pa.array(["protein-coding gene"]),
            description=pa.array([None], pa.string()),
        ),
        "gene_locus_types": _t(
            id=pa.array([1], pa.int64()),
            name=pa.array(["gene with protein product"]),
            description=pa.array([None], pa.string()),
        ),
        "gene_masters": _t(
            id=pa.array([101, 102, 103], pa.int64()),
            entity_id=pa.array([TP53, BRCA1, NOLOC], pa.int64()),
            symbol=pa.array(["TP53", "BRCA1", "NOLOC1"]),
            hgnc_status=pa.array(["Approved", "Approved", "Approved"]),
            omic_status_id=pa.array([1, 1, 1], pa.int64()),
            locus_group_id=pa.array([1, 1, 1], pa.int64()),
            locus_type_id=pa.array([1, 1, 1], pa.int64()),
            chromosome=pa.array([17, 17, 22], pa.int32()),
        ),
        "gene_groups": _t(
            id=pa.array([1, 2], pa.int64()),
            name=pa.array(["p53 family", "BRCA1 A complex"]),
            description=pa.array([None, None], pa.string()),
        ),
        "gene_group_memberships": _t(
            gene_id=pa.array([101, 102], pa.int64()),
            group_id=pa.array([1, 2], pa.int64()),
        ),
        "entity_aliases": _t(
            id=pa.array(list(range(1, 11)), pa.int64()),
            entity_id=pa.array(
                [TP53, TP53, TP53, TP53, TP53, BRCA1, BRCA1, BRCA1, NOLOC, NOLOC],
                pa.int64(),
            ),
            alias_value=pa.array(
                [
                    "TP53", "HGNC:11998", "ENSG00000141510", "7157", "p53",
                    "BRCA1", "HGNC:1100", "ENSG00000012048",
                    "NOLOC1", "no-location gene",
                ]
            ),
            alias_norm=pa.array(
                [
                    "tp53", "hgnc:11998", "ensg00000141510", "7157", "p53",
                    "brca1", "hgnc:1100", "ensg00000012048",
                    "noloc1", "no-location gene",
                ]
            ),
            alias_type=pa.array(
                [
                    "symbol", "code", "code", "code", "synonym",
                    "symbol", "code", "code",
                    "symbol", "name",
                ]
            ),
            xref_source=pa.array(
                [
                    "HGNC", "HGNC", "ENSEMBL", "ENTREZ", "HGNC",
                    "HGNC", "HGNC", "ENSEMBL",
                    "HGNC", "HGNC",
                ]
            ),
            is_primary=pa.array(
                [True, False, False, False, False,
                 True, False, False,
                 True, False]
            ),
            is_active=pa.array([True] * 10),
            data_source_id=pa.array([DS_HGNC] * 10, pa.int64()),
        ),
        # Aliases for everything that is not a gene, plus one name
        # deliberately shared by two entities so entity_filter has
        # something ambiguous to flag.
        "entity_aliases_extra": _t(
            id=pa.array(list(range(11, 24)), pa.int64()),
            entity_id=pa.array(
                [
                    NOLOC, DGENE, DGENE,
                    PROTEIN, PROTEIN, PROTEIN_ISO,
                    PATHWAY,
                    DISEASE, DISEASE, DISEASE_BARE,
                    GO_PARENT, GO_CHILD, GO_CHILD,
                ],
                pa.int64(),
            ),
            alias_value=pa.array(
                [
                    "AMBIGUOUS", "AMBIGUOUS", "DGENE1",
                    "P04637", "TP53_HUMAN", "P04637-2",
                    "R-HSA-0001",
                    "MONDO:0001", "breast cancer", "MONDO:0002",
                    "GO:0000001", "GO:0000002", "apoptosis",
                ]
            ),
            alias_norm=pa.array(
                [
                    "ambiguous", "ambiguous", "dgene1",
                    "p04637", "tp53_human", "p04637-2",
                    "r-hsa-0001",
                    "mondo:0001", "breast cancer", "mondo:0002",
                    "go:0000001", "go:0000002", "apoptosis",
                ]
            ),
            alias_type=pa.array(
                [
                    "synonym", "synonym", "symbol",
                    "code", "name", "code",
                    "code",
                    "code", "name", "code",
                    "code", "code", "synonym",
                ]
            ),
            xref_source=pa.array(
                [
                    "HGNC", "HGNC", "HGNC",
                    "UNIPROT", "UNIPROT", "UNIPROT",
                    "REACTOME",
                    "MONDO", "MONDO", "MONDO",
                    "GO", "GO", "GO",
                ]
            ),
            is_primary=pa.array(
                [
                    False, False, True,
                    True, False, True,
                    True,
                    True, False, True,
                    True, True, False,
                ]
            ),
            is_active=pa.array([True] * 13),
            data_source_id=pa.array(
                [
                    DS_HGNC, DS_HGNC, DS_HGNC,
                    DS_UNIPROT, DS_UNIPROT, DS_UNIPROT,
                    DS_REACTOME,
                    DS_MONDO, DS_MONDO, DS_MONDO,
                    DS_GO, DS_GO, DS_GO,
                ],
                pa.int64(),
            ),
        ),
        # NOLOC deliberately has none: it is the `partial` case.
        "entity_locations": _t(
            id=pa.array([1, 2], pa.int64()),
            entity_id=pa.array([TP53, BRCA1], pa.int64()),
            build=pa.array([38, 38], pa.int32()),
            chromosome=pa.array([17, 17], pa.int32()),
            start_pos=pa.array([100, 5000], pa.int64()),
            end_pos=pa.array([400, 5400], pa.int64()),
        ),
        # --- Diseases ------------------------------------------------
        "disease_masters": _t(
            id=pa.array([301, 302], pa.int64()),
            entity_id=pa.array([DISEASE, DISEASE_BARE], pa.int64()),
            disease_id=pa.array(["MONDO:0001", "MONDO:0002"]),
            label=pa.array(["breast cancer", "bare disease"]),
            description=pa.array(["a tumour of the breast", None]),
            omic_status_id=pa.array([1, 1], pa.int64()),
            data_source_id=pa.array([DS_MONDO, DS_MONDO], pa.int64()),
            etl_package_id=pa.array([71, 71], pa.int64()),
        ),
        "disease_groups": _t(
            id=pa.array([1], pa.int64()),
            name=pa.array(["neoplasm"]),
            description=pa.array([None], pa.string()),
        ),
        "disease_group_memberships": _t(
            id=pa.array([1], pa.int64()),
            disease_id=pa.array([301], pa.int64()),
            group_id=pa.array([1], pa.int64()),
        ),
        # --- Gene Ontology -------------------------------------------
        # GO_PARENT is_a-parent of GO_CHILD, so one side of each pair has
        # a parent and the other a child.
        "go_masters": _t(
            id=pa.array([401, 402], pa.int64()),
            entity_id=pa.array([GO_PARENT, GO_CHILD], pa.int64()),
            go_id=pa.array(["GO:0000001", "GO:0000002"]),
            name=pa.array(["cell death", "apoptotic process"]),
            namespace=pa.array(["biological_process", "biological_process"]),
            data_source_id=pa.array([DS_GO, DS_GO], pa.int64()),
            etl_package_id=pa.array([72, 72], pa.int64()),
        ),
        "go_relations": _t(
            id=pa.array([1], pa.int64()),
            parent_id=pa.array([401], pa.int64()),
            child_id=pa.array([402], pa.int64()),
            relation_type=pa.array(["is_a"]),
        ),
        # --- Pathways -------------------------------------------------
        "pathway_masters": _t(
            id=pa.array([201], pa.int64()),
            entity_id=pa.array([PATHWAY], pa.int64()),
            pathway_id=pa.array(["R-HSA-0001"]),
            description=pa.array(["apoptosis signalling"]),
            data_source_id=pa.array([DS_REACTOME], pa.int64()),
            etl_package_id=pa.array([73], pa.int64()),
        ),
        # --- Proteins -------------------------------------------------
        # One protein, one isoform of it. The isoform has no relationships
        # of its own — following to the canonical entity is what makes an
        # isoform input report anything at all.
        "protein_masters": _t(
            id=pa.array([101], pa.int64()),
            protein_id=pa.array(["P04637"]),
            function=pa.array(["tumour suppressor"]),
            location=pa.array(["nucleus"]),
            tissue_expression=pa.array(["ubiquitous"]),
            pseudogene_note=pa.array([None], pa.string()),
            data_source_id=pa.array([DS_UNIPROT], pa.int64()),
            etl_package_id=pa.array([74], pa.int64()),
        ),
        "protein_entities": _t(
            id=pa.array([1, 2], pa.int64()),
            entity_id=pa.array([PROTEIN, PROTEIN_ISO], pa.int64()),
            protein_id=pa.array([101, 101], pa.int64()),
            is_isoform=pa.array([False, True]),
            isoform_accession=pa.array([None, "P04637-2"]),
        ),
        "protein_pfams": _t(
            id=pa.array([1, 2], pa.int64()),
            pfam_acc=pa.array(["PF00870", "PF08563"]),
            pfam_id=pa.array(["P53", "P53_TAD"]),
            description=pa.array(["P53 DNA-binding domain", "P53 transactivating"]),
            type=pa.array(["Domain", "Family"]),
        ),
        "protein_pfam_links": _t(
            protein_id=pa.array([101, 101], pa.int64()),
            pfam_pk_id=pa.array([1, 2], pa.int64()),
        ),
        "entity_relationship_types": _t(
            id=pa.array([1, 2], pa.int64()),
            code=pa.array(["interacts_with", "in_pathway"]),
            description=pa.array(["physical interaction", "member of pathway"]),
        ),
        # TP53 on the left once and on the right once, so the report has
        # to count both directions.
        "entity_relationships": _t(
            id=pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
            entity_1_id=pa.array(
                [TP53, TP53, PATHWAY, DISEASE, DISEASE, DISEASE], pa.int64()
            ),
            entity_1_group_id=pa.array(
                [
                    GROUP_GENES, GROUP_GENES, GROUP_PATHWAYS,
                    GROUP_DISEASES, GROUP_DISEASES, GROUP_DISEASES,
                ],
                pa.int64(),
            ),
            entity_2_id=pa.array(
                [PROTEIN, PATHWAY, TP53, DGENE, DGENE, PROTEIN], pa.int64()
            ),
            entity_2_group_id=pa.array(
                [
                    GROUP_PROTEINS, GROUP_PATHWAYS, GROUP_GENES,
                    GROUP_GENES, GROUP_GENES, GROUP_PROTEINS,
                ],
                pa.int64(),
            ),
            # Mixed types, so a type filter has something to filter.
            relationship_type_id=pa.array([1, 2, 2, 1, 1, 1], pa.int64()),
            # The disease has two ClinGen assertions naming the SAME gene
            # and one MONDO link to a protein: so its ClinGen gene count
            # is 1, its ClinGen relationship count 2, and its total 3.
            data_source_id=pa.array(
                [DS_HGNC, DS_REACTOME, DS_REACTOME, DS_CLINGEN, DS_CLINGEN, DS_MONDO],
                pa.int64(),
            ),
            etl_package_id=pa.array([81] * 6, pa.int64()),
        ),
    }


def _variant_tables() -> dict[str, pa.Table]:
    """
    The flat variant tables, and one variant annotated properly.
    
    17:150:A:G is the interesting one: three transcripts of one gene with
    three different consequences, one of them canonical, and an
    AlphaMissense score attached to that transcript by a **versioned**
    id — which is how AlphaMissense spells transcripts and VEP does not.
    """
    return {
        "variant_consequences": _t(
            name=pa.array(["missense_variant", "synonymous_variant", "intron_variant"]),
            severity_rank=pa.array([13, 20, 25], pa.int32()),
            consequence_group=pa.array(["coding", "coding", "non-coding"]),
            consequence_category=pa.array(["moderate", "low", "modifier"]),
            description=pa.array([None, None, None], pa.string()),
            is_active=pa.array([True, True, True]),
        ),
        "variant_impacts": _t(
            name=pa.array(["MODERATE", "LOW", "MODIFIER"]),
            severity_rank=pa.array([2, 3, 4], pa.int32()),
            description=pa.array([None, None, None], pa.string()),
        ),
        "variant_rsid": _t(
            chromosome=pa.array([17, 17, 22], pa.int32()),
            position=pa.array([150, 200, 777], pa.int64()),
            reference_allele=pa.array(["A", "A", "C"]),
            alternate_allele=pa.array(["G", "G", "T"]),
            rsid=pa.array(["rs101", "rs102", "rs103"]),
        ),
        "variant_predictions": _t(
            chromosome=pa.array([17], pa.int32()),
            position=pa.array([150], pa.int64()),
            reference_allele=pa.array(["A"]),
            alternate_allele=pa.array(["G"]),
            cadd_raw_score=pa.array([3.5]),
            cadd_phred=pa.array([23.6]),
            revel_max=pa.array([0.198]),
            sift_max=pa.array([0.01]),
            polyphen_max=pa.array([0.806]),
            spliceai_ds_max=pa.array([0.0]),
            pangolin_largest_ds=pa.array([0.0]),
            phylop=pa.array([2.1]),
        ),
        # 17:150 is scored on the transcript VEP also calls most severe,
        # so both join grains agree there.
        #
        # 17:200 is scored on ENST00000099 — a transcript VEP never
        # reports for it. That is the common case in the real bundle:
        # AlphaMissense picks its own transcript, and requiring it to
        # match VEP's most-severe one drops ~97% of the scores.
        "variant_alphamissense": _t(
            chromosome=pa.array([17, 17], pa.int32()),
            position=pa.array([150, 200], pa.int64()),
            reference_allele=pa.array(["A", "A"]),
            alternate_allele=pa.array(["G", "G"]),
            # Versioned, as AlphaMissense writes it.
            transcript_id=pa.array(["ENST00000001.9", "ENST00000099.3"]),
            predictor_key=pa.array(["am", "am"]),
            predictor_name=pa.array(["alphamissense", "alphamissense"]),
            score=pa.array([0.17, 0.91]),
            classification=pa.array(["likely_benign", "likely_pathogenic"]),
        ),
        # eQTL evidence. Deliberately not all brain: which tissues a
        # bundle carries is a build flag (50 in the GTEx catalogue, 13
        # loaded today), so nothing about tissue may be hardcoded.
        #
        # The variant sits inside TP53's range and regulates BRCA1 —
        # which is the point of the report: the gene a variant is *in*
        # and the gene it *regulates* are usually different.
        "variant_gtex": _t(
            chromosome=pa.array([17, 17, 17, 17], pa.int32()),
            position=pa.array([150, 150, 150, 200], pa.int64()),
            reference_allele=pa.array(["A"] * 4),
            alternate_allele=pa.array(["G"] * 4),
            gene_id=pa.array(
                [
                    "ENSG00000012048",  # BRCA1, which the bundle knows
                    "ENSG00000012048",
                    "ENSG09999999999",  # no BF4 entity for this one
                    "ENSG00000012048",
                ]
            ),
            bio_context=pa.array(
                ["Brain_Cortex", "Liver", "Brain_Cortex", "Liver"]
            ),
            qtl_type=pa.array(["eQTL", "eQTL", "eQTL", "sQTL"]),
            beta=pa.array([1.2, 0.8, -0.4, 0.1]),
            se=pa.array([0.1, 0.2, 0.3, 0.4]),
            p_value=pa.array([1e-20, 1e-5, 1e-3, 0.4]),
            n=pa.array([200, 150, 200, 150], pa.int64()),
            effect_allele=pa.array(["G"] * 4),
            evidence_key=pa.array(["k1", "k2", "k3", "k4"]),
        ),
        "variant_molecular_effects": _t(
            chromosome=pa.array([17, 17, 17, 17], pa.int32()),
            position=pa.array([150, 150, 150, 200], pa.int64()),
            reference_allele=pa.array(["A"] * 4),
            alternate_allele=pa.array(["G"] * 4),
            variant_key=pa.array(
                ["17:150:A:G", "17:150:A:G", "17:150:A:G", "17:200:A:G"]
            ),
            # Unversioned, as VEP writes it.
            feature=pa.array(
                ["ENST00000001", "ENST00000002", "ENST00000003", "ENST00000009"]
            ),
            feature_type=pa.array(["Transcript"] * 4),
            symbol=pa.array(["TP53", "TP53", "TP53", "BRCA1"]),
            gene=pa.array(["ENSG0001", "ENSG0001", "ENSG0001", "ENSG0002"]),
            biotype=pa.array(["protein_coding"] * 4),
            consequence=pa.array(
                ["missense_variant", "synonymous_variant", "intron_variant",
                 "missense_variant"]
            ),
            impact=pa.array(["MODERATE", "LOW", "MODIFIER", "MODERATE"]),
            canonical=pa.array(["YES", None, None, "YES"]),
            mane_select=pa.array(["NM_000546.6", None, None, None]),
        ),
    }


def _variant_partitions() -> dict[int, pa.Table]:
    """Three variants inside TP53's range (100-400), two elsewhere."""
    return {
        17: _t(
            chromosome=pa.array([17] * 4, pa.int32()),
            position=pa.array([150, 200, 300, 9000], pa.int64()),
            reference_allele=pa.array(["A"] * 4),
            alternate_allele=pa.array(["G"] * 4),
            variant_key=pa.array([f"17:{p}:A:G" for p in (150, 200, 300, 9000)]),
        ),
        22: _t(
            chromosome=pa.array([22], pa.int32()),
            position=pa.array([777], pa.int64()),
            reference_allele=pa.array(["C"]),
            alternate_allele=pa.array(["T"]),
            variant_key=pa.array(["22:777:C:T"]),
        ),
    }


@pytest.fixture(scope="session", autouse=True)
def _models_bootstrapped():
    """
    Populate `Base.metadata`.

    Several variant tables are mapped by function rather than by a
    declarative class, so they only reach the metadata once bootstrap has
    run against an engine.
    """
    from sqlalchemy import create_engine

    from biofilter.utils.db_loader import bootstrap_models

    bootstrap_models(create_engine("sqlite:///:memory:"))


@pytest.fixture
def fixture_bundle(tmp_path: Path) -> Path:
    return _write_bundle(
        tmp_path / "20260101", _tables(), _variant_tables(), _variant_partitions()
    )


@pytest.fixture
def pairing_bundle(tmp_path: Path) -> Path:
    """
    The same bundle, plus what it takes to make a pair.

    `pair_variants` needs two things the shared fixture deliberately does
    not have, because adding them there would change counts a dozen other
    tests assert on: a connector entity reaching two *different* genes,
    and variants inside the second of those genes.

    PATHWAY reaches TP53 and BRCA1 — two genes, so one pair. DISEASE
    reaches three (TP53, BRCA1, DGENE), so `max_group_size=2` drops the
    disease and keeps the pathway, which is exactly what that parameter
    is for.
    """
    tables = _tables()
    tables["entity_relationships"] = pa.concat_tables(
        [tables["entity_relationships"], _pair_links()]
    )
    # DGENE has an alias and relationships but no `gene_masters` row, so
    # it resolves to an entity and never to a gene. A third *pairable*
    # gene is what makes global deduplication testable: with only two,
    # every item pair arrives by one path and the rule is vacuous.
    tables["gene_masters"] = pa.concat_tables(
        [tables["gene_masters"], _dgene_master()]
    )
    tables["entity_aliases"] = _with_colliding_alias(tables["entity_aliases"])
    partitions = _variant_partitions()
    partitions[17] = pa.concat_tables([partitions[17], _brca1_variants()])
    return _write_bundle(
        tmp_path / "20260102", tables, _variant_tables(), partitions
    )


def _pair_links() -> pa.Table:
    """Connectors that reach more than one gene."""
    return _t(
        id=pa.array([7, 8, 9], pa.int64()),
        entity_1_id=pa.array([PATHWAY, DISEASE, DISEASE], pa.int64()),
        entity_1_group_id=pa.array(
            [GROUP_PATHWAYS, GROUP_DISEASES, GROUP_DISEASES], pa.int64()
        ),
        entity_2_id=pa.array([BRCA1, TP53, BRCA1], pa.int64()),
        entity_2_group_id=pa.array(
            [GROUP_GENES, GROUP_GENES, GROUP_GENES], pa.int64()
        ),
        relationship_type_id=pa.array([2, 1, 1], pa.int64()),
        data_source_id=pa.array([DS_REACTOME, DS_CLINGEN, DS_CLINGEN], pa.int64()),
        etl_package_id=pa.array([81] * 3, pa.int64()),
    )


def _dgene_master() -> pa.Table:
    """DGENE as a gene, so three genes can pair with each other."""
    return _t(
        id=pa.array([104], pa.int64()),
        entity_id=pa.array([DGENE], pa.int64()),
        symbol=pa.array(["DGENE1"]),
        hgnc_status=pa.array(["Approved"]),
        omic_status_id=pa.array([1], pa.int64()),
        locus_group_id=pa.array([1], pa.int64()),
        locus_type_id=pa.array([1], pa.int64()),
        chromosome=pa.array([17], pa.int32()),
    )


def _with_colliding_alias(aliases: pa.Table) -> pa.Table:
    """
    Give TP53 an alias whose text is BRCA1's entity id.

    This is the real shape of the problem: 174,410 aliases in the bundle
    are bare numbers (Entrez ids) and 14,335 of those are also the entity
    id of a different gene — Entrez 2 is A2M, entity 2 is A1BG-AS1.
    Nothing can tell them apart by looking, which is why the caller says
    which column they mean rather than the report guessing.

    Built from the existing table's own schema so it cannot drift from it.
    """
    row = {name: [None] for name in aliases.column_names}
    row.update(
        id=[900], entity_id=[TP53], alias_value=["2"], alias_norm=["2"],
        alias_type=["code"], xref_source=["ENTREZ"], is_primary=[False],
    )
    if "is_active" in row:
        row["is_active"] = [True]
    if "data_source_id" in row:
        row["data_source_id"] = [DS_HGNC]
    extra = pa.table(
        {name: pa.array(values, type=aliases.schema.field(name).type)
         for name, values in row.items()}
    )
    return pa.concat_tables([aliases, extra])


def _brca1_variants() -> pa.Table:
    """Two variants inside BRCA1 (5000-5400), so a pair has two sides."""
    return _t(
        chromosome=pa.array([17, 17], pa.int32()),
        position=pa.array([5100, 5200], pa.int64()),
        reference_allele=pa.array(["A", "A"]),
        alternate_allele=pa.array(["G", "G"]),
        variant_key=pa.array(["17:5100:A:G", "17:5200:A:G"]),
    )


def _write_bundle(
    root: Path,
    tables: dict[str, pa.Table],
    variant_tables: dict[str, pa.Table],
    partitions: dict[int, pa.Table],
) -> Path:
    tables_dir = root / "tables"
    entries: list[dict] = []

    def write(rel: str, data: pa.Table, **entry) -> None:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(data, path)
        entries.append(
            {
                "file": rel,
                "rows": data.num_rows,
                "bytes": path.stat().st_size,
                **entry,
            }
        )

    # Written as one relation: split in the source only so each domain's
    # aliases sit next to the rest of that domain.
    tables["entity_aliases"] = pa.concat_tables(
        [tables["entity_aliases"], tables.pop("entity_aliases_extra")]
    )
    for name, table in tables.items():
        write(
            f"tables/{name}.parquet",
            _conform_to_models(name, table),
            name=name,
            branch="core",
        )

    for name, table in variant_tables.items():
        write(
            f"tables/{name}.parquet",
            _conform_to_models(name, table),
            name=name,
            branch="variant",
        )

    for chrom, table in partitions.items():
        write(
            f"tables/variant_masters/variant_masters_chr{chrom}.parquet",
            _conform_to_models("variant_masters", table),
            name=f"variant_masters_chr{chrom}",
            table="variant_masters",
            branch="variant",
        )

    # Undeclared, empty, wrong schema — the shadowing parent.
    pq.write_table(
        pa.table(
            {
                "variant_id": pa.array([], pa.int64()),
                "position_start": pa.array([], pa.int64()),
            }
        ),
        tables_dir / "variant_masters.parquet",
    )

    manifest = {
        "manifest_version": 2,
        "biofilter_version": "4.3.0",
        "schema_version": "4.3.0",
        "format": "parquet",
        "created_at": "2026-01-01T00:00:00+00:00",
        "bundle_id": "fixturebundle0001",
        "tables": entries,
    }
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2))
    (root / "build_record.json").write_text(
        json.dumps({"built_at": "2026-01-01T00:00:00+00:00", "steps": []})
    )
    return root
